//go:build !js

package collab

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Eviction's careful paths are the ones a session only meets when the timing is
// against it, and waiting for that timing is not a test. These reach them by
// putting the server in the state and calling the thing.

// evicting marks a document as one being let go of, and returns the function
// that finishes the eviction.
func evicting(t *testing.T, s *Server, name string) (*document, func()) {
	t.Helper()
	d, err := s.open(context.Background(), name)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	d.mu.Lock()
	d.evicted = true
	d.gone = make(chan struct{})
	d.mu.Unlock()
	return d, func() {
		s.mu.Lock()
		delete(s.docs, name)
		s.mu.Unlock()
		close(d.gone)
	}
}

// A document being saved on its way out stays in the table, so nobody can load
// a second replica of it. Asking for it waits, and what comes back is the one
// loaded in its place — never the one on its way out.
func TestOpeningWaitsForAnEvictionToFinish(t *testing.T) {
	s := NewServer(Config{})
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	going, finish := evicting(t, s, "doc")

	got := make(chan *document, 1)
	go func() {
		d, err := s.open(context.Background(), "doc")
		if err != nil {
			t.Error(err)
			return
		}
		got <- d
	}()

	// Nothing comes back while the eviction is in flight.
	select {
	case d := <-got:
		t.Fatalf("open returned %p while the eviction was still running", d)
	case <-time.After(20 * time.Millisecond):
	}

	finish()
	select {
	case d := <-got:
		if d == going {
			t.Fatal("open handed back the document that was being evicted")
		}
	case <-time.After(time.Second):
		t.Fatal("open never returned after the eviction finished")
	}
}

// And a caller that gives up while waiting is told why, rather than waiting on.
func TestOpeningAnEvictedDocumentGivesUpWithTheCaller(t *testing.T) {
	s := NewServer(Config{})
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	_, finish := evicting(t, s, "doc")
	defer finish()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.open(ctx, "doc"); status.Code(err) != codes.Canceled {
		t.Fatalf("open with a cancelled caller = %v, want Canceled", err)
	}
}

// A session handed a document just as it was let go of asks again, and the
// second answer is the replica the server now hands out.
func TestJoiningRetriesAnEviction(t *testing.T) {
	s := NewServer(Config{})
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	going, finish := evicting(t, s, "doc")

	// join refuses it outright, which is what openAndJoin acts on.
	if _, err := going.join(joinMsg{Document: "doc", Site: 1}); !errors.Is(err, errEvicted) {
		t.Fatalf("joining an evicted document = %v, want errEvicted", err)
	}

	joined := make(chan *document, 1)
	go func() {
		d, sub, err := s.openAndJoin(context.Background(), joinMsg{Document: "doc", Site: 1})
		if err != nil {
			t.Error(err)
			return
		}
		_ = sub
		joined <- d
	}()
	finish()
	select {
	case d := <-joined:
		if d == going {
			t.Fatal("the session joined the document that was being evicted")
		}
	case <-time.After(time.Second):
		t.Fatal("the session never joined anything")
	}
}

// The window the retry exists for, made to happen: a document handed over and
// then let go of before the session joined it. The session asks again and lands
// on the replica the server now hands out — never on the one being saved.
func TestASessionEvictedBetweenOpeningAndJoiningAsksAgain(t *testing.T) {
	s := NewServer(Config{})
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	s.EvictBetweenOpenAndJoin(context.Background(), time.Minute)

	doc, sub, err := s.openAndJoin(context.Background(), joinMsg{Document: "doc", Site: 1})
	if err != nil {
		t.Fatalf("openAndJoin: %v", err)
	}
	if sub == nil {
		t.Fatal("no subscriber came back")
	}
	doc.mu.Lock()
	evicted := doc.evicted
	doc.mu.Unlock()
	if evicted {
		t.Fatal("the session joined the document that was being evicted")
	}
	// And it is the one the server hands out, so the next session joins it too.
	s.mu.Lock()
	held := s.docs["doc"]
	s.mu.Unlock()
	if held != doc {
		t.Fatal("the session is on a replica the server does not hand out")
	}
}

// Giving up while asking again is reported as giving up, not as a document that
// does not exist.
func TestJoiningGivesUpWithTheCaller(t *testing.T) {
	s := NewServer(Config{})
	t.Cleanup(func() { _ = s.Close(context.Background()) })

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err := s.openAndJoin(ctx, joinMsg{Document: "doc", Site: 1})
	if status.Code(err) != codes.Canceled {
		t.Fatalf("openAndJoin with a cancelled caller = %v, want Canceled", err)
	}
}

// brokenStore cannot be read, which is what a document that will not open looks
// like from here.
type brokenStore struct{}

func (brokenStore) Load(context.Context, string) ([]byte, error) {
	return nil, errors.New("this store cannot be read")
}
func (brokenStore) Save(context.Context, string, []byte) error { return nil }

// A document that will not open is reported, rather than retried until the
// caller gives up: asking again would read the same broken store.
func TestJoiningReportsADocumentThatWillNotOpen(t *testing.T) {
	s := NewServer(Config{Store: brokenStore{}})
	t.Cleanup(func() { _ = s.Close(context.Background()) })

	_, _, err := s.openAndJoin(context.Background(), joinMsg{Document: "doc", Site: 1})
	if status.Code(err) != codes.Internal {
		t.Fatalf("openAndJoin against a broken store = %v, want Internal", err)
	}
}

// And a join refused for any other reason is refused, not retried.
func TestJoiningReportsARefusalAsItIs(t *testing.T) {
	s := NewServer(Config{})
	t.Cleanup(func() { _ = s.Close(context.Background()) })

	_, _, err := s.openAndJoin(context.Background(),
		joinMsg{Document: "doc", Site: uint64(serverSite)})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("joining as the server's own replica = %v, want InvalidArgument", err)
	}
}
