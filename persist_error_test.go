//go:build !js

package collab

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// A store that loads but cannot save, the way a full disk cannot save and the
// way a name a filesystem will not take cannot save. refusingStore next door
// refuses both, which is a different question.
type unsavableStore struct {
	*MemoryStore
	attempts atomic.Int64
}

func (s *unsavableStore) Save(context.Context, string, []byte) error {
	s.attempts.Add(1)
	return errors.New("no space left on device")
}

// A server that cannot write says so.
//
// It used to say nothing. The periodic save's error was dropped, so a store
// that could not write went on failing every pass while participants went on
// editing and were told nothing, and the work was there until the process
// stopped and then was not. Measured before the fix: forty saves attempted and
// refused in two hundred milliseconds, and nobody told once.
//
// A disk that filled, a document whose name a filesystem will not take, a
// credential that expired — all of them look exactly like a server that is
// working, which is the worst way for durability to fail.
func TestAServerThatCannotWriteSaysSo(t *testing.T) {
	store := &unsavableStore{MemoryStore: NewMemoryStore()}

	var mu sync.Mutex
	told := map[string]error{}
	srv := NewServer(Config{
		Store:        store,
		PersistEvery: 5 * time.Millisecond,
		OnPersistError: func(document string, err error) {
			mu.Lock()
			told[document] = err
			mu.Unlock()
		},
	})
	defer func() { _ = srv.Close(context.Background()) }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	transport, conn := Pipe()
	go func() { _ = srv.ServePipe(ctx, conn) }()
	c, err := Join(ctx, transport, ClientConfig{Document: "d", Site: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	body, err := c.Text("body")
	if err != nil {
		t.Fatal(err)
	}
	if err := body.Insert(0, "work somebody did"); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(10 * time.Second)
	for {
		mu.Lock()
		got, ok := told["d"]
		mu.Unlock()
		if ok {
			if !strings.Contains(got.Error(), "no space left on device") {
				t.Fatalf("the report does not carry what the store said: %v", got)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the server refused %d saves and reported none", store.attempts.Load())
		}
		time.Sleep(2 * time.Millisecond)
	}
	if store.attempts.Load() == 0 {
		t.Fatal("the server never even tried to save")
	}
}

// Flush reports every document that could not be written, not the first.
//
// A caller given one error when four documents failed knows less than it needs
// to, and the housekeeping needs each failure to name its own document.
func TestFlushReportsEveryDocumentThatCouldNotBeWritten(t *testing.T) {
	srv := NewServer(Config{Store: &unsavableStore{MemoryStore: NewMemoryStore()}})
	defer func() { _ = srv.Close(context.Background()) }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, name := range []string{"one", "two", "three", "four"} {
		transport, conn := Pipe()
		go func() { _ = srv.ServePipe(ctx, conn) }()
		c, err := Join(ctx, transport, ClientConfig{Document: name, Site: 1})
		if err != nil {
			t.Fatal(err)
		}
		body, err := c.Text("body")
		if err != nil {
			t.Fatal(err)
		}
		if err := body.Insert(0, name); err != nil {
			t.Fatal(err)
		}
		arrived(t, ctx, srv, name, name)
		_ = c.Close()
	}

	err := srv.Flush(ctx)
	if err == nil {
		t.Fatal("a flush that could write nothing reported success")
	}
	if got := len(strings.Split(err.Error(), "\n")); got != 4 {
		t.Fatalf("a flush of four unwritable documents reported %d errors:\n%v", got, err)
	}
	// And a flush is not where the hook fires: a caller that asks is given the
	// error to handle.
	var fired int
	quiet := NewServer(Config{
		Store:          &unsavableStore{MemoryStore: NewMemoryStore()},
		OnPersistError: func(string, error) { fired++ },
	})
	defer func() { _ = quiet.Close(context.Background()) }()
	transport, conn := Pipe()
	go func() { _ = quiet.ServePipe(ctx, conn) }()
	c, err := Join(ctx, transport, ClientConfig{Document: "d", Site: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	body, err := c.Text("body")
	if err != nil {
		t.Fatal(err)
	}
	if err := body.Insert(0, "x"); err != nil {
		t.Fatal(err)
	}
	arrived(t, ctx, quiet, "d", "x")
	if err := quiet.Flush(ctx); err == nil {
		t.Fatal("Flush reported success on an unwritable store")
	}
	if fired != 0 {
		t.Fatalf("Flush called OnPersistError %d times; it is for the housekeeping", fired)
	}
}

// A name is a length a peer chose, and this package does not allocate on one of
// those without checking it first.
//
// The name is worse than an ordinary allocation because it is kept: it becomes
// a key in the table of open documents. Measured before the bound existed:
// sixty-four names of a mebibyte each made a server hold 66 MB, and twenty
// thousand ordinary ones cost 16 MB in a hundred milliseconds from a single
// connection.
func TestADocumentNameIsBounded(t *testing.T) {
	srv := NewServer(Config{Store: NewMemoryStore()})
	defer func() { _ = srv.Close(context.Background()) }()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	open := func(name string) error {
		raw, err := encodeClient(kindJoin, joinMsg{Document: name, Site: 1})
		if err != nil {
			t.Fatal(err)
		}
		kind, msg, err := decodeClient(raw)
		if err != nil {
			t.Fatal(err)
		}
		return srv.session(&scriptedCarrier{ctx: ctx, in: []scripted{{kind: kind, msg: msg}}, hangsUp: true})
	}

	// A session that ends because the peer hung up is not a refusal, so what is
	// asserted is what the error says rather than that there is one.
	if err := open(strings.Repeat("x", maxDocumentName)); err != nil && strings.Contains(err.Error(), "document name may be") {
		t.Fatalf("a name of exactly the bound was refused: %v", err)
	}
	err := open(strings.Repeat("x", maxDocumentName+1))
	if err == nil {
		t.Fatal("a name one byte over the bound was accepted")
	}
	if !strings.Contains(err.Error(), "4096") || !strings.Contains(err.Error(), "4097") {
		t.Fatalf("the refusal says neither the bound nor what arrived: %v", err)
	}
	// And nothing was opened for it: a refused name must not be a document the
	// server now holds.
	srv.mu.Lock()
	held := len(srv.docs)
	srv.mu.Unlock()
	if held != 1 {
		t.Fatalf("the server holds %d documents; the refused name should not be one", held)
	}
}

// arrived waits until the server holds an edit, by asking a second participant.
//
// A flush can only write what the server has, so a test that edits and flushes
// without waiting is racing — and the half it loses looks exactly like the bug
// these tests are about.
func arrived(t *testing.T, ctx context.Context, srv *Server, document, want string) {
	t.Helper()
	transport, conn := Pipe()
	go func() { _ = srv.ServePipe(ctx, conn) }()
	w, err := Join(ctx, transport, ClientConfig{Document: document, Site: 999})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()
	body, err := w.Text("body")
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for body.String() != want {
		if time.Now().After(deadline) {
			t.Fatalf("%q never reached the server; it reads %q", want, body.String())
		}
		time.Sleep(2 * time.Millisecond)
	}
}
