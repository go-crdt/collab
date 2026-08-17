//go:build !js

package collab_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-crdt/collab"
	"github.com/go-crdt/crdt"
	"google.golang.org/grpc"
)

// What a session carries is not only a text people are looking at. It is the
// comments on it, the record of who changed what, the messages beside it —
// written once and expected to still be there. A server that saved only when
// the last participant left lost all of it if it was restarted while anybody
// was still connected, and said nothing.

// countingStore records what was saved and when, so a test can watch a server
// keep its promise rather than wait and hope.
type countingStore struct {
	inner collab.Store

	mu     sync.Mutex
	saves  int
	failOn string
	saved  chan struct{}
	tried  chan struct{}
}

func newCountingStore() *countingStore {
	return &countingStore{
		inner: collab.NewMemoryStore(),
		saved: make(chan struct{}, 64),
		tried: make(chan struct{}, 64),
	}
}

func (s *countingStore) Load(ctx context.Context, document string) ([]byte, error) {
	return s.inner.Load(ctx, document)
}

func (s *countingStore) Save(ctx context.Context, document string, snapshot []byte) error {
	s.mu.Lock()
	fail := s.failOn == document
	if !fail {
		s.saves++
	}
	s.mu.Unlock()
	select {
	case s.tried <- struct{}{}:
	default:
	}
	if fail {
		return errors.New("this store is broken")
	}
	if err := s.inner.Save(ctx, document, snapshot); err != nil {
		return err
	}
	select {
	case s.saved <- struct{}{}:
	default:
	}
	return nil
}

// awaitAttempt waits for the store to be written to whether or not the write
// succeeded, which is how a test waits for a server to notice a departure when
// the save that follows it is going to fail.
func (s *countingStore) awaitAttempt(t *testing.T, what string) {
	t.Helper()
	select {
	case <-s.tried:
	case <-time.After(settle):
		t.Fatalf("timed out waiting for %s", what)
	}
}

func (s *countingStore) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.saves
}

// awaitSave waits for the store to be written to, so nothing here sleeps for a
// fixed time and calls that a test.
func (s *countingStore) awaitSave(t *testing.T, what string) {
	t.Helper()
	select {
	case <-s.saved:
	case <-time.After(settle):
		t.Fatalf("timed out waiting for %s", what)
	}
}

func TestWhatWasWrittenSurvivesARestartMidSession(t *testing.T) {
	store := newCountingStore()
	srv, conn := serve(t, collab.Config{Store: store, PersistEvery: 10 * time.Millisecond})

	ada := join(t, conn, collab.ClientConfig{Document: "project:default", Site: 1})
	comment, err := ada.Map("comment:9f3c")
	if err != nil {
		t.Fatal(err)
	}
	if err := comment.Set("body", []byte("à revoir")); err != nil {
		t.Fatal(err)
	}
	chat, err := ada.List("chat")
	if err != nil {
		t.Fatal(err)
	}
	if err := chat.Append([]byte("on commence")); err != nil {
		t.Fatal(err)
	}

	// Ada is still connected. Before this, nothing would have been saved.
	store.awaitSave(t, "the periodic save")

	// The server goes away with the session still open, which is what a restart
	// or a redeploy looks like from here.
	if err := srv.Close(t.Context()); err != nil {
		t.Fatalf("Close: %v", err)
	}

	kept, err := store.Load(t.Context(), "project:default")
	if err != nil {
		t.Fatal(err)
	}
	doc, err := crdt.LoadComposite(9, kept)
	if err != nil {
		t.Fatalf("what was stored is unreadable: %v", err)
	}
	keptComment, err := doc.Map("comment:9f3c")
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := keptComment.Get("body"); !ok || !bytes.Equal(got, []byte("à revoir")) {
		t.Fatalf("the comment is %q, want %q", got, "à revoir")
	}
	keptChat, err := doc.List("chat")
	if err != nil {
		t.Fatal(err)
	}
	if got := keptChat.Values(); len(got) != 1 || string(got[0]) != "on commence" {
		t.Fatalf("the chat is %q", got)
	}
}

// A document nobody is in is saved and let go of, so a long-lived server does
// not hold every document it has ever served.
func TestADocumentNobodyIsInIsLetGoOf(t *testing.T) {
	store := newCountingStore()
	clock := &testClock{now: time.Now()}
	srv, conn := serveClocked(t, clock, collab.Config{Store: store, EvictAfter: time.Minute})

	ada := join(t, conn, collab.ClientConfig{Document: "doc", Site: 1})
	body, err := ada.Text("body")
	if err != nil {
		t.Fatal(err)
	}
	if err := body.Insert(0, "quelque chose"); err != nil {
		t.Fatal(err)
	}
	if err := ada.Close(); err != nil {
		t.Fatal(err)
	}
	// Leaving persists it, which is what it always did.
	store.awaitSave(t, "the save on the last participant leaving")

	// A document nobody is in for long enough is dropped. Moving the clock is
	// how that happens in under a minute.
	clock.advance(2 * time.Minute)
	srv.Housekeep(t.Context(), time.Minute)
	if got := srv.Documents(); got != 0 {
		t.Fatalf("the server still holds %d documents", got)
	}

	// And what it held is still there: evicting costs a read, not anything
	// anybody wrote.
	back := join(t, conn, collab.ClientConfig{Document: "doc", Site: 2})
	backBody, err := back.Text("body")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := backBody.String(), "quelque chose"; got != want {
		t.Fatalf("the reopened document holds %q, want %q", got, want)
	}
	if err := srv.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
}

// The hazard eviction brings, and the one worth a test of its own: a session
// that was handed a document just as it was dropped must not go on editing it,
// or the next session loads a second replica from the store and whichever saves
// last erases the other.
func TestASessionIsNeverLeftEditingADroppedReplica(t *testing.T) {
	store := newCountingStore()
	clock := &testClock{now: time.Now()}
	srv, conn := serveClocked(t, clock, collab.Config{Store: store, EvictAfter: time.Millisecond})
	t.Cleanup(func() { _ = srv.Close(context.Background()) })

	// Sessions arrive while the server is dropping the document under them, as
	// fast as both can go. Every one of them has to end up on the replica the
	// server hands out.
	var wg sync.WaitGroup
	var written sync.WaitGroup
	var mu sync.Mutex
	wrote := 0
	const sessions = 40
	for i := range sessions {
		wg.Add(1)
		go func(site int) {
			defer wg.Done()
			// Each session moves the clock past the eviction deadline, so the
			// housekeeping below is always dropping the document somebody is
			// joining.
			clock.advance(time.Millisecond)
			c, err := collab.Join(t.Context(), collab.GRPC(conn),
				collab.ClientConfig{Document: "raced", Site: crdt.SiteID(site + 1)})
			if err != nil {
				return // a refused join is not the failure under test
			}
			defer c.Close()
			text, err := c.Text("body")
			if err != nil {
				return
			}
			if err := text.Insert(0, "x"); err != nil {
				return
			}
			mu.Lock()
			wrote++
			mu.Unlock()
		}(i)
	}
	// And the eviction runs throughout rather than once at the end, so the two
	// really do overlap.
	done := make(chan struct{})
	written.Add(1)
	go func() {
		defer written.Done()
		for {
			select {
			case <-done:
				return
			default:
				srv.Housekeep(context.Background(), time.Millisecond)
			}
		}
	}()
	wg.Wait()
	close(done)
	written.Wait()

	// Every character that was written is in the document the server holds, or
	// in what it saved. Losing one would mean two replicas had been live.
	if err := srv.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	kept, err := store.Load(t.Context(), "raced")
	if err != nil {
		t.Fatal(err)
	}
	doc, err := crdt.LoadComposite(99, kept)
	if err != nil {
		t.Fatalf("what was stored is unreadable: %v", err)
	}
	body, err := doc.Text("body")
	if err != nil {
		t.Fatal(err)
	}
	// Every character an accepted session wrote is there. One missing would mean
	// two replicas of this document had been live at once, which is the whole
	// hazard eviction brings.
	if got := body.Len(); got != wrote {
		t.Fatalf("%d characters were written and %d survived: a replica was lost", wrote, got)
	}
	if wrote == 0 {
		t.Fatal("no session got far enough to write, so nothing was tested")
	}
	t.Logf("all %d characters survived %d sessions racing eviction", wrote, sessions)
}

// A document that cannot be saved as it is evicted has nobody to return an
// error to, and cannot be kept — a session may already have opened a fresh
// replica of it. So it is reported where an operator can see it.
func TestAnEvictionThatCannotSaveIsReported(t *testing.T) {
	store := newCountingStore()
	store.failOn = "doomed"
	clock := &testClock{now: time.Now()}
	told := make(chan string, 1)
	srv, conn := serveClocked(t, clock, collab.Config{
		Store:      store,
		EvictAfter: time.Minute,
		OnEvictError: func(document string, err error) {
			select {
			case told <- document + ": " + err.Error():
			default:
			}
		},
	})
	t.Cleanup(func() { _ = srv.Close(context.Background()) })

	c := join(t, conn, collab.ClientConfig{Document: "doomed", Site: 1})
	body, err := c.Text("body")
	if err != nil {
		t.Fatal(err)
	}
	if err := body.Insert(0, "x"); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	// The departure is handled by the server's own goroutine, so wait for the
	// save it attempts rather than assume it has run.
	store.awaitAttempt(t, "the server to notice the departure")
	clock.advance(2 * time.Minute)
	srv.Housekeep(t.Context(), time.Minute)

	select {
	case got := <-told:
		if !strings.Contains(got, "doomed") || !strings.Contains(got, "broken") {
			t.Fatalf("the report was %q", got)
		}
	case <-time.After(settle):
		t.Fatal("a document that could not be saved was dropped without a word")
	}
}

// A server that asked for neither behaves as it always did, and Close still
// works so a caller need not know which kind it configured.
func TestAServerThatAsksForNeitherIsUnchanged(t *testing.T) {
	store := newCountingStore()
	srv, conn := serve(t, collab.Config{Store: store})

	c := join(t, conn, collab.ClientConfig{Document: "plain", Site: 1})
	body, err := c.Text("body")
	if err != nil {
		t.Fatal(err)
	}
	if err := body.Insert(0, "rien de spécial"); err != nil {
		t.Fatal(err)
	}
	// Nothing saves on its own.
	time.Sleep(20 * time.Millisecond)
	if got := store.count(); got != 0 {
		t.Fatalf("a server with no interval saved %d times", got)
	}
	// And closing saves, twice over without complaint.
	if err := srv.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := srv.Close(t.Context()); err != nil {
		t.Fatalf("closing twice: %v", err)
	}
	if got := store.count(); got != 1 {
		t.Fatalf("closing saved %d times, want once", got)
	}
}

// serveClocked is serve with a clock the test can move. The clock goes in the
// configuration rather than being set afterwards, because the housekeeping
// goroutine starts reading it the moment the server exists.
func serveClocked(t *testing.T, clock *testClock, cfg collab.Config) (*collab.Server, *grpc.ClientConn) {
	t.Helper()
	cfg.Clock = clock.Now
	return serve(t, cfg)
}

// testClock is a clock a test can move, so that idleness is reached by saying
// so rather than by waiting.
type testClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *testClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// The most ordinary way there is to lose work: write something and close. The
// edit was sent — the call returned — and the session was torn down before the
// server had read it. Somebody writes a comment and closes the tab.
//
// The sessions run at once because that is what makes the window wide enough to
// see: one at a time, a server with nothing else to do reads the operation
// before the cancellation reaches it, and the fault hides. It did not hide from
// the eviction test, which is where it turned up.
func TestAnEditMadeJustBeforeClosingIsNotLost(t *testing.T) {
	store := newCountingStore()
	srv, conn := serve(t, collab.Config{Store: store})
	t.Cleanup(func() { _ = srv.Close(context.Background()) })

	const tabs = 40
	var wg sync.WaitGroup
	var mu sync.Mutex
	opened := 0
	for i := range tabs {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			c, err := collab.Join(t.Context(), collab.GRPC(conn),
				collab.ClientConfig{Document: "tabs", Site: crdt.SiteID(n + 1)})
			if err != nil {
				return
			}
			comment, err := c.Map(fmt.Sprintf("comment:%02d", n))
			if err != nil {
				return
			}
			if err := comment.Set("body", []byte("à revoir")); err != nil {
				return
			}
			mu.Lock()
			opened++
			mu.Unlock()
			// No wait of any kind: this is what closing a tab looks like.
			_ = c.Close()
		}(i)
	}
	wg.Wait()

	// Every comment somebody wrote is on the server, so every one of them will
	// be saved. One missing is one person's work gone with no sign of it.
	watcher := join(t, conn, collab.ClientConfig{Document: "tabs", Site: 1000})
	await(t, watcher, "every comment to arrive", func() bool {
		return len(watcher.Parts()) >= opened
	})
	if got := len(watcher.Parts()); got != opened {
		t.Fatalf("%d comments of %d survived being written just before a close", got, opened)
	}
	if opened == 0 {
		t.Fatal("no session got far enough to write, so nothing was tested")
	}
	t.Logf("all %d comments survived being written just before a close", opened)
}
