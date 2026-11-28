//go:build !js

package collab

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-crdt/crdt"
)

// The survivors and the panic-only catches of a clean `mutsweep -sites all` of
// v0.83.10. Each test asserts what the guard is for, with the control first
// where there is one. Those that would otherwise die of a nil dereference or
// wait for a context that never ends run through noPanicS and promptCtx, so the
// failure is an assertion that says which guard went.

// server.go:316. A server given no store keeps its documents in memory, and says
// nothing about it: the zero Config is a working server, and the tests that
// build one that way never touch the store directly.
func TestAServerWithoutAStoreHoldsItsDocumentsInMemory(t *testing.T) {
	var srv *Server
	noPanicS(t, "NewServer", func() {
		srv = NewServer(Config{})
		if _, ok := srv.store.(*MemoryStore); !ok {
			t.Errorf("the store of a server given none is %T, want a *MemoryStore", srv.store)
		}
	})
	if srv == nil {
		return
	}
	t.Cleanup(func() { _ = srv.Close(context.Background()) })
	noPanicS(t, "Flush", func() {
		if _, err := srv.open(promptCtx(t), "doc"); err != nil {
			t.Fatalf("open: %v", err)
		}
		if err := srv.Flush(promptCtx(t)); err != nil {
			t.Errorf("Flush = %v", err)
		}
	})
}

// server.go:415. A document nobody has opened is loaded and handed back at once.
// With the guard gone the loop waits on a channel that is nil, which is a wait
// that only the context ends.
func TestOpeningADocumentNobodyHasOpenedDoesNotWait(t *testing.T) {
	srv := NewServer(Config{Store: NewMemoryStore()})
	t.Cleanup(func() { _ = srv.Close(context.Background()) })
	doc, err := srv.open(promptCtx(t), "fresh")
	if err != nil || doc == nil {
		t.Fatalf("open = (%v, %v), want the document and no error", doc, err)
	}
}

// loadFailsStore is a store that cannot read.
type loadFailsStore struct{ Store }

func (loadFailsStore) Load(context.Context, string) ([]byte, error) {
	return nil, errors.New("the disk is gone")
}

// server.go:743. A join the server cannot open a document for is refused, and
// the session ends with that error rather than going on with no document.
func TestASessionWhoseDocumentCannotBeOpenedIsRefused(t *testing.T) {
	srv := NewServer(Config{Store: loadFailsStore{NewMemoryStore()}})
	t.Cleanup(func() { _ = srv.Close(context.Background()) })
	c := &scriptedCarrier{ctx: promptCtx(t), hangsUp: true, in: []scripted{
		{kind: kindJoin, msg: joinMsg{Document: "doc", Site: 1}},
	}}
	noPanicS(t, "session", func() {
		if err := srv.session(c); err == nil {
			t.Error("a session for a document the store cannot read ended without an error")
		}
	})
}

// server.go:910. A participant that joins is answered with the state it joined:
// Join returns on the welcome, so one that is never queued is a join that waits
// for its context.
func TestAJoinIsAnsweredWithAWelcome(t *testing.T) {
	srv := NewServer(Config{Store: NewMemoryStore()})
	t.Cleanup(func() { _ = srv.Close(context.Background()) })
	transport, server := Pipe()
	ctx := promptCtx(t)
	go func() { _ = srv.ServePipe(ctx, server) }()
	c, err := Join(ctx, transport, ClientConfig{Document: "doc", Site: 3})
	if err != nil {
		t.Fatalf("Join = %v, want a welcome", err)
	}
	_ = c.Close()
}

// follow.go:151. A peer that cannot be reached is the error Follow returns, and
// not a carrier to be written to.
func TestFollowingAPeerThatCannotBeOpenedReportsIt(t *testing.T) {
	srv := NewServer(Config{Store: NewMemoryStore()})
	t.Cleanup(func() { _ = srv.Close(context.Background()) })
	noPanicS(t, "Follow", func() {
		if err := srv.Follow(promptCtx(t), &scriptedTransport{}, "doc", 5); !errors.Is(err, ErrTransport) {
			t.Errorf("Follow = %v, want the peer's own failure, ErrTransport", err)
		}
	})
}

// client.go:168. A client given a snapshot to resume from starts from it and
// says what it holds, which is what lets the server answer with the difference.
func TestAClientResumesFromTheSnapshotItWasGiven(t *testing.T) {
	seed := crdt.NewComposite(1)
	part, err := seed.Text("t")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := part.Insert(0, "kept between sessions"); err != nil {
		t.Fatal(err)
	}
	tp := &scriptedTransport{welcomes: []welcomeMsg{{}}}
	c, err := Join(promptCtx(t), tp, ClientConfig{Document: "d", Site: 2, Resume: seed.Snapshot()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	if len(tp.joins) != 1 || len(tp.joins[0].Have) == 0 {
		t.Fatalf("a resuming join said what it holds as %v, want a version", tp.joins)
	}
	text, err := c.Text("t")
	if err != nil {
		t.Fatal(err)
	}
	if got := text.String(); got != "kept between sessions" {
		t.Fatalf("the resumed text = %q", got)
	}
}

// parts.go:103. An edit that produced nothing writes nothing to the carrier.
// The server no longer counts such a frame as a change, so the only place left
// to see the guard is the wire: a frame of no operations, and an acknowledgement
// after it, that somebody has to read.
func TestAnEditThatProducedNothingWritesNothingToTheCarrier(t *testing.T) {
	c, tp := recClient(t)
	text, err := c.Text("t")
	if err != nil {
		t.Fatal(err)
	}
	if err := text.Insert(0, ""); err != nil {
		t.Fatal(err)
	}
	if err := text.Delete(0, 0); err != nil {
		t.Fatal(err)
	}
	if got := tp.count(kindOperation); got != 0 {
		t.Fatalf("edits that produced no operations wrote %d frames of operations", got)
	}
	// Control: an edit that produces something does write one.
	if err := text.Insert(0, "a"); err != nil {
		t.Fatal(err)
	}
	if got := tp.count(kindOperation); got != 1 {
		t.Fatalf("a real edit wrote %d frames of operations, want 1", got)
	}
}

// parts.go:247. A negative length is out of range and says so, even where
// converting its far end would have said something else: here it names the
// middle of a surrogate pair.
func TestDeleteUTF16RefusesANegativeLengthAsOutOfRange(t *testing.T) {
	c, _ := recClient(t)
	text, err := c.Text("t")
	if err != nil {
		t.Fatal(err)
	}
	if err := text.Insert(0, "\U0001F600x"); err != nil {
		t.Fatal(err)
	}
	if err := text.DeleteUTF16(2, -1); !errors.Is(err, crdt.ErrOutOfRange) {
		t.Fatalf("DeleteUTF16(2, -1) = %v, want ErrOutOfRange", err)
	}
	if got := text.String(); got != "\U0001F600x" {
		t.Fatalf("a refused delete changed the text to %q", got)
	}
}

// rejoin.go:113. A client whose context ends is cancelled, not down: nothing is
// to be retried, and it does not say it has lost its carrier on its way out.
func TestACancelledClientIsNotMarkedDown(t *testing.T) {
	// Bounded, so that a server that never answers fails the join and not the
	// suite's own timeout.
	ctx, cancel := context.WithTimeout(t.Context(), 2*promptWait)
	defer cancel()
	srv := NewServer(Config{Store: NewMemoryStore()})
	t.Cleanup(func() { _ = srv.Close(context.Background()) })
	link := &gatedLink{srv: srv, ctx: ctx}
	c, err := JoinWithRetry(ctx, link.dial, ClientConfig{Document: "paper", Site: 1},
		RetryPolicy{Wait: time.Second, Ceiling: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case <-c.finished:
	case <-time.After(promptWait):
		t.Fatal("the client did not finish after its context was cancelled")
	}
	c.mu.Lock()
	down := c.down
	c.mu.Unlock()
	if down {
		t.Error("a cancelled client was marked down, as if its carrier had been lost")
	}
}
