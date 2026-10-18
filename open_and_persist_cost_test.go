//go:build !js

// The helpers these tests use (serve, join, body, text, awaitFor) live in
// collab_test.go, which is !js: there is no in-process server to dial from a
// browser. Without the same constraint here the js/wasm lane fails to compile
// -- `undefined: serve` -- and the lane is the only thing that says so, since a
// native `go test` never reads this file for that configuration.
package collab_test

import (
	"context"
	"sync"
	"testing"

	"github.com/go-crdt/collab"
)

// Two refusals in server.go survived a mutation sweep of its 24 on 2026-10-11,
// and neither is a bug. Both are decisions about WORK, so neither can be reached
// by asserting an answer -- which is why nothing held them.
//
// Server.open's fast path. Deleting `if ok { return existing, nil }` is safe,
// and the reason is thirty lines further down: the insert is double-checked
// under the lock, `if existing, ok := s.docs[name]; ok { return existing, nil }`,
// so a second caller still gets the one document. What it loses is the point of
// the fast path -- every open of a document already open re-reads the snapshot
// AND the participants from the store. Measured below: once rather than twice
// for two joins, and the second join is every join after the first.
//
// document.persist's. Deleting `if !d.dirty && !(keeps && d.sitesDirty)` writes
// nothing wrong: the snapshot is guarded by `if snapshot != nil` further down,
// so a clean document cannot overwrite a good save with an empty one. What it
// loses is that a store which keeps participants is handed them again on every
// tick, whether or not anybody has arrived. With a plain Store the deletion
// changes nothing at all -- `keeps` is false and the function returns before
// writing -- so this test needs a SiteStore, which is why the stub below has one.

// tallyStore counts what it was asked to do, and keeps participants so that the
// cost of saving them can be seen at all.
type tallyStore struct {
	inner collab.Store

	mu         sync.Mutex
	loads      int
	loadSites  int
	saves      int
	savedSites int
	sites      map[string][]byte
}

func newTallyStore() *tallyStore {
	return &tallyStore{inner: collab.NewMemoryStore(), sites: map[string][]byte{}}
}

func (s *tallyStore) Load(ctx context.Context, document string) ([]byte, error) {
	s.mu.Lock()
	s.loads++
	s.mu.Unlock()
	return s.inner.Load(ctx, document)
}

func (s *tallyStore) Save(ctx context.Context, document string, snapshot []byte) error {
	s.mu.Lock()
	s.saves++
	s.mu.Unlock()
	return s.inner.Save(ctx, document, snapshot)
}

func (s *tallyStore) LoadSites(ctx context.Context, document string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.loadSites++
	return s.sites[document], nil
}

func (s *tallyStore) SaveSites(ctx context.Context, document string, sites []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.savedSites++
	s.sites[document] = append([]byte(nil), sites...)
	return nil
}

func (s *tallyStore) counts() (loads, loadSites, saves, savedSites int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.loads, s.loadSites, s.saves, s.savedSites
}

// A document already open is not read from the store again.
func TestOpeningADocumentTwiceReadsTheStoreOnce(t *testing.T) {
	store := newTallyStore()
	srv, conn := serve(t, collab.Config{Store: store})
	_ = srv

	first := join(t, conn, collab.ClientConfig{Document: "doc", Site: 1})
	if err := body(t, first).Insert(0, "hello"); err != nil {
		t.Fatal(err)
	}
	loads, loadSites, _, _ := store.counts()
	// The control: the first join DID read it, so a zero below would otherwise
	// mean the store was never consulted at all.
	if loads != 1 {
		t.Fatalf("the first join read the store %d times, want 1", loads)
	}
	if loadSites != 1 {
		t.Fatalf("the first join read the participants %d times, want 1", loadSites)
	}

	second := join(t, conn, collab.ClientConfig{Document: "doc", Site: 2})
	awaitFor(t, second, "the first participant's text", settleJS, func() bool {
		return text(t, second) == "hello"
	})

	loads, loadSites, _, _ = store.counts()
	if loads != 1 {
		t.Errorf("two joins read the snapshot %d times, want 1: every join after the first "+
			"is paying for a document the server already holds", loads)
	}
	if loadSites != 1 {
		t.Errorf("two joins read the participants %d times, want 1", loadSites)
	}
}

// A document with nothing to save is not written.
func TestFlushingAnUnchangedDocumentWritesNothing(t *testing.T) {
	store := newTallyStore()
	srv, conn := serve(t, collab.Config{Store: store})

	c := join(t, conn, collab.ClientConfig{Document: "doc", Site: 1})
	if err := body(t, c).Insert(0, "hello"); err != nil {
		t.Fatal(err)
	}
	// Waiting on c's own text proves nothing: a client holds its own insertion
	// the moment it makes it, whatever the server has. A SECOND participant
	// seeing it is the witness that the server does -- which the first version
	// of this test got wrong, and its own control said so ("the first flush
	// saved nothing").
	witness := join(t, conn, collab.ClientConfig{Document: "doc", Site: 2})
	awaitFor(t, witness, "the edit to reach the server", settleJS, func() bool {
		return text(t, witness) == "hello"
	})

	ctx := context.Background()
	if err := srv.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	_, _, saves, savedSites := store.counts()
	// The control, again first: the flush that HAD something to write did write,
	// so the zero below is about there being nothing left rather than about
	// Flush never writing.
	if saves == 0 {
		t.Fatal("the first flush saved nothing, so this test cannot see a save it should not make")
	}
	if savedSites == 0 {
		t.Fatal("the first flush recorded no participants, so the SiteStore half is not exercised")
	}

	before, beforeSites := saves, savedSites
	for i := range 3 {
		if err := srv.Flush(ctx); err != nil {
			t.Fatalf("flush %d: %v", i+2, err)
		}
	}
	_, _, saves, savedSites = store.counts()
	if saves != before {
		t.Errorf("three flushes with nothing changed wrote the snapshot %d more time(s)", saves-before)
	}
	if savedSites != beforeSites {
		t.Errorf("three flushes with nothing changed wrote the participants %d more time(s): "+
			"a store that keeps them is paying on every tick for a document nobody touched", savedSites-beforeSites)
	}
}

// The other two survivors of that sweep are named here rather than pinned.
//
//	wire.go:186, copied's empty-slice path. It returns (nil, true) for an
//	empty frame, and deleting it reaches `append([]byte(nil), got...)` with
//	nothing to append -- which returns nil. The same value, allocated no
//	differently. Run under `mutate -expect-pass`: the suite still passes.
//
//	server.go:765, the stream reader's return after an error. I first read this
//	as a goroutine left spinning on a dead stream, which would have been worth
//	a test. Reading it again, it is not: after the error the loop sends one more
//	message and then leaves through `case <-done`, which the consumer closes on
//	seeing that error. The cost is a bounded extra turn, not an unbounded one,
//	and the suite does not see it either. Named with the correction rather than
//	with the first guess, because the first guess is what a reader would
//	otherwise inherit.

// ⛔ An edit that produced NOTHING is not published, and the saving is why.
//
// parts.go:103, `if batch.Text == nil && batch.List == nil && batch.Map ==
// nil`, survived a mutation sweep of the root package's remaining 32 refusals
// on 2026-10-17. It reads like a cost guard -- a frame nobody needs -- and it
// is not: an empty batch reaches the server, the server marks the document
// dirty, and the next tick writes the WHOLE snapshot. Measured below, with the
// guard deleted: one no-op edit, one more save of everything.
//
// The inputs are not contrived. `Insert(pos, "")` is what a text widget does
// for a keystroke that produced no text -- an IME composition that has not
// resolved, a paste of an empty clipboard, a programmatic edit of a selection
// that turned out to be empty -- and `Delete(pos, 0)` is the same for a
// deletion. Every one of those would otherwise cost a full snapshot write.
func TestAnEditThatProducedNothingIsNotPublished(t *testing.T) {
	store := newTallyStore()
	srv, conn := serve(t, collab.Config{Store: store})

	c := join(t, conn, collab.ClientConfig{Document: "doc", Site: 1})
	// Not named `text`: that is the helper this file's witness check calls,
	// and a local of the same name shadows it.
	doc := body(t, c)
	if err := doc.Insert(0, "hello"); err != nil {
		t.Fatal(err)
	}
	// A second participant is the witness that the server has the edit; the
	// editing client holds its own insertion whatever the server has.
	witness := join(t, conn, collab.ClientConfig{Document: "doc", Site: 2})
	awaitFor(t, witness, "the edit to reach the server", settleJS, func() bool {
		return text(t, witness) == "hello"
	})

	ctx := context.Background()
	if err := srv.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	_, _, saves, _ := store.counts()
	// The control, first: a REAL edit was saved, so a zero below would
	// otherwise mean the store is never written at all.
	if saves == 0 {
		t.Fatal("the flush after a real edit saved nothing, so this test cannot see a save it should not make")
	}

	// Now the edits that produce no operations. Neither is an error: asking to
	// insert nothing is not a failure, and a client told otherwise would learn
	// to ignore the error.
	if err := doc.Insert(0, ""); err != nil {
		t.Errorf("inserting nothing was reported as a failure: %v", err)
	}
	if err := doc.Delete(0, 0); err != nil {
		t.Errorf("deleting nothing was reported as a failure: %v", err)
	}
	if err := srv.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	_, _, after, _ := store.counts()
	if after != saves {
		t.Errorf("two edits that produced no operations cost %d more snapshot write(s): an empty batch "+
			"reaches the server, marks the document dirty, and the next tick writes everything", after-saves)
	}
	// And the text is what it was, which says the no-ops were no-ops rather
	// than refused somewhere that also dropped a real edit.
	if got := doc.String(); got != "hello" {
		t.Errorf("the text is %q after two no-op edits", got)
	}
}
