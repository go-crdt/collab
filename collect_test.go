//go:build !js

package collab_test

import (
	"errors"
	"testing"
	"time"

	"github.com/go-crdt/collab"
	"github.com/go-crdt/crdt"
)

// A document being edited in a room gives back what everybody in it has
// certainly seen, while they are still in it.
//
// This is the whole point of the acknowledgements: a server can know a version
// every replica has delivered, and that is the one thing collecting asks for
// and a replica on its own cannot supply.
func TestADocumentInUseGivesBackWhatEverybodyHasSeen(t *testing.T) {
	store := collab.NewMemoryStore()
	srv, conn := serve(t, collab.Config{
		Store:        store,
		PersistEvery: 2 * time.Millisecond,
		CollectEvery: 2 * time.Millisecond,
	})
	ada := join(t, conn, collab.ClientConfig{Document: "paper", Site: 1})
	grace := join(t, conn, collab.ClientConfig{Document: "paper", Site: 2})

	// Written and revised the way a document is: each of them types a line and
	// takes the other's away, so whole runs die.
	const line = "a sentence somebody wrote. "
	for round := 0; round < 40; round++ {
		if err := body(t, ada).Insert(body(t, ada).Len(), line); err != nil {
			t.Fatal(err)
		}
		awaitText(t, grace, body(t, ada).String())
		if err := body(t, grace).Delete(0, len(line)); err != nil {
			t.Fatal(err)
		}
		awaitText(t, ada, body(t, grace).String())
	}
	want := body(t, ada).String()

	// The server saves what it holds; collecting has to make that smaller
	// without changing what it says.
	sizeOf := func() int {
		t.Helper()
		if err := srv.Flush(t.Context()); err != nil {
			t.Fatal(err)
		}
		raw, err := store.Load(t.Context(), "paper")
		if err != nil {
			t.Fatal(err)
		}
		return len(raw)
	}
	before := sizeOf()

	deadline := time.Now().Add(20 * time.Second)
	var after int
	for {
		after = sizeOf()
		if after < before {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the document never gave anything back: %d bytes throughout", before)
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Logf("a document two people worked on: %d bytes became %d (%.2fx), still saying the same thing",
		before, after, float64(before)/float64(after))

	// And it still says what it said, to both of them and to the store.
	if got := body(t, ada).String(); got != want {
		t.Fatalf("ada reads %d characters, want %d", len(got), len(want))
	}
	if got := body(t, grace).String(); got != want {
		t.Fatalf("grace reads %d characters, want %d", len(got), len(want))
	}
	raw, err := store.Load(t.Context(), "paper")
	if err != nil {
		t.Fatal(err)
	}
	doc, err := crdt.LoadComposite(99, raw)
	if err != nil {
		t.Fatalf("what was stored is unreadable: %v", err)
	}
	stored, err := doc.Text("body")
	if err != nil {
		t.Fatal(err)
	}
	if stored.String() != want {
		t.Fatal("what was stored says something else")
	}

	// Both of them carry on editing afterwards, which is the difference between
	// collecting and rewriting.
	if err := body(t, ada).Insert(0, "AFTER "); err != nil {
		t.Fatalf("editing after a collection: %v", err)
	}
	awaitText(t, grace, "AFTER "+want)
}

// A participant that was away while the document collected, and wrote nothing
// while away, is re-seeded rather than handed a difference it could not apply.
//
// This is the rule Loro states for its shallow snapshots: a peer whose version
// predates the trim point cannot be synced from it. What it can be is sent the
// whole document, which costs a snapshot and loses nothing.
func TestAParticipantThatWasAwayIsReseeded(t *testing.T) {
	store := collab.NewMemoryStore()
	srv, conn := serve(t, collab.Config{Store: store})
	ada := join(t, conn, collab.ClientConfig{Document: "paper", Site: 1})
	away := join(t, conn, collab.ClientConfig{Document: "paper", Site: 2})

	// Each of them writes a line of their own. A site typing straight on
	// extends the run it is already in, so two writers are what makes two runs
	// — and a run is collected whole or not at all.
	const line = "a sentence somebody wrote. "
	if err := body(t, ada).Insert(0, line); err != nil {
		t.Fatal(err)
	}
	awaitText(t, away, line)
	if err := body(t, away).Insert(body(t, away).Len(), "and one somebody else wrote. "); err != nil {
		t.Fatal(err)
	}
	awaitText(t, ada, line+"and one somebody else wrote. ")
	kept := away.Snapshot()
	if err := away.Close(); err != nil {
		t.Fatal(err)
	}

	// It writes while away, against the text as it was.
	offline, err := crdt.LoadComposite(2, kept)
	if err != nil {
		t.Fatal(err)
	}
	offlineBody, err := offline.Text("body")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := offlineBody.Insert(offlineBody.Len(), "and this, while away"); err != nil {
		t.Fatal(err)
	}

	// Meanwhile the room takes that participant's line away and carries on.
	if err := body(t, ada).Delete(len(line), len("and one somebody else wrote. ")); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		if err := body(t, ada).Insert(body(t, ada).Len(), line); err != nil {
			t.Fatal(err)
		}
	}
	collectSomething(t, srv, store, "paper")

	_, err = collab.Join(t.Context(), collab.GRPC(conn),
		collab.ClientConfig{Document: "paper", Site: 2, Resume: offline.Snapshot()})
	if !errors.Is(err, collab.ErrTooFarBehind) {
		t.Fatalf("joining with work that cannot merge = %v, want ErrTooFarBehind", err)
	}
}

// A participant that was away, wrote while away, and wrote against something the
// document has since collected is refused rather than served.
//
// Serving it would mean re-seeding, and re-seeding would throw away what that
// person wrote while they were away without saying so. Refusing is what lets an
// application do the one thing that is actually right: show somebody their own
// work and let them decide.
func TestAParticipantWhoseOfflineWorkCannotMergeIsRefused(t *testing.T) {
	store := collab.NewMemoryStore()
	srv, conn := serve(t, collab.Config{Store: store})
	ada := join(t, conn, collab.ClientConfig{Document: "paper", Site: 1})
	away := join(t, conn, collab.ClientConfig{Document: "paper", Site: 2})

	const line = "a sentence somebody wrote. "
	const theirs = "and one somebody else wrote. "
	if err := body(t, ada).Insert(0, line); err != nil {
		t.Fatal(err)
	}
	awaitText(t, away, line)
	if err := body(t, away).Insert(body(t, away).Len(), theirs); err != nil {
		t.Fatal(err)
	}
	awaitText(t, ada, line+theirs)
	kept := away.Snapshot()
	if err := away.Close(); err != nil {
		t.Fatal(err)
	}

	// It writes while away, against the text as it was.
	offline, err := crdt.LoadComposite(2, kept)
	if err != nil {
		t.Fatal(err)
	}
	offlineBody, err := offline.Text("body")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := offlineBody.Insert(offlineBody.Len(), "and this, while away"); err != nil {
		t.Fatal(err)
	}

	// Meanwhile the room takes that participant's line away and carries on
	// past where they left off.
	if err := body(t, ada).Delete(len(line), len(theirs)); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		if err := body(t, ada).Insert(body(t, ada).Len(), line); err != nil {
			t.Fatal(err)
		}
	}
	collectSomething(t, srv, store, "paper")

	_, err = collab.Join(t.Context(), collab.GRPC(conn),
		collab.ClientConfig{Document: "paper", Site: 2, Resume: offline.Snapshot()})
	if !errors.Is(err, collab.ErrTooFarBehind) {
		t.Fatalf("joining with work that cannot merge = %v, want ErrTooFarBehind", err)
	}
}

// collectSomething waits until everybody in the document has vouched for what
// they hold, then collects — and insists that tombstones actually went, so that
// a test which depends on a collection having happened cannot pass because none
// did.
//
// Tombstones rather than bytes: what collection adds to the header — the floor
// and the per-site tallies — is a fixed cost, and on a document of a few
// hundred characters it can be most of what the collection saved. The document
// gets smaller where it matters and the file does not always, which is worth
// knowing and not worth asserting.
func collectSomething(t *testing.T, srv *collab.Server, store collab.Store, name string) {
	t.Helper()
	tombstones := func() int {
		t.Helper()
		if err := srv.Flush(t.Context()); err != nil {
			t.Fatal(err)
		}
		raw, err := store.Load(t.Context(), name)
		if err != nil {
			t.Fatal(err)
		}
		doc, err := crdt.LoadComposite(99, raw)
		if err != nil {
			t.Fatalf("what was stored is unreadable: %v", err)
		}
		body, err := doc.Text("body")
		if err != nil {
			t.Fatal(err)
		}
		return body.Tombstones()
	}
	// Wait for the deletion to reach the server before measuring: the edits
	// that made it were sent, not applied, and on a loaded machine the
	// difference is visible. Measuring first and finding nothing said more
	// about the machine than about the collection.
	deadline := time.Now().Add(20 * time.Second)
	before := tombstones()
	for before == 0 {
		if time.Now().After(deadline) {
			t.Fatalf("nothing that was deleted in %q ever reached the server, so nothing was tested", name)
		}
		time.Sleep(2 * time.Millisecond)
		before = tombstones()
	}
	for {
		if _, ok := srv.Stable(name); ok {
			srv.CollectNow()
			if tombstones() < before {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("nothing was ever collected from %q, so nothing below was tested", name)
		}
		time.Sleep(2 * time.Millisecond)
	}
}
