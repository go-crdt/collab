//go:build !js

package collab

import (
	"context"
	"testing"
	"time"

	"github.com/go-crdt/crdt"
)

// The site set a document collects against lives in memory, and a document that
// is evicted comes back without it.
//
// So the same divergence has a second door. Everybody leaves; the document goes
// idle and is let go of; one participant comes back, edits, deletes and
// acknowledges; and the server, which now remembers nobody else, collects
// against that one participant alone. The one that was away comes back to a
// superseded run where the deletion was, and goes on holding a value nobody
// else has — with a version vector equal to the server's.
//
// A restart is the same door: a server that has just started remembers nobody
// either.
func TestCollectingAfterTheDocumentWasLetGoOf(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := NewServer(Config{
		Store:      NewMemoryStore(),
		EvictAfter: 5 * time.Millisecond,
	})
	defer func() { _ = srv.Close(context.Background()) }()

	linkA := &gatedLink{srv: srv, ctx: ctx}
	a, err := JoinWithRetry(ctx, linkA.dial, ClientConfig{Document: "paper", Site: 1},
		RetryPolicy{Wait: time.Millisecond, Ceiling: 5 * time.Millisecond})
	if err != nil {
		t.Fatalf("A could not join: %v", err)
	}
	defer func() { _ = a.Close() }()

	linkB := &gatedLink{srv: srv, ctx: ctx}
	b, err := JoinWithRetry(ctx, linkB.dial, ClientConfig{Document: "paper", Site: 2},
		RetryPolicy{Wait: time.Millisecond, Ceiling: 5 * time.Millisecond})
	if err != nil {
		t.Fatalf("B could not join: %v", err)
	}
	defer func() { _ = b.Close() }()

	cellsA, err := a.Map("cells")
	if err != nil {
		t.Fatal(err)
	}
	cellsB, err := b.Map("cells")
	if err != nil {
		t.Fatal(err)
	}
	if err := cellsA.Set("k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	// B writes too, because that is what a participant in a document does and
	// because it is what puts B's site in the document's own version vector --
	// the only durable record that B was ever here. See sitesIn.
	if err := cellsB.Set("b", []byte("b")); err != nil {
		t.Fatal(err)
	}
	until(t, "B to see A's write", func() bool {
		v, ok := cellsB.Get("k")
		return ok && string(v) == "v"
	})
	until(t, "A to see B's", func() bool {
		_, ok := cellsA.Get("b")
		return ok
	})

	// Everybody goes away, holding what they have.
	linkA.away()
	linkB.away()
	until(t, "the document to be let go of", func() bool {
		srv.mu.Lock()
		defer srv.mu.Unlock()
		_, held := srv.docs["paper"]
		return !held
	})

	// A comes back to a document the server has just loaded, and is the only
	// participant it has ever heard of.
	linkA.back()
	until(t, "A to be connected again", func() bool {
		srv.mu.Lock()
		d := srv.docs["paper"]
		srv.mu.Unlock()
		if d == nil {
			return false
		}
		d.mu.Lock()
		defer d.mu.Unlock()
		return len(d.subs) == 1
	})
	if err := cellsA.Delete("k"); err != nil {
		t.Fatal(err)
	}
	document := func() *document {
		srv.mu.Lock()
		defer srv.mu.Unlock()
		return srv.docs["paper"]
	}
	until(t, "the server to hold the deletion", func() bool {
		d := document()
		d.mu.Lock()
		defer d.mu.Unlock()
		m, err := d.doc.Map("cells")
		if err != nil {
			return false
		}
		_, held := m.Get("k")
		return !held
	})
	until(t, "A to acknowledge it", func() bool {
		d := document()
		if d == nil {
			return false
		}
		_, ok := d.stable()
		return ok
	})

	document().collect()

	// And B comes back.
	linkB.back()
	until(t, "B to be connected again", func() bool {
		d := document()
		if d == nil {
			return false
		}
		d.mu.Lock()
		defer d.mu.Unlock()
		return len(d.subs) == 2
	})
	until(t, "B to have caught up with the server", func() bool {
		d := document()
		d.mu.Lock()
		server := d.doc.Version()
		d.mu.Unlock()
		return b.Version().Equal(server)
	})

	if v, held := cellsB.Get("k"); held {
		t.Fatalf("B came back holding k=%q, which A deleted while B was away and the document had been let go of; A holds it: %v; the two versions are equal: %v",
			v, mapHolds(cellsA, "k"), a.Version().Equal(b.Version()))
	}
}

// The server's own replica is not a participant and never acknowledges, so a
// site set that named it would hold collection back for ever. Nothing this
// server writes carries that site — but an operation arriving from a peer says
// whose it is, and a peer is free to say anything. So the guard is here rather
// than argued away, and this is it working.
func TestTheServersOwnSiteIsNotAParticipant(t *testing.T) {
	doc := crdt.NewComposite(serverSite)
	cells, err := doc.Map("cells")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cells.Set("k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	if _, named := doc.Version()[crdt.Part{Kind: crdt.PartMap, Name: "cells"}][serverSite]; !named {
		t.Fatal("the fixture did not put the server's site in the version vector")
	}
	if got := sitesIn(doc); len(got) != 0 {
		t.Fatalf("sitesIn = %v, want the server's own site left out", got)
	}
}

// A participant that has only ever read is remembered too, when the store can
// remember it.
//
// The document's own version vector names everyone who has written here, which
// is what a document that has been let go of comes back knowing. It does not
// name a reader: nothing a reader did is in any version vector. So before a
// store could be asked to keep the participants, a reader that was away across
// an eviction was collected past, and came back holding a value everybody else
// had removed.
func TestAReaderIsRememberedAcrossAnEviction(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := NewMemoryStore()
	if _, keeps := Store(store).(SiteStore); !keeps {
		t.Fatal("the fixture needs a store that can keep participants")
	}
	srv := NewServer(Config{Store: store, EvictAfter: 5 * time.Millisecond})
	defer func() { _ = srv.Close(context.Background()) }()

	linkA := &gatedLink{srv: srv, ctx: ctx}
	author, err := JoinWithRetry(ctx, linkA.dial, ClientConfig{Document: "paper", Site: 1},
		RetryPolicy{Wait: time.Millisecond, Ceiling: 5 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = author.Close() }()

	// A reader, which writes nothing at all.
	linkR := &gatedLink{srv: srv, ctx: ctx}
	reader, err := JoinWithRetry(ctx, linkR.dial, ClientConfig{Document: "paper", Site: 2},
		RetryPolicy{Wait: time.Millisecond, Ceiling: 5 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Close() }()

	cells, err := author.Map("cells")
	if err != nil {
		t.Fatal(err)
	}
	theirs, err := reader.Map("cells")
	if err != nil {
		t.Fatal(err)
	}
	if err := cells.Set("k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	until(t, "the reader to see it", func() bool {
		v, held := theirs.Get("k")
		return held && string(v) == "v"
	})

	// Everybody leaves, and the document is let go of.
	linkA.away()
	linkR.away()
	until(t, "the document to be let go of", func() bool {
		srv.mu.Lock()
		defer srv.mu.Unlock()
		_, held := srv.docs["paper"]
		return !held
	})

	// The author comes back to a document the server has just loaded, deletes
	// the key, and acknowledges.
	linkA.back()
	document := func() *document {
		srv.mu.Lock()
		defer srv.mu.Unlock()
		return srv.docs["paper"]
	}
	until(t, "the author to be connected again", func() bool {
		d := document()
		if d == nil {
			return false
		}
		d.mu.Lock()
		defer d.mu.Unlock()
		return len(d.subs) == 1
	})
	if err := cells.Delete("k"); err != nil {
		t.Fatal(err)
	}
	until(t, "the server to hold the deletion", func() bool {
		d := document()
		d.mu.Lock()
		defer d.mu.Unlock()
		m, err := d.doc.Map("cells")
		return err == nil && m.Tombstones() == 1
	})

	// It must not be collected: the reader is one of the participants this
	// document has had, and it has said nothing since it went away.
	for range 10 {
		document().collect()
	}
	d := document()
	d.mu.Lock()
	m, _ := d.doc.Map("cells")
	left := m.Tombstones()
	d.mu.Unlock()
	if left != 1 {
		t.Fatal("the tombstone was given back while a reader that had been here was away")
	}

	// And the reader comes back to a key that is gone.
	linkR.back()
	until(t, "the reader to agree that the key is gone", func() bool {
		_, held := theirs.Get("k")
		return !held
	})
}

// plainStore is a Store and nothing more: it keeps documents and cannot be
// asked to keep anybody.
type plainStore struct{ inner *MemoryStore }

func (p plainStore) Load(ctx context.Context, document string) ([]byte, error) {
	return p.inner.Load(ctx, document)
}

func (p plainStore) Save(ctx context.Context, document string, snapshot []byte) error {
	return p.inner.Save(ctx, document, snapshot)
}

func (p plainStore) Idle(ctx context.Context, d time.Duration) ([]string, error) {
	return p.inner.Idle(ctx, d)
}

// And with a store that cannot keep them, the reader is not remembered. This is
// the fallback [SiteStore] documents, pinned rather than described: a store that
// does not implement it leaves the behaviour that was there before, and that
// behaviour has this hole in it.
func TestAReaderIsNotRememberedByAStoreThatCannotKeepThem(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := plainStore{inner: NewMemoryStore()}
	if _, keeps := Store(store).(SiteStore); keeps {
		t.Fatal("the fixture needs a store that cannot keep participants")
	}
	srv := NewServer(Config{Store: store, EvictAfter: 5 * time.Millisecond})
	defer func() { _ = srv.Close(context.Background()) }()

	linkA := &gatedLink{srv: srv, ctx: ctx}
	author, err := JoinWithRetry(ctx, linkA.dial, ClientConfig{Document: "paper", Site: 1},
		RetryPolicy{Wait: time.Millisecond, Ceiling: 5 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = author.Close() }()
	linkR := &gatedLink{srv: srv, ctx: ctx}
	reader, err := JoinWithRetry(ctx, linkR.dial, ClientConfig{Document: "paper", Site: 2},
		RetryPolicy{Wait: time.Millisecond, Ceiling: 5 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Close() }()

	cells, err := author.Map("cells")
	if err != nil {
		t.Fatal(err)
	}
	theirs, err := reader.Map("cells")
	if err != nil {
		t.Fatal(err)
	}
	if err := cells.Set("k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	until(t, "the reader to see it", func() bool {
		v, held := theirs.Get("k")
		return held && string(v) == "v"
	})

	linkA.away()
	linkR.away()
	until(t, "the document to be let go of", func() bool {
		srv.mu.Lock()
		defer srv.mu.Unlock()
		_, held := srv.docs["paper"]
		return !held
	})

	linkA.back()
	document := func() *document {
		srv.mu.Lock()
		defer srv.mu.Unlock()
		return srv.docs["paper"]
	}
	until(t, "the author to be connected again", func() bool {
		d := document()
		if d == nil {
			return false
		}
		d.mu.Lock()
		defer d.mu.Unlock()
		return len(d.subs) == 1
	})
	if err := cells.Delete("k"); err != nil {
		t.Fatal(err)
	}
	until(t, "the server to hold the deletion", func() bool {
		d := document()
		d.mu.Lock()
		defer d.mu.Unlock()
		m, err := d.doc.Map("cells")
		return err == nil && m.Tombstones() == 1
	})

	// The reader is not among the participants this document came back
	// knowing, so nothing holds the floor at its version and the tombstone goes.
	until(t, "the tombstone to be given back", func() bool {
		document().collect()
		d := document()
		d.mu.Lock()
		defer d.mu.Unlock()
		m, err := d.doc.Map("cells")
		return err == nil && m.Tombstones() == 0
	})
}
