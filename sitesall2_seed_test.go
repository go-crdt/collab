//go:build !js

package collab

import (
	"context"
	"testing"
	"time"
)

// follow.go:660. A snapshot adopted from a peer names the sites that wrote in
// it, and each is owed an acknowledgement before anything of its is collected:
// it is recorded as one the server has not heard from.
func TestASeededSnapshotNamesItsWritersAsSitesToHearFrom(t *testing.T) {
	s := NewServer(Config{Store: NewMemoryStore()})
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	d, link := joinedTo(t, s, "paper", 9001)

	d.mu.Lock()
	_, before := d.seen[4]
	d.mu.Unlock()
	if before {
		t.Fatal("site 4 was known before the snapshot named it")
	}

	if err := d.seed(link, writtenSnapshot(t, 4, "written by site four")); err != nil {
		t.Fatal(err)
	}
	d.mu.Lock()
	_, after := d.seen[4]
	dirty := d.sitesDirty
	d.mu.Unlock()
	if !after {
		t.Fatal("a site the snapshot names is not among those the server is waiting to hear from")
	}
	if !dirty {
		t.Fatal("the new site was recorded and not marked for saving")
	}
}

// durability.go:242. The hook between opening a document and joining it is
// called, and the session ends up on the replica that replaced the one let go
// of: the test that reaches the window must be able to tell that it did.
func TestTheWindowBetweenOpeningAndJoiningIsOpened(t *testing.T) {
	s := NewServer(Config{})
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	s.EvictBetweenOpenAndJoin(context.Background(), time.Minute)

	inner := s.betweenOpenAndJoin
	var calls int
	var first *document
	s.betweenOpenAndJoin = func(d *document) {
		calls++
		if first == nil {
			first = d
		}
		inner(d)
	}

	doc, _, err := s.openAndJoin(context.Background(), joinMsg{Document: "doc", Site: 1})
	if err != nil {
		t.Fatalf("openAndJoin: %v", err)
	}
	if calls != 2 {
		t.Fatalf("the window was opened %d times, want 2: once evicted, once asking again", calls)
	}
	first.mu.Lock()
	evicted := first.evicted
	first.mu.Unlock()
	if !evicted || first == doc {
		t.Fatal("the first replica was not the one let go of, or the session joined it")
	}
}
