//go:build !js

package collab

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/go-crdt/crdt"
)

func TestWhatADocumentRemembersAboutWhoHasBeenInIt(t *testing.T) {
	cells := crdt.Part{Kind: crdt.PartMap, Name: "cells"}
	seen := map[crdt.SiteID]crdt.CompositeVersion{
		1: {cells: crdt.VersionVector{1: 4}},
		7: nil, // joined, and has said nothing
		3: {cells: crdt.VersionVector{1: 2, 3: 9}},
	}
	reached := map[crdt.SiteID]crdt.CompositeClocks{
		1: {cells: 11},
		3: {cells: 9},
	}

	raw, err := encodeSites(seen, reached)
	if err != nil {
		t.Fatal(err)
	}
	gotSeen, gotReached, err := decodeSites(raw)
	if err != nil {
		t.Fatalf("what was just written did not read back: %v", err)
	}
	if len(gotSeen) != 3 {
		t.Fatalf("read back %d sites, wrote 3", len(gotSeen))
	}
	if v, named := gotSeen[7]; !named || v != nil {
		t.Fatalf("the site that has said nothing came back as %v, named=%v", v, named)
	}
	if gotSeen[1][cells][1] != 4 || gotSeen[3][cells][3] != 9 {
		t.Fatalf("versions came back as %v", gotSeen)
	}
	if gotReached[1][cells] != 11 || gotReached[3][cells] != 9 {
		t.Fatalf("clocks came back as %v", gotReached)
	}
	if _, said := gotReached[7]; said {
		t.Fatal("a site that has said nothing came back having said something")
	}

	// Sorted, so two servers holding the same thing write the same bytes.
	again, err := encodeSites(map[crdt.SiteID]crdt.CompositeVersion{
		3: {cells: crdt.VersionVector{1: 2, 3: 9}},
		1: {cells: crdt.VersionVector{1: 4}},
		7: nil,
	}, reached)
	if err != nil {
		t.Fatal(err)
	}
	if string(again) != string(raw) {
		t.Fatal("the same participants encoded to different bytes")
	}

	// Nothing that comes back from a store is trusted.
	for _, bad := range [][]byte{
		{},
		{2, 1, 0, 0},          // says two, gives one
		{2, 3, 0, 0, 1, 0, 0}, // sites out of order
		{2, 1, 0, 0, 1, 0, 0}, // one site named twice
		{1, 1, 9, 0, 0},       // a version longer than the message
		{1, 1, 1, 0xff, 0},    // a version that is not one
		{1, 1, 0, 1, 0xff},    // clocks that are not any
		{1, 1, 0, 0, 0, 7},    // bytes left over
	} {
		if _, _, err := decodeSites(bad); !errors.Is(err, crdt.ErrMalformed) {
			t.Fatalf("decodeSites(%v) = %v, want ErrMalformed", bad, err)
		}
	}
}

// A store that cannot be encoded for is reported rather than half-written.
func TestParticipantsThatCannotBeEncoded(t *testing.T) {
	bad := crdt.Part{Kind: crdt.PartKind(9), Name: "not a part"}
	if _, err := encodeSites(
		map[crdt.SiteID]crdt.CompositeVersion{1: {bad: crdt.VersionVector{1: 1}}}, nil,
	); err == nil {
		t.Fatal("a version naming a part that is not one was encoded")
	}
	if _, err := encodeSites(
		map[crdt.SiteID]crdt.CompositeVersion{1: nil},
		map[crdt.SiteID]crdt.CompositeClocks{1: {bad: 1}},
	); err == nil {
		t.Fatal("clocks naming a part that is not one were encoded")
	}
}

// refusingSites keeps documents and refuses to keep anybody, in whichever
// direction a test asks for.
type refusingSites struct {
	*MemoryStore
	load func() ([]byte, error)
	save error
}

func (r refusingSites) LoadSites(ctx context.Context, document string) ([]byte, error) {
	if r.load != nil {
		return r.load()
	}
	return r.MemoryStore.LoadSites(ctx, document)
}

func (r refusingSites) SaveSites(ctx context.Context, document string, sites []byte) error {
	if r.save != nil {
		return r.save
	}
	return r.MemoryStore.SaveSites(ctx, document, sites)
}

// A store that will not give the participants back, or gives back something
// that is not them, stops the document being opened. Neither is a document that
// does not exist, and opening one anyway would collect against a set nobody
// vouched for.
func TestADocumentWhoseParticipantsCannotBeReadDoesNotOpen(t *testing.T) {
	for _, tt := range []struct {
		name  string
		store Store
		want  string
	}{
		{
			"the store will not say",
			refusingSites{MemoryStore: NewMemoryStore(), load: func() ([]byte, error) {
				return nil, errors.New("the disk is gone")
			}},
			"reading the participants",
		},
		{
			"what it says is not participants",
			refusingSites{MemoryStore: NewMemoryStore(), load: func() ([]byte, error) {
				return []byte{9, 9, 9}, nil
			}},
			"unreadable",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			srv := NewServer(Config{Store: tt.store})
			defer func() { _ = srv.Close(context.Background()) }()
			// Asked of the server rather than through a session: an internal
			// error ends the session, and what reaches a client is that the
			// carrier closed. What is being checked is the reason it closed.
			_, err := srv.open(context.Background(), "paper")
			if err == nil {
				t.Fatal("the document opened")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("the error is %q, want it to say %q", err, tt.want)
			}
		})
	}
}

// And a store that will not keep them says so, rather than letting the server
// believe they are kept.
func TestAStoreThatWillNotKeepTheParticipantsSaysSo(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := refusingSites{MemoryStore: NewMemoryStore(), save: errors.New("no room for anybody")}
	srv := NewServer(Config{Store: store})
	defer func() { _ = srv.Close(context.Background()) }()

	tr, sc := Pipe()
	go func() { _ = srv.ServePipe(ctx, sc) }()
	author, err := Join(ctx, tr, ClientConfig{Document: "paper", Site: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = author.Close() }()
	cells, err := author.Map("cells")
	if err != nil {
		t.Fatal(err)
	}
	if err := cells.Set("k", []byte("v")); err != nil {
		t.Fatal(err)
	}

	srv.mu.Lock()
	d := srv.docs["paper"]
	srv.mu.Unlock()
	until(t, "the write to reach the server", func() bool {
		d.mu.Lock()
		defer d.mu.Unlock()
		return d.dirty
	})
	if err := d.persist(ctx); err == nil {
		t.Fatal("a store that would not keep the participants reported success")
	}
	// The snapshot went, and the participants are owed again rather than
	// forgotten.
	d.mu.Lock()
	owed := d.sitesDirty
	d.mu.Unlock()
	if !owed {
		t.Fatal("the participants were given up on rather than kept for the next save")
	}
}
