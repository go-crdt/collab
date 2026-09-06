//go:build !js

package collab

import (
	"context"
	"testing"
	"time"

	"github.com/go-crdt/crdt"
)

// A server in a federation collects, because a link says what it holds.
//
// A link is a participant of the document it follows: it joins that server, and
// [Server.Follow] joins its own as well. A participant that never says anything
// holds a document's floor at nothing for ever, so before the link acknowledged,
// Config.CollectEvery did nothing at all in a federation — silently, which is
// the worst way for a setting to do nothing.
//
// What a link promises is what its own server has applied, not what everybody
// behind it has. That is enough, and the next test is why.
func TestAFederatedServerCollectsOnceTheLinkSaysWhatItHolds(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	lyon := NewServer(Config{Store: NewMemoryStore()})
	defer func() { _ = lyon.Close(context.Background()) }()
	paris := NewServer(Config{Store: NewMemoryStore()})
	defer func() { _ = paris.Close(context.Background()) }()

	tr, sc := Pipe()
	go func() { _ = paris.ServePipe(ctx, sc) }()
	author, err := Join(ctx, tr, ClientConfig{Document: "paper", Site: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = author.Close() }()
	cells, err := author.Map("cells")
	if err != nil {
		t.Fatal(err)
	}

	document := func(s *Server) *document {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.docs["paper"]
	}
	tombstones := func(s *Server) int {
		d := document(s)
		if d == nil {
			return -1
		}
		d.mu.Lock()
		defer d.mu.Unlock()
		m, err := d.doc.Map("cells")
		if err != nil {
			return -1
		}
		return m.Tombstones()
	}

	link := &directDial{srv: paris, ctx: ctx}
	go func() { _ = lyon.Follow(ctx, link, "paper", crdt.SiteID(9001)) }()
	until(t, "the link to reach Paris", func() bool {
		d := document(paris)
		if d == nil {
			return false
		}
		d.mu.Lock()
		defer d.mu.Unlock()
		return len(d.subs) == 2
	})

	if err := cells.Set("k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	// The tombstone has to be there before its going away means anything: an
	// empty document has none either, and a test that cannot tell those apart
	// passes whatever the code does. This one did, until it was checked against
	// the change reverted.
	until(t, "Paris to hold the write", func() bool { return tombstones(paris) == 0 && len(cells.Keys()) == 1 })
	if err := cells.Delete("k"); err != nil {
		t.Fatal(err)
	}
	until(t, "Paris to hold the deletion", func() bool { return tombstones(paris) == 1 })

	// Collecting is asked for rather than left to a timer. A timer would be
	// racing this test: it could take the tombstone away between the deletion
	// landing and the assertion seeing it, and a test that loses that race says
	// the opposite of what it means. Measured before this was written that way:
	// one run in six.
	// Asked for until it happens, rather than once. The link acknowledges after
	// applying what it was sent, so its answer arrives when it arrives: the
	// first ask can be too early, and waiting for it to have said *something*
	// is not the same as waiting for it to have said something recent enough.
	// Before the link acknowledged at all, no number of asks would do.
	until(t, "Paris to give the tombstone back", func() bool {
		document(paris).collect()
		return tombstones(paris) == 0
	})
}

// And a participant behind the link is not collected past — by EITHER server,
// which is what the name says and what this test used to get wrong.
//
// It asserted that Paris gives the tombstone back while a reader on Lyon is
// away holding the value, on the argument that "the server they ask still holds
// what it told the peer it had". That argument has an unstated precondition:
// the server they ask. Federation exists so that a participant whose server is
// down can come back on the OTHER one, and a reader that did was answered with
// a superseded run over the stretch Paris had collected, went on showing a
// value everybody else had removed, and its version matched Paris's, so no
// rejoin would ever repair it.
//
// A link therefore promises what its own server could COLLECT against, not what
// its replica holds, and while somebody behind it is quiet it promises nothing.
// So neither server collects until the reader is back and has said where it is
// — and then both do, which is the half that keeps this from passing on a
// federation that collects nothing ever.
func TestAParticipantBehindALinkIsNotCollectedPast(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	lyon := NewServer(Config{Store: NewMemoryStore()})
	defer func() { _ = lyon.Close(context.Background()) }()
	paris := NewServer(Config{Store: NewMemoryStore()})
	defer func() { _ = paris.Close(context.Background()) }()

	tr, sc := Pipe()
	go func() { _ = paris.ServePipe(ctx, sc) }()
	author, err := Join(ctx, tr, ClientConfig{Document: "paper", Site: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = author.Close() }()
	cells, err := author.Map("cells")
	if err != nil {
		t.Fatal(err)
	}

	link := &directDial{srv: paris, ctx: ctx}
	go func() { _ = lyon.Follow(ctx, link, "paper", crdt.SiteID(9001)) }()

	// A reader on Lyon, which takes the value and then goes away holding it.
	away := &gatedLink{srv: lyon, ctx: ctx}
	reader, err := JoinWithRetry(ctx, away.dial, ClientConfig{Document: "paper", Site: 2},
		RetryPolicy{Wait: time.Millisecond, Ceiling: 5 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Close() }()
	theirs, err := reader.Map("cells")
	if err != nil {
		t.Fatal(err)
	}
	if err := cells.Set("k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	until(t, "the reader on Lyon to see it", func() bool {
		v, held := theirs.Get("k")
		return held && string(v) == "v"
	})
	away.away()

	// Paris deletes it and gives the tombstone back, which it may: the link has
	// told it that Lyon holds the deletion.
	if err := cells.Delete("k"); err != nil {
		t.Fatal(err)
	}
	document := func(s *Server) *document {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.docs["paper"]
	}
	tombstones := func(s *Server) int {
		d := document(s)
		if d == nil {
			return -1
		}
		d.mu.Lock()
		defer d.mu.Unlock()
		m, err := d.doc.Map("cells")
		if err != nil {
			return -1
		}
		return m.Tombstones()
	}
	until(t, "Paris to hold the deletion", func() bool { return tombstones(paris) == 1 })
	until(t, "Lyon to hold it too", func() bool { return tombstones(lyon) == 1 })

	// Neither may give it back while the reader is away, however often it is
	// asked. collect() is synchronous, so this is a decision being made ten
	// times over rather than a race being waited on.
	for range 10 {
		document(paris).collect()
		document(lyon).collect()
	}
	if got := tombstones(lyon); got != 1 {
		t.Fatalf("Lyon gave back the tombstone its own reader has not delivered: %d left", got)
	}
	if got := tombstones(paris); got != 1 {
		t.Fatalf("Paris gave back a tombstone a reader on Lyon is still holding the value against: %d left", got)
	}
	// And the mechanism, so that this is not merely an absence: what the link
	// promised Paris is behind what Paris holds. The promise it made before the
	// reader went away still stands and is not withdrawn -- it was true when it
	// was made -- but it does not cover the deletion, and the link has not
	// promised anything since, because Lyon would not collect against it
	// either.
	d := document(paris)
	d.mu.Lock()
	promised := d.seen[crdt.SiteID(9001)]
	held := d.doc.Version()
	d.mu.Unlock()
	cellsPart := crdt.Part{Kind: crdt.PartMap, Name: "cells"}
	if promised == nil {
		t.Fatal("the link never promised anything at all, so this proves nothing about the promise")
	}
	if promised[cellsPart][1] >= held[cellsPart][1] {
		t.Fatalf("the link promised %v, which covers the deletion Paris holds at %v", promised, held)
	}

	// The reader comes back to a key that is gone, which is the whole point...
	away.back()
	until(t, "the reader to agree that the key is gone", func() bool {
		_, held := theirs.Get("k")
		return !held
	})
	// ...and now that it has said where it is, both servers may collect. This
	// is the control: without it the test would pass on a federation that had
	// simply stopped collecting.
	// The reader says where it is by writing, which is what a participant that
	// has come back does. An acknowledgement rides with it.
	if err := theirs.Set("mine", []byte("back")); err != nil {
		t.Fatal(err)
	}
	until(t, "the author to see the reader's write", func() bool {
		v, held := cells.Get("mine")
		return held && string(v) == "back"
	})
	until(t, "Lyon to give the tombstone back once its reader is back", func() bool {
		document(lyon).collect()
		return tombstones(lyon) == 0
	})
	// One more thing said across the link, because the promise rides on
	// traffic: a link re-promises when it relays or receives, so a federation
	// that fell silent in the moment the reader's acknowledgement landed tells
	// its peer on the next thing anybody says. Collection is an economy, not a
	// correctness requirement, so being a round late costs a tombstone and
	// nothing else -- and a timer to shave that round would be machinery in
	// the wrong place.
	if err := cells.Set("later", []byte("something else")); err != nil {
		t.Fatal(err)
	}
	until(t, "the reader to see it", func() bool {
		v, held := theirs.Get("later")
		return held && string(v) == "something else"
	})
	until(t, "Paris to give the tombstone back once the link can promise", func() bool {
		document(paris).collect()
		return tombstones(paris) == 0
	})
}

// directDial hands out sessions on a server in this process.
type directDial struct {
	srv *Server
	ctx context.Context
}

func (d *directDial) open(ctx context.Context) (carrierConn, error) {
	transport, server := Pipe()
	go func() { _ = d.srv.ServePipe(d.ctx, server) }()
	return transport.open(ctx)
}
