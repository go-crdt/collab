//go:build !js

package collab

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/go-crdt/crdt"
)

// gatedLink is a dialler that can be shut, so a participant can be taken off the
// air and kept off it while the room carries on without them. A cut carrier on
// its own is not enough: a supervised client redials in milliseconds, and what
// has to be arranged here is an absence long enough for the server to collect
// during it.
type gatedLink struct {
	srv *Server
	ctx context.Context

	mu   sync.Mutex
	shut bool
	conn carrierConn
}

func (g *gatedLink) dial(context.Context) (Transport, error) { return g, nil }

func (g *gatedLink) open(ctx context.Context) (carrierConn, error) {
	g.mu.Lock()
	shut := g.shut
	g.mu.Unlock()
	if shut {
		return nil, ErrPipeClosed
	}
	transport, server := Pipe()
	go func() { _ = g.srv.ServePipe(g.ctx, server) }()
	conn, err := transport.open(ctx)
	if err != nil {
		return nil, err
	}
	g.mu.Lock()
	g.conn = conn
	g.mu.Unlock()
	return conn, nil
}

func (g *gatedLink) away() {
	g.mu.Lock()
	g.shut = true
	conn := g.conn
	g.mu.Unlock()
	if conn != nil {
		_ = conn.Close()
	}
}

func (g *gatedLink) back() {
	g.mu.Lock()
	g.shut = false
	g.mu.Unlock()
}

// A participant that is away is still a participant, and collecting past one is
// a divergence that never heals.
//
// [crdt.Map.Collect] asks for a version every replica has delivered. Given one
// some replica has not, [crdt.Map.OpsSince] answers for the collected stretch
// with a superseded run — which is right under correct use, and which under
// misuse advances that replica's version vector over a deletion without ever
// telling it what the operation did. It goes on showing a value everybody else
// removed, with a version vector equal to the server's, so a rejoin has nothing
// left to send and the disagreement is permanent.
//
// This is two participants, one write, one deletion and one absence, and before
// the meet was taken over every site rather than every open session it ended
// with B holding a key A had deleted and the two versions equal.
func TestCollectingPastAnAbsentParticipant(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := NewServer(Config{Store: NewMemoryStore()})
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

	document := func() *document {
		srv.mu.Lock()
		defer srv.mu.Unlock()
		return srv.docs["paper"]
	}
	subscribers := func() int {
		d := document()
		if d == nil {
			return 0
		}
		d.mu.Lock()
		defer d.mu.Unlock()
		return len(d.subs)
	}

	if err := cellsA.Set("k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	until(t, "B to see the write", func() bool {
		v, ok := cellsB.Get("k")
		return ok && string(v) == "v"
	})

	// B goes away, holding what it has and knowing nothing of what follows.
	linkB.away()
	until(t, "the server to be down to one session", func() bool { return subscribers() == 1 })

	// A deletes the key, and acknowledges — so every participant the server can
	// see has certainly delivered the deletion, which is the version it used to
	// collect against.
	if err := cellsA.Delete("k"); err != nil {
		t.Fatal(err)
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
		_, ok := document().stable()
		return ok
	})

	document().collect()

	linkB.back()
	until(t, "B to be connected again", func() bool { return subscribers() == 2 })
	until(t, "B to have caught up with the server", func() bool {
		d := document()
		d.mu.Lock()
		server := d.doc.Version()
		d.mu.Unlock()
		return b.Version().Equal(server)
	})

	if v, held := cellsB.Get("k"); held {
		t.Fatalf("B came back holding k=%q, which A deleted while B was away; A holds it: %v; the two versions are equal: %v",
			v, mapHolds(cellsA, "k"), a.Version().Equal(b.Version()))
	}
}

func mapHolds(m *Map, key string) bool {
	_, ok := m.Get(key)
	return ok
}

// until waits for a condition, and names it when it does not arrive.
func until(t *testing.T, what string, ok func() bool) {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); {
		if ok() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// The rule collectable states, on its own: a document nobody has ever been in
// has no version to collect against, a site that has said nothing holds the
// answer back entirely, and otherwise the answer is the meet.
func TestCollectableIsTheMeetOverEverySiteSeen(t *testing.T) {
	part := crdt.Part{Kind: crdt.PartMap, Name: "cells"}

	d := &document{}
	if _, ok := d.collectable(); ok {
		t.Fatal("a document nobody has been in reported a version to collect against")
	}

	// One site, which has joined and acknowledged nothing.
	d.seen = map[crdt.SiteID]crdt.CompositeVersion{1: nil}
	if _, ok := d.collectable(); ok {
		t.Fatal("a site that has said nothing did not hold the answer back")
	}

	// It speaks, and is now the whole of the answer.
	d.seen[1] = crdt.CompositeVersion{part: crdt.VersionVector{1: 4, 2: 7}}
	got, ok := d.collectable()
	if !ok {
		t.Fatal("a room of one that has spoken reported nothing")
	}
	if got[part][1] != 4 || got[part][2] != 7 {
		t.Fatalf("collectable = %v, want what the only site acknowledged", got)
	}

	// A second site, further behind on one replica and ahead on the other: the
	// answer is the lower of each, which is what everybody has.
	d.seen[2] = crdt.CompositeVersion{part: crdt.VersionVector{1: 9, 2: 3}}
	got, ok = d.collectable()
	if !ok {
		t.Fatal("two sites that have both spoken reported nothing")
	}
	if got[part][1] != 4 || got[part][2] != 3 {
		t.Fatalf("collectable = %v, want the element-wise minimum", got)
	}

	// And one that goes quiet again — an absent site is still a site, which is
	// the whole point — keeps holding the answer where it left it.
	d.seen[3] = nil
	if _, ok := d.collectable(); ok {
		t.Fatal("a third site that has said nothing did not hold the answer back")
	}
}

// The rule clockFloor states, on its own: nobody here means no promise, a site
// that has not said where its clocks stand means no promise, and otherwise the
// answer is the least of what they said — part by part, with a part somebody
// does not have left out entirely, because that is a part they could write to
// next at clock one.
func TestTheClockFloorIsTheLeastEverybodyPromised(t *testing.T) {
	cells := crdt.Part{Kind: crdt.PartMap, Name: "cells"}
	notes := crdt.Part{Kind: crdt.PartMap, Name: "notes"}

	d := &document{doc: crdt.NewComposite(serverSite)}
	if _, ok := d.clockFloor(); ok {
		t.Fatal("a document nobody has been in promised something")
	}

	// One site, which has joined and said nothing about its clocks.
	d.seen = map[crdt.SiteID]crdt.CompositeVersion{1: nil}
	if _, ok := d.clockFloor(); ok {
		t.Fatal("a site that has said nothing did not hold the answer back")
	}

	// It speaks, and is the whole of the answer.
	d.reached = map[crdt.SiteID]crdt.CompositeClocks{1: {cells: 9, notes: 4}}
	got, ok := d.clockFloor()
	if !ok {
		t.Fatal("a room of one that has spoken promised nothing")
	}
	if got[cells] != 9 || got[notes] != 4 {
		t.Fatalf("clockFloor = %v, want what the only site said", got)
	}

	// A second site, further along on one part and behind on the other.
	d.seen[2] = nil
	d.reached[2] = crdt.CompositeClocks{cells: 3, notes: 11}
	got, _ = d.clockFloor()
	if got[cells] != 3 || got[notes] != 4 {
		t.Fatalf("clockFloor = %v, want the least of each", got)
	}

	// A third that does not have one of the parts at all: it could write to it
	// next at clock one, so nothing can be promised about it.
	d.seen[3] = nil
	d.reached[3] = crdt.CompositeClocks{cells: 20}
	got, ok = d.clockFloor()
	if !ok {
		t.Fatalf("clockFloor promised nothing at all: %v", got)
	}
	if _, named := got[notes]; named {
		t.Fatalf("clockFloor = %v, want the part somebody does not have left out", got)
	}
	if got[cells] != 3 {
		t.Fatalf("clockFloor = %v, want the least on the part everybody has", got)
	}

	// And when there is no part everybody has, there is nothing to promise.
	d.reached[3] = crdt.CompositeClocks{{Kind: crdt.PartMap, Name: "elsewhere"}: 20}
	if got, ok := d.clockFloor(); ok {
		t.Fatalf("clockFloor promised %v with no part in common", got)
	}

	// And a document whose version everybody agrees on but whose clocks nobody
	// has promised is left alone rather than collected against nothing.
	empty := crdt.CompositeVersion{}
	d.seen = map[crdt.SiteID]crdt.CompositeVersion{1: empty, 2: empty}
	if _, ok := d.collectable(); !ok {
		t.Fatal("the fixture is wrong: there is no version to collect against")
	}
	d.reached = nil
	d.collect()
}
