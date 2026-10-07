package collab

import (
	"context"
	"testing"
	"time"
)

// [Config.CollectEvery] is "off by default, and being off is not a failure of
// nerve". Off has to mean off: collecting drops map tombstones, and an operator
// who has not asked for that is entitled not to get it.
//
// One line holds it -- collectStable returns when collectEvery is not positive
// -- and nothing tested that line. Removing it leaves the whole suite green,
// because every other test that collects has the interval set, and
// [Server.CollectNow] sets the interval itself so that a test need not wait for
// a timer: it cannot see this.
//
// The control is the same document with the interval armed. Without it, "the
// floor did not move" would also be satisfied by a document nobody could
// collect anyway, which is the way a test like this passes for the wrong
// reason. Measured here: 0 after twenty passes with collection off, 30 after
// twenty with it on.
func TestCollectionOffMeansOff(t *testing.T) {
	srv := NewServer(Config{Store: NewMemoryStore()}) // CollectEvery unset
	t.Cleanup(func() { _ = srv.Close(context.Background()) })
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	transport, conn := Pipe()
	go func() { _ = srv.ServePipe(ctx, conn) }()
	ada, err := Join(ctx, transport, ClientConfig{Document: "paper", Site: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ada.Close() }()

	cells, err := ada.Map("cells")
	if err != nil {
		t.Fatal(err)
	}
	const keys = 20
	for i := range keys {
		if err := cells.Set(string(rune('a'+i)), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < keys; i += 2 {
		if err := cells.Delete(string(rune('a' + i))); err != nil {
			t.Fatal(err)
		}
	}

	// What a collection would move. Zero is "nothing has been given back".
	floor := func() uint64 {
		srv.mu.Lock()
		d := srv.docs["paper"]
		srv.mu.Unlock()
		if d == nil {
			return 0
		}
		d.mu.Lock()
		defer d.mu.Unlock()
		m, err := d.doc.Map("cells")
		if err != nil {
			return 0
		}
		return m.CollectedBelow()
	}

	sweep := func() {
		for range 20 {
			srv.mu.Lock()
			srv.lastCollect = time.Time{} // every pass is due
			srv.mu.Unlock()
			srv.collectStable()
			time.Sleep(time.Millisecond)
		}
	}

	sweep()
	if got := floor(); got != 0 {
		t.Fatalf("collection is off and the floor moved to %d: a server gave back "+
			"tombstones nobody asked it to give back", got)
	}

	srv.mu.Lock()
	srv.collectEvery = time.Nanosecond
	srv.mu.Unlock()
	sweep()
	if floor() == 0 {
		t.Fatal("the floor did not move with collection ON either, so the document " +
			"was never collectable and the assertion above was about nothing")
	}
}
