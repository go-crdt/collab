//go:build !js

package collab

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// bcast.go:375. A host's beacon beats at hostBeacon (40 ms), not at the 30 ms a
// hello repeats at. Over 800 ms that is about 21 frames against about 27, so the
// bound sits between them; the floor says the beacon beat at all.
func TestAHostBeaconBeatsAtItsOwnPace(t *testing.T) {
	hub := &memHub{}
	spy := hub.join()
	var frames atomic.Int64
	spy.onFrame(func([]byte) { frames.Add(1) })

	ctx, cancel := context.WithCancel(t.Context())
	stop := reannounce(ctx, attach(hub.join(), 1), ctlHost)
	time.Sleep(800 * time.Millisecond)
	stop()
	cancel()

	got := frames.Load()
	if got < 12 {
		t.Fatalf("the beacon posted %d frames in 800 ms, want about 21", got)
	}
	if got > 24 {
		t.Fatalf("the beacon posted %d frames in 800 ms: it beat at the hello pace, not its own (about 21)", got)
	}
}

// bcast.go:571 and 575. A bye ends the tab it names — takes it out of the table
// and closes its session — and one for a tab never seen changes nothing.
func TestAByeEndsOnlyTheTabItNames(t *testing.T) {
	hub := &memHub{}
	self := attach(hub.join(), 1)
	end := &bcastEnd{ctx: context.Background(), bc: self, clientID: 2, in: newInbox()}
	h := &bcastHost{ends: map[uint64]*bcastEnd{2: end}}

	// Control: a bye for a tab nobody knows leaves the one that is there.
	h.onBye(3)
	if len(h.ends) != 1 {
		t.Fatalf("a bye for an unknown tab left %d tabs, want 1", len(h.ends))
	}
	if err := end.Send(kindOperation, opsMsg{}); err != nil {
		t.Fatalf("the tab was closed by a bye that was not for it: %v", err)
	}

	h.onBye(2)
	if len(h.ends) != 0 {
		t.Fatalf("a bye left %d tabs, want 0", len(h.ends))
	}
	if err := end.Send(kindOperation, opsMsg{}); !errors.Is(err, ErrBroadcastClosed) {
		t.Fatalf("Send after a bye = %v, want ErrBroadcastClosed", err)
	}
}

// bcast.go:631. An election that ended because the caller gave up is reported as
// that, and nothing more is said on the bus: the tab is not handed a role, and
// does not go on to dial.
func TestAnElectionThatWasGivenUpOnDoesNotGoOnToDial(t *testing.T) {
	hub := &memHub{}
	spy := hub.join()
	var frames atomic.Int64
	spy.onFrame(func([]byte) { frames.Add(1) })

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, host, conn, err := hostOrJoinBus(ctx, attach(hub.join(), 1), bcastSettle)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("hostOrJoinBus on a cancelled context = %v, want context.Canceled", err)
	}
	if host != nil || conn != nil {
		t.Fatalf("hostOrJoinBus handed back a role (%v, %v) with its error", host, conn)
	}
	// One hello, from the election; a dial would say a second.
	if got := frames.Load(); got != 1 {
		t.Fatalf("the tab posted %d frames, want exactly the election's hello", got)
	}
}
