//go:build js && wasm

package collab

import (
	"errors"
	"strings"
	"syscall/js"
	"testing"
)

// A browser without the Web Locks API falls back to the timing window instead
// of calling a method on nothing.
//
// electByLock is the RULE for deciding which tab hosts, and electRole's window
// is the FALLBACK -- that is the shape bcast_lock_js.go documents, and it is the
// right way round, because a lock answers immediately and exclusively while a
// window can leave two tabs hosting on a loaded machine. The feature detection
// is what chooses between them, and with it deleted `locks.Call("request", ...)`
// runs against an undefined `navigator.locks`: a panic in the tab, instead of
// the fallback that exists for exactly this browser.
//
// The first mutation sweep of this package's js-only files found it surviving.
//
// Hiding the property took measuring. `navigator.locks` is NOT an own property
// -- it lives on the prototype -- so `delete navigator.locks` does nothing and
// the first version of this test read it as still present and failed. An own
// property defined over it does shadow it, and deleting THAT own property
// afterwards reveals the original, which is why the restore is a delete.
func TestWithoutWebLocksTheElectionFallsBackInsteadOfPanicking(t *testing.T) {
	g := js.Global()
	nav := g.Get("navigator")
	had := nav.Get("locks").Truthy() && nav.Get("locks").Get("request").Truthy()

	// The control, where the runner has the API at all: the detection does not
	// refuse when it is there. Said out loud rather than skipped silently when
	// it is not, because a control that did not run is not a control.
	if had {
		if _, ok := electByLock(t.Context(), "room:control"); !ok {
			t.Error("the control: with navigator.locks present the detection still refused")
		}
	} else {
		t.Log("this runner has no navigator.locks, so the positive side is not exercised here")
	}

	g.Call("eval", `Object.defineProperty(navigator, "locks", {value: undefined, configurable: true})`)
	t.Cleanup(func() { g.Call("eval", `delete navigator.locks`) })
	if nav.Get("locks").Truthy() {
		t.Fatal("the fixture did not hide navigator.locks, so nothing below is about its absence")
	}

	if role, ok := electByLock(t.Context(), "room:none"); ok {
		t.Fatalf("the election claimed to have decided (%v) with no Web Locks API", role)
	}
}

// A data channel that is not there is refused by name, not wired up.
//
// openDataChannel is what a Peer's channel and a WebSocket both become, and it
// is reached with whatever the browser handed over. An undefined value there is
// a handshake that finished without a channel -- one of the two ways a WebRTC
// handshake stops halfway, which the fake in peer_js_test.go can produce on
// purpose. With the check deleted that value is stored and its first method
// call panics, somewhere later than the mistake.
//
// The positive side is not re-created here: openDataChannel WAITS for a channel
// to open, so handing it a stub object blocks rather than returning -- measured,
// a ninety-second test timeout. TestTwoBrowsersConnectAndConverge drives this
// function with a channel that really opens, and that is the control.
func TestADataChannelThatIsNotThereIsRefused(t *testing.T) {
	for _, c := range []struct {
		name string
		ch   js.Value
	}{
		{"undefined", js.Undefined()},
		{"null", js.Null()},
	} {
		t.Run(c.name, func(t *testing.T) {
			conn, err := openDataChannel(t.Context(), c.ch, decodeServer, nil)
			if err == nil {
				t.Fatal("openDataChannel accepted it")
			}
			if conn != nil {
				t.Error("openDataChannel returned a connection alongside its error")
			}
			if !errors.Is(err, ErrTransport) {
				t.Errorf("got %v, want an ErrTransport", err)
			}
			if !strings.Contains(err.Error(), "no data channel") {
				t.Errorf("got %q, which does not say what was missing", err)
			}
		})
	}
}

// stalled decorates an error and leaves a non-error alone.
//
// It exists to turn "the handshake timed out" into a sentence naming the
// connection's three states, and both of its callers hand it ctx.Err() from
// inside a `case <-ctx.Done()`, so neither can pass nil today. The guard that
// refuses nil is therefore unreachable from here -- and worth one line anyway,
// because of what the measurement says happens without it:
//
//	stalled(nil) = %!w(<nil>) (connection new, ice new, gathering new)
//
// A non-nil error built out of a nil one, with a formatting artefact for a
// message. Any future caller that decorates unconditionally would turn every
// success into a failure, and the failure would say nothing about why.
func TestStalledLeavesANilErrorAlone(t *testing.T) {
	installFake(t)
	p, err := NewPeer(PeerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })

	if got := p.stalled(nil); got != nil {
		t.Errorf("stalled(nil) = %v, want nil", got)
	}

	// The control: a real error is decorated rather than swallowed, so this test
	// cannot pass by making stalled return nil for everything.
	decorated := p.stalled(ErrTransport)
	if decorated == nil {
		t.Fatal("stalled swallowed a real error")
	}
	if !errors.Is(decorated, ErrTransport) {
		t.Errorf("stalled(ErrTransport) = %v, which no longer wraps it", decorated)
	}
	if !strings.Contains(decorated.Error(), "connection ") {
		t.Errorf("stalled(ErrTransport) = %q, which does not name the connection's state", decorated)
	}
}

// The rest of the twenty survivors from that sweep, each with the reason it is
// not pinned and the measurement behind it. Written here so the next sweep's
// reader does not re-derive them.
//
//	peer_js.go:255 / :275, the data channel's readyState, and
//	peer_js.go:321 / :337, ICE gathering's state. Two pairs of the same shape:
//	the state is read once before the listener is wired and once after, so
//	deleting the first only wires listeners that are thrown away and deleting
//	the second leaves the first answering. The one that carries the behaviour is
//	the SECOND of each pair, whose window -- the state changing between the
//	first read and the wiring -- cannot be opened on purpose without controlling
//	the browser's own timing. The fake does not offer that.
//
//	peer_js.go:303, "the connection is gone". Reachable only from a Peer whose
//	pc was never set: Close does not zero it, and NewPeer always sets it, so no
//	path here produces one. Measured: a zero Peer's pc is indeed untruthy, which
//	is what the guard is for and what nothing constructs.
//
//	peer_js.go:308, "that state is not a string". Cosmetic. syscall/js does not
//	panic on String() of a non-string: measured, an object renders as "<object>"
//	and undefined as "<undefined>". With the guard gone a missing state reads as
//	"<undefined>" in the message instead of "?".
//
//	peer_js.go:368, bcast_js.go:98 and websocket_js.go:212, the three "already
//	closed" guards. Cost, and measured as such: with each deleted the whole
//	js/wasm suite still passes, because js.Func.Release and a select on an
//	already-closed channel are both idempotent under Go 1.27.2. The CONTRACT is
//	held by TestReleasingAConnectionTwiceIsANoOp, which is the part that could
//	stop being true under us.
//
//	bcast_js.go:136 (after dialBus), bcast_js.go:220 (after openWith) and
//	peer_js.go:209 (after createAnswer). Real, and not pinned because the fake
//	cannot make any of those three fail: it has knobs for a stalled ICE and for
//	an answer with no channel, and nothing for a rejected promise. Holding these
//	means extending the fake, which is a change to the fixture rather than a
//	test, and it is the obvious next step here.
