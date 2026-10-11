//go:build js && wasm

package collab

import (
	"context"
	"errors"
	"strings"
	"syscall/js"
	"testing"
	"time"
)

// A page with no BroadcastChannel is told so by both doors into it, rather than
// left to a type error.
//
// This package already had the equivalent test for RTCPeerConnection, and one
// was added for WebSocket in #239. BroadcastChannel had none, and the first
// mutation sweep of the js-only files found the checks after newBCBus surviving
// in both of its callers: bcast_js.go:132, in the transport a joining tab uses,
// and bcast_js.go:207, in the session an electing tab opens.
//
// Deleted, each carries a nil *bcBus into the line below it -- attach(b, ...)
// and then a method call on it. That is a panic inside a browser tab, where the
// stack ends in a console nobody is reading and the tab simply stops syncing.
// An old browser, or a page served from a context where BroadcastChannel is not
// exposed, is owed a sentence it can show instead.
func TestBothDoorsRefuseAPageWithNoBroadcastChannel(t *testing.T) {
	// The control first, and it calls the function under test rather than a door
	// into it: with the fake installed, newBCBus succeeds, so what follows is
	// about the missing global and not about the fixture.
	//
	// newBCBus and not open(), for a reason worth leaving here. The first
	// version of this control called open(), which dials and waits for a host
	// that does not exist in a room nobody is serving: it blocked until the
	// test's own context ended and `go test` killed the package after ten
	// minutes. A control must not be able to wait.
	js.Global().Call("eval", fakeBroadcastChannel)
	bus, err := newBCBus("room:control")
	if err != nil {
		t.Fatalf("the control failed with the fake installed: %v", err)
	}
	bus.close()

	saved := js.Global().Get("BroadcastChannel")
	js.Global().Delete("BroadcastChannel")
	defer js.Global().Set("BroadcastChannel", saved)

	t.Run("the transport a joining tab uses", func(t *testing.T) {
		conn, err := JoinBroadcastChannel("room:none").open(t.Context())
		if err == nil {
			t.Fatal("open succeeded with no BroadcastChannel in the page")
		}
		if conn != nil {
			t.Error("open returned a connection alongside its error")
		}
		assertNamesTheMissingChannel(t, err)
	})

	t.Run("the session an electing tab opens", func(t *testing.T) {
		bs, err := OpenBroadcastSession(t.Context(), "room:none", 50*time.Millisecond)
		if err == nil {
			t.Fatal("OpenBroadcastSession succeeded with no BroadcastChannel in the page")
		}
		if bs != nil {
			t.Error("OpenBroadcastSession returned a session alongside its error")
		}
		assertNamesTheMissingChannel(t, err)
	})
}

func assertNamesTheMissingChannel(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, ErrTransport) {
		t.Errorf("got %v, want an ErrTransport", err)
	}
	if !strings.Contains(err.Error(), "no BroadcastChannel") {
		t.Errorf("got %q, which does not name what the page is missing", err)
	}
}

// A tab that cannot reach a host in time is told so, and handed nothing.
//
// The transport dials and waits for a host to answer. Given a room nobody
// serves and a deadline that has already passed, it has to come back with an
// error and no connection -- and the check that makes sure of it,
// bcast_js.go:136 after dialBus, survived the first sweep of these files.
// Deleted, open returns (nil, nil): a caller told nothing went wrong, holding
// nothing, with the bus left open behind it, since that branch is also what
// closes it.
//
// The deadline is short on purpose. The same combination without a deadline is
// how the first version of the control above went wrong -- it called open()
// with the test's own context and waited the ten minutes `go test` allows -- so
// here the waiting is the subject rather than an accident, and bounded.
//
// Its sibling, bcast_js.go:220 after openWith, is NOT held and could not be by
// this means: measured, OpenBroadcastSession succeeds on a context that is
// already over, because a tab alone in a room becomes its host immediately and
// the election never has to wait for anything. Making openWith fail needs a
// fixture that can break the election itself, which the BroadcastChannel fake
// has no knob for.
func TestATabThatCannotReachAHostInTimeIsToldSo(t *testing.T) {
	js.Global().Call("eval", fakeBroadcastChannel)

	ctx, cancel := context.WithTimeout(t.Context(), time.Millisecond)
	defer cancel()
	conn, err := JoinBroadcastChannel("room:nobody-serves-this").open(ctx)
	if err == nil {
		t.Fatal("open succeeded in a room nobody serves")
	}
	if conn != nil {
		t.Error("open returned a connection alongside its error")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("got %v, want the deadline it was given", err)
	}
}
