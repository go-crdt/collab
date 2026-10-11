//go:build js && wasm

package collab

import (
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
