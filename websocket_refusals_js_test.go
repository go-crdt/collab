//go:build js && wasm

package collab

import (
	"errors"
	"strings"
	"syscall/js"
	"testing"
)

// A page with no WebSocket is told so, rather than left to a type error.
//
// The twin of TestNewPeerWithoutWebRTC, which this package already had for
// RTCPeerConnection. This one did not exist, and the first mutation sweep of the
// js-only files found its guard surviving on 2026-10-10: deleted,
// `ctor.New(t.url)` runs against an undefined global, which is a JavaScript
// TypeError surfacing as a Go panic in whatever goroutine the dial was on.
//
// An old browser, or a page served where WebSocket has been removed, is the
// case: the answer a transport owes there is a named refusal, which a page can
// show, and not a stack.
func TestOpeningAWebSocketWhereThereIsNoneIsRefused(t *testing.T) {
	// The control first: with WebSocket present, open gets past the guard. It
	// will fail later for having nowhere to connect, and that is the point --
	// the error must not be this one.
	tr := &wsTransport{url: "ws://127.0.0.1:1/never"}
	if _, err := tr.open(t.Context()); err != nil && strings.Contains(err.Error(), "has no WebSocket") {
		t.Fatalf("the control was refused for the reason under test: %v", err)
	}

	saved := js.Global().Get("WebSocket")
	js.Global().Delete("WebSocket")
	defer js.Global().Set("WebSocket", saved)

	_, err := tr.open(t.Context())
	if err == nil {
		t.Fatal("open succeeded with no WebSocket in the page")
	}
	if !errors.Is(err, ErrTransport) {
		t.Errorf("open gave %v, want an ErrTransport", err)
	}
	if !strings.Contains(err.Error(), "has no WebSocket") {
		t.Errorf("open gave %q, which does not name what the page is missing", err)
	}
}

// Releasing a connection twice is a no-op.
//
// This pins the contract and NOT the guard, and the difference was measured
// rather than assumed. I wrote it expecting the guard to be load-bearing --
// release drops four js.Func callbacks, and a second Release of the same Func
// looked like something syscall/js would refuse. It does not: with the guard
// deleted this test still passes, so under Go 1.27.2 both finish() (a select on
// an already-closed channel) and js.Func.Release() are idempotent on their own.
// The guard is cost.
//
// The test stays because the contract is worth holding on its own. A session is
// closed by whoever owns it and again by the defer that was guarding it, which
// is the ordinary shape of Go cleanup rather than a mistake, so a second call
// has to be absorbed -- and it is absorbed today by three separate things, two
// of which are the standard library's and could change under us.
func TestReleasingAConnectionTwiceIsANoOp(t *testing.T) {
	c := &jsConn{
		closed:    make(chan struct{}),
		onOpen:    js.FuncOf(func(js.Value, []js.Value) any { return nil }),
		onMessage: js.FuncOf(func(js.Value, []js.Value) any { return nil }),
		onError:   js.FuncOf(func(js.Value, []js.Value) any { return nil }),
		onClose:   js.FuncOf(func(js.Value, []js.Value) any { return nil }),
	}

	c.release()
	if !c.released {
		t.Fatal("the first release did not take, so the second one is not what is being measured")
	}
	c.release() // must not panic, and must not release a Func twice
}
