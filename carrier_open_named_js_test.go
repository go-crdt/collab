//go:build js && wasm

package collab

import (
	"context"
	"errors"
	"strings"
	"syscall/js"
	"testing"
)

// ⛔⛔ THE TENTH AND ELEVENTH WAITS ON THE BROWSER, and they are here because a
// review COUNTED the package's selects against a sentence I had written in the
// README: "every wait on the browser says which call stalled".
//
// That claim was too wide three times in one morning. The first round found the
// sixth wait of six in peer_js.go. The second found the seventh and eighth in
// the same file — selects, not calls, so the static check could not see them.
// This round found the ninth and tenth in OTHER FILES of the same package, and
// those two said nothing at all: not the operation, not even the three
// connection-state words the peer's waits carry, just "context deadline
// exceeded".
//
// ⭐ Each time, what refuted the claim was somebody counting a number I had
// written down. A code reading follows the paths the reader thinks of; a
// sentence saying "every" is falsifiable by anyone who can count. That is the
// whole argument for writing the claim out, and it cost three rounds to learn
// in one file.
//
// The waits are DETERMINISTIC here without a stall fixture: js/wasm runs no
// browser task while Go runs, so between addEventListener and the select no
// event can have fired. An already-cancelled context therefore has exactly one
// ready case.

func TestTheWebSocketOpenWaitNamesItself(t *testing.T) {
	// A port nothing listens on, so nothing can resolve this before the
	// select; the cancelled context is what ends it.
	tr := &wsTransport{url: "ws://127.0.0.1:1/never"}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := tr.open(ctx)
	if err == nil {
		t.Fatal("a cancelled context must refuse")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("the context's error must survive the wrapping, got %v", err)
	}
	if !strings.Contains(err.Error(), "websocket open") {
		t.Errorf("the error must name which wait stalled; it says %q", err)
	}
}

// A data channel the page never opens. The fixture is deliberately minimal:
// readyState "connecting" and listeners that are recorded and never fired, so
// the only ready case at the select is the cancelled context.
// ⛔⛔ IT COUNTS WHAT WAS ADDED AND WHAT WAS REMOVED, not what is left. The
// first version of this fixture recorded only the current listeners and the
// test asserted the count afterwards, with a comment claiming "the control is
// that they were ON". There was no control: a review renamed the event keys in
// webrtc_js.go so that NOTHING was wired, and the test stayed green —
// `mutate … THE SUITE SURVIVED IT`. Zero-left and never-added are the same
// number, which is the whole failure mode this file is about, written into the
// test demonstrating it.
const neverOpeningChannel = `(() => {
  const listeners = {};
  let added = 0, removed = 0;
  return {
    readyState: "connecting",
    binaryType: "blob",
    bufferedAmount: 0,
    addEventListener(event, fn) { added++; (listeners[event] = listeners[event] || []).push(fn); },
    removeEventListener(event, fn) {
      const l = listeners[event] || [];
      const i = l.indexOf(fn);
      if (i >= 0) { l.splice(i, 1); removed++; }
    },
    send() {},
    close() { this.readyState = "closed"; },
    _added() { return added; },
    _removed() { return removed; },
  };
})()`

func TestTheDataChannelOpenWaitNamesItself(t *testing.T) {
	ch := js.Global().Call("eval", neverOpeningChannel)
	if got := ch.Get("readyState").String(); got != "connecting" {
		t.Fatalf("the fixture must not be open already, it is %q: this test would pass blind", got)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := DataChannel(ch).open(ctx)
	if err == nil {
		t.Fatal("a cancelled context must refuse")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("the context's error must survive the wrapping, got %v", err)
	}
	if !strings.Contains(err.Error(), "data channel open") {
		t.Errorf("the error must name which wait stalled; it says %q", err)
	}
	// ⛔ A refused open must not leave callbacks wired to a channel nobody
	// holds — and the ADDED count is the control, asserted first. Without it,
	// "nothing is wired now" is satisfied just as well by "nothing was ever
	// wired", which is exactly what a mutation of the event names produced
	// while this test stayed green.
	if added := ch.Call("_added").Int(); added != 4 {
		t.Fatalf("%d listener(s) were wired, want 4: this test would otherwise pass blind", added)
	}
	if removed := ch.Call("_removed").Int(); removed != 4 {
		t.Errorf("%d listener(s) were removed after a refused open, want 4", removed)
	}
}
