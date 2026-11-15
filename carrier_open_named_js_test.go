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
const neverOpeningChannel = `(() => {
  const listeners = {};
  return {
    readyState: "connecting",
    binaryType: "blob",
    bufferedAmount: 0,
    addEventListener(event, fn) { (listeners[event] = listeners[event] || []).push(fn); },
    removeEventListener(event, fn) {
      const l = listeners[event] || [];
      const i = l.indexOf(fn);
      if (i >= 0) l.splice(i, 1);
    },
    send() {},
    close() { this.readyState = "closed"; },
    _events() { return listeners; },
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
	// ⛔ And the listeners are off again: a refused open must not leave four
	// callbacks wired to a channel nobody holds. The control is that they were
	// ON, or a zero here would mean the walk found nothing to look at.
	evs := ch.Call("_events")
	n := 0
	for _, k := range []string{"open", "message", "error", "close"} {
		if v := evs.Get(k); v.Truthy() {
			n += v.Length()
		}
	}
	if n != 0 {
		t.Errorf("%d listener(s) are still wired after a refused open", n)
	}
}
