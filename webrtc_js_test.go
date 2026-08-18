//go:build js && wasm

package collab

import (
	"context"
	"syscall/js"

	"github.com/go-crdt/crdt"
	"testing"
	"time"
)

// A pair of channels wired to each other, standing in for what WebRTC hands a
// page once two browsers have swapped their connection descriptions.
//
// What is stubbed is the network, and only the network. Everything above it is
// the real thing: the real framing, the real session logic, two real replicas.
// WebRTC's own part — carrying bytes between two machines that cannot listen —
// is the browser's to do and not this package's to test.
const linkedChannels = `
(function () {
  function chan(name) {
    const listeners = {};
    return {
      name,
      readyState: "open",
      binaryType: "blob",
      peer: null,
      addEventListener(event, fn) { (listeners[event] ||= []).push(fn); },
      fire(event, arg) { for (const fn of listeners[event] || []) fn(arg); },
      send(data) {
        const copy = data.slice();
        // Asynchronously, as a real channel delivers: a page that took a
        // message inside send would be a page whose stack never unwound.
        setTimeout(() => this.peer.fire("message", { data: copy.buffer }), 0);
      },
      close() { this.fire("close", {}); this.peer.fire("close", {}); },
    };
  }
  const a = chan("a"), b = chan("b");
  a.peer = b; b.peer = a;
  return { a, b };
})()
`

// Two browsers editing one document, with nothing in between.
//
// One of them holds it — that is what a Server does, and it is a role rather
// than a machine, since neither is listening for anything. The other joins. The
// only thing they share is a data channel their pages opened.
func TestTwoBrowsersShareADocumentOverADataChannel(t *testing.T) {
	pair := js.Global().Call("eval", linkedChannels)
	host, guest := pair.Get("a"), pair.Get("b")

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	srv := NewServer(Config{Store: NewMemoryStore()})
	defer func() { _ = srv.Close(context.Background()) }()

	served := make(chan error, 1)
	go func() { served <- srv.ServeDataChannel(ctx, host) }()

	client, err := Join(ctx, DataChannel(guest), ClientConfig{Document: "these.tex", Site: 2})
	if err != nil {
		t.Fatalf("joining the browser holding the document: %v", err)
	}
	defer func() { _ = client.Close() }()

	body, err := client.Text("file:main.tex")
	if err != nil {
		t.Fatal(err)
	}
	if err := body.Insert(0, `\section{Résultats}`); err != nil {
		t.Fatal(err)
	}

	// What the holder now has is what the guest typed, which is the whole
	// claim: the document crossed a channel with no server on it.
	deadline := time.After(15 * time.Second)
	for {
		doc, err := srv.open(ctx, "these.tex")
		if err != nil {
			t.Fatal(err)
		}
		doc.mu.Lock()
		text, terr := doc.doc.Text("file:main.tex")
		got := ""
		if terr == nil {
			got = text.String()
		}
		doc.mu.Unlock()
		if got == `\section{Résultats}` {
			break
		}
		select {
		case err := <-served:
			t.Fatalf("the session ended before the text arrived: %v", err)
		case <-deadline:
			t.Fatalf("the holder has %q", got)
		case <-time.After(10 * time.Millisecond):
		}
	}

	// And back the other way, which is what makes it co-editing rather than
	// publishing: the holder's own edit reaches the guest.
	doc, err := srv.open(ctx, "these.tex")
	if err != nil {
		t.Fatal(err)
	}
	doc.mu.Lock()
	text, _ := doc.doc.Text("file:main.tex")
	ops, err := text.Insert(text.Len(), " sont clairs.")
	doc.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := crdt.AppendPartOps(nil, []crdt.PartOps{{
		Part: crdt.Part{Kind: crdt.PartText, Name: "file:main.tex"}, Text: ops,
	}})
	if err != nil {
		t.Fatal(err)
	}
	doc.mu.Lock()
	doc.dirty = true
	doc.broadcast(nil, operationsMessage(raw))
	doc.mu.Unlock()

	want := `\section{Résultats} sont clairs.`
	deadline = time.After(15 * time.Second)
	for body.String() != want {
		select {
		case <-client.Changes():
		case <-deadline:
			t.Fatalf("the guest has %q, want %q", body.String(), want)
		}
	}
	t.Logf("two browsers, no server, converged on %q", body.String())
}
