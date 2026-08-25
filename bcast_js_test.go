//go:build js && wasm

package collab

import (
	"context"
	"syscall/js"
	"testing"
	"time"
)

// A BroadcastChannel standing in for the browser's, so the js adapter is run
// rather than only compiled. What is stubbed is the browser object and nothing
// above it: the real framing, the real routing, the real session logic and two
// real replicas all run. Every tab constructed with the same name is on one bus,
// a post reaches every other tab on it and never the sender, and delivery is
// asynchronous the way the real one is — a tab that acted on a message inside
// postMessage would be a tab whose stack never unwound.
const fakeBroadcastChannel = `
(function () {
  const rooms = {};
  globalThis.BroadcastChannel = function (name) {
    const self = this;
    self.name = name;
    self._listeners = [];
    (rooms[name] ||= []).push(self);
    self.addEventListener = function (event, fn) {
      if (event === "message") self._listeners.push(fn);
    };
    self.postMessage = function (data) {
      const copy = data.slice();
      // Membership and closed-ness are read at delivery, not at post: a channel
      // closed between the two must not be delivered to, the way a real one that
      // has stopped listening is not.
      setTimeout(() => {
        for (const peer of rooms[name] || []) {
          if (peer === self || peer._closed) continue;
          for (const fn of peer._listeners) fn({ data: copy });
        }
      }, 0);
    };
    self.close = function () {
      self._closed = true;
      const a = rooms[name];
      const i = a.indexOf(self);
      if (i >= 0) a.splice(i, 1);
    };
    return self;
  };
})()
`

// Two tabs of one browser editing a document, over a BroadcastChannel and
// nothing else: no WebRTC, no server, and nothing for a person to carry between
// the windows. One tab holds the document — a role, not a machine — and the
// other joins it, both through the whole js adapter this package ships.
func TestTwoTabsShareADocumentOverABroadcastChannel(t *testing.T) {
	js.Global().Call("eval", fakeBroadcastChannel)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	srv := NewServer(Config{Store: NewMemoryStore()})
	defer func() { _ = srv.Close(context.Background()) }()

	// The holder serves the room; the other tab joins it.
	served := make(chan error, 1)
	go func() { served <- srv.ServeBroadcastChannel(ctx, "these.tex") }()

	client, err := Join(ctx, JoinBroadcastChannel("these.tex"),
		ClientConfig{Document: "these.tex", Site: 2})
	if err != nil {
		t.Fatalf("joining the tab holding the document: %v", err)
	}
	defer func() { _ = client.Close() }()

	body, err := client.Text("file:main.tex")
	if err != nil {
		t.Fatal(err)
	}
	if err := body.Insert(0, `\section{Résultats}`); err != nil {
		t.Fatal(err)
	}

	// What the holder now has is what the other tab typed: the document crossed
	// the bus with no server on it.
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
	t.Logf("two tabs, one browser, no server, converged on %q", body.String())
}

// HostOrJoin makes the room zero-config: a lone tab is told to host, and a
// second tab, finding the first already answering, is told to join — the whole
// point of the election, run through the js entrypoint the page will call.
func TestHostOrJoinDecidesTheRole(t *testing.T) {
	js.Global().Call("eval", fakeBroadcastChannel)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	role, err := HostOrJoin(ctx, "room:election", DefaultElectionWindow)
	if err != nil {
		t.Fatalf("HostOrJoin for the first tab: %v", err)
	}
	if role != RoleHost {
		t.Fatalf("the first tab was told to be %v, want RoleHost", role)
	}

	// With a tab now serving the room, a later tab finds a host answering and is
	// told to join. The server is proven up first by a real join, so the later
	// tab's decision does not race the server attaching to the bus.
	srv := NewServer(Config{Store: NewMemoryStore()})
	defer func() { _ = srv.Close(context.Background()) }()
	go func() { _ = srv.ServeBroadcastChannel(ctx, "room:election") }()

	proof, err := Join(ctx, JoinBroadcastChannel("room:election"),
		ClientConfig{Document: "room:election", Site: 7})
	if err != nil {
		t.Fatalf("proving the host is up: %v", err)
	}
	defer func() { _ = proof.Close() }()

	role, err = HostOrJoin(ctx, "room:election", 5*time.Second)
	if err != nil {
		t.Fatalf("HostOrJoin for the later tab: %v", err)
	}
	if role != RoleClient {
		t.Fatalf("the later tab was told to be %v, want RoleClient", role)
	}
}
