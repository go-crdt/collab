//go:build js && wasm

package collab

import (
	"context"
	"errors"
	"strings"
	"syscall/js"
	"testing"
	"time"

	"github.com/go-crdt/crdt"
)

// A fake RTCPeerConnection, so that everything a [Peer] does around the browser's
// own connection can be run without one. What is stubbed is WebRTC and only
// WebRTC: the offer and the answer are carried between two of these in the same
// page, ICE gathering completes on a timer, and the data channel it hands back
// is a pair wired to each other — exactly the shape the browser gives once two
// real connections have found each other. Everything above it, the whole of a
// [Peer] and the whole of a session, is the real code.
//
// globalThis.__collabFake tunes it: stallICE leaves gathering forever
// incomplete, and noChannel answers an offer without ever delivering a channel —
// the two ways a handshake stops halfway, which a Peer has to wait on and a
// context has to be able to cut short.
const fakeWebRTC = `
(function () {
  globalThis.__collabFake = globalThis.__collabFake || {};
  const registry = {};
  let nextId = 0;

  function channel(label) {
    const listeners = {};
    return {
      label, readyState: "connecting", binaryType: "blob", peer: null,
      addEventListener(event, fn) { (listeners[event] ||= []).push(fn); },
      fire(event, arg) { for (const fn of listeners[event] || []) fn(arg); },
      open() { this.readyState = "open"; this.fire("open", {}); },
      send(data) {
        const copy = data.slice();
        setTimeout(() => this.peer.fire("message", { data: copy.buffer }), 0);
      },
      close() {
        this.readyState = "closed"; this.fire("close", {});
        if (this.peer) this.peer.fire("close", {});
      },
    };
  }

  const ctor = function (config) {
    const id = nextId++;
    const evs = {};
    const pc = {
      config, iceGatheringState: "new", localDescription: null, remoteDescription: null,
      _channel: null,
      addEventListener(event, fn) { (evs[event] ||= []).push(fn); },
      _emit(event, arg) { for (const fn of evs[event] || []) fn(arg); },
      createDataChannel(label) { this._channel = channel(label); return this._channel; },
      createOffer() {
        if (globalThis.__collabFake.rejectOffer) return Promise.reject(new Error("createOffer refused"));
        return Promise.resolve({ type: "offer", sdp: "v=0\r\n" });
      },
      createAnswer() { return Promise.resolve({ type: "answer", sdp: "v=0\r\n" }); },
      setLocalDescription(desc) {
        // Stamp the registry id into the sdp so an answerer can find the offerer.
        this.localDescription = { type: desc.type, sdp: desc.sdp + "a=pcid:" + id + "\r\n" };
        if (!globalThis.__collabFake.stallICE) {
          setTimeout(() => { this.iceGatheringState = "complete"; this._emit("icegatheringstatechange", {}); }, 0);
        }
        return Promise.resolve();
      },
      setRemoteDescription(desc) {
        this.remoteDescription = desc;
        const m = /a=pcid:(\d+)/.exec(desc.sdp || "");
        if (desc.type === "offer" && m && !globalThis.__collabFake.noChannel) {
          const offerer = registry[m[1]];
          const ours = channel(offerer._channel.label);
          ours.peer = offerer._channel; offerer._channel.peer = ours;
          setTimeout(() => { this._emit("datachannel", { channel: ours }); ours.open(); offerer._channel.open(); }, 0);
        }
        return Promise.resolve();
      },
      close() { this.iceGatheringState = "closed"; if (this._channel) this._channel.readyState = "closed"; },
    };
    registry[id] = pc;
    return pc;
  };
  ctor.__fake = true;
  globalThis.RTCPeerConnection = ctor;
})()
`

func installFake(t *testing.T) {
	t.Helper()
	js.Global().Call("eval", fakeWebRTC)
	t.Cleanup(func() { js.Global().Set("__collabFake", map[string]any{}) })
}

func setFake(field string, v bool) { js.Global().Get("__collabFake").Set(field, v) }

// Two browsers reach each other with nothing between them, and then edit one
// document over what they reached each other with. This is the whole claim of
// the Peer: an offer and an answer swapped, a channel opened, a session run.
func TestTwoBrowsersConnectAndConverge(t *testing.T) {
	installFake(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	host, err := NewPeer(PeerConfig{})
	if err != nil {
		t.Fatalf("host: %v", err)
	}
	defer func() { _ = host.Close() }()
	// A STUN server is named on one side to prove the configuration is accepted;
	// the fake needs none, as two browsers on one network need none.
	guest, err := NewPeer(PeerConfig{ICEServers: []string{"stun:stun.l.google.com:19302"}})
	if err != nil {
		t.Fatalf("guest: %v", err)
	}
	defer func() { _ = guest.Close() }()

	offer, err := host.Offer(ctx, "collab")
	if err != nil {
		t.Fatalf("offer: %v", err)
	}
	answer, err := guest.Answer(ctx, offer)
	if err != nil {
		t.Fatalf("answer: %v", err)
	}
	if err := host.AcceptAnswer(answer); err != nil {
		t.Fatalf("accept answer: %v", err)
	}

	hostCh, err := host.DataChannel(ctx)
	if err != nil {
		t.Fatalf("host channel: %v", err)
	}
	guestCh, err := guest.DataChannel(ctx)
	if err != nil {
		t.Fatalf("guest channel: %v", err)
	}
	t.Log("both data channels open")

	srv := NewServer(Config{Store: NewMemoryStore()})
	defer func() { _ = srv.Close(context.Background()) }()

	served := make(chan error, 1)
	go func() { served <- srv.ServeDataChannel(ctx, hostCh) }()

	client, err := Join(ctx, DataChannel(guestCh), ClientConfig{Document: "these.tex", Site: 2})
	if err != nil {
		t.Fatalf("join: %v", err)
	}
	defer func() { _ = client.Close() }()

	body, err := client.Text("file:main.tex")
	if err != nil {
		t.Fatal(err)
	}
	if err := body.Insert(0, `\section{Résultats}`); err != nil {
		t.Fatal(err)
	}

	// The guest's edit reaches the holder: the document crossed a channel that a
	// pair of Peers opened, with no server on the connection.
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
	t.Log("edit propagated guest to host")

	// And back: the holder's own edit reaches the guest, which is what makes it
	// co-editing over the connection rather than one-way publishing.
	doc, err := srv.open(ctx, "these.tex")
	if err != nil {
		t.Fatal(err)
	}
	doc.mu.Lock()
	serverText, _ := doc.doc.Text("file:main.tex")
	ops, oerr := serverText.Insert(serverText.Len(), " sont clairs.")
	doc.mu.Unlock()
	if oerr != nil {
		t.Fatal(oerr)
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
	t.Logf("two Peers, no server on the wire, converged on %q", body.String())
}

// The blob a person pastes is a description in and a description out, unchanged.
func TestSignalRoundTrips(t *testing.T) {
	in := signal{Type: "offer", SDP: "v=0\r\no=- 1 2 IN IP4 127.0.0.1\r\n"}
	blob := encodeSignal(in)
	if strings.ContainsAny(blob, "\r\n") {
		t.Fatalf("a pasteable blob has no newline in it: %q", blob)
	}
	out, err := decodeSignal(blob)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out != in {
		t.Fatalf("round trip changed the description: %+v became %+v", in, out)
	}
	// Surrounding whitespace, which a copy-paste picks up, is tolerated.
	if _, err := decodeSignal("  " + blob + "\n"); err != nil {
		t.Fatalf("a blob with whitespace around it should still decode: %v", err)
	}
}

func TestDecodeSignalRejectsRubbish(t *testing.T) {
	cases := map[string]string{
		"not base64":           "this is not base64!!!",
		"not json":             encodeToBase64("not json at all"),
		"missing sdp":          encodeToBase64(`{"type":"offer"}`),
		"missing type":         encodeToBase64(`{"sdp":"v=0"}`),
		"empty object":         encodeToBase64(`{}`),
		"a number, not an obj": encodeToBase64(`42`),
	}
	for name, blob := range cases {
		if _, err := decodeSignal(blob); err == nil {
			t.Errorf("%s: decoded when it should have failed", name)
		} else if !errors.Is(err, ErrTransport) {
			t.Errorf("%s: error is not an ErrTransport: %v", name, err)
		}
	}
}

// encodeToBase64 is the test's own way to make a bad blob, kept apart from the
// package's encodeSignal so a bug in one cannot hide a bug in the other.
func encodeToBase64(s string) string {
	return js.Global().Get("btoa").Invoke(s).String()
}

// The ICE server list becomes the RTCPeerConnection configuration a browser
// takes: one entry per URL, each an object whose urls is that URL.
func TestICEConfiguration(t *testing.T) {
	empty := iceConfiguration(PeerConfig{})
	if servers := empty.Get("iceServers"); !servers.IsUndefined() {
		t.Fatalf("an empty config should not set iceServers, got %v", servers)
	}

	config := iceConfiguration(PeerConfig{ICEServers: []string{
		"stun:stun.l.google.com:19302", "turn:turn.example.org",
	}})
	servers := config.Get("iceServers")
	if servers.Length() != 2 {
		t.Fatalf("two URLs should make two servers, got %d", servers.Length())
	}
	if got := servers.Index(0).Get("urls").String(); got != "stun:stun.l.google.com:19302" {
		t.Fatalf("first server is %q", got)
	}
	if got := servers.Index(1).Get("urls").String(); got != "turn:turn.example.org" {
		t.Fatalf("second server is %q", got)
	}
}

// A credentialed TURN relay is given through ICEServersAuth, appended after the
// plain URL list, and carries username/credential — which a bare URL cannot. An
// entry with no credentials sends neither field rather than empty strings.
func TestICEConfigurationWithCredentials(t *testing.T) {
	config := iceConfiguration(PeerConfig{
		ICEServers: []string{"stun:stun.l.google.com:19302"},
		ICEServersAuth: []ICEServerAuth{
			{URLs: []string{"turn:turn.eu.example:3478", "turns:turn.eu.example:5349"}, Username: "u", Credential: "secret"},
			{URLs: []string{"stun:stun.eu.example:3478"}}, // no credentials
		},
	})
	servers := config.Get("iceServers")
	if servers.Length() != 3 {
		t.Fatalf("one URL + two auth servers should make three, got %d", servers.Length())
	}
	// The plain STUN URL comes first, as a single string.
	if got := servers.Index(0).Get("urls").String(); got != "stun:stun.l.google.com:19302" {
		t.Fatalf("first server urls = %q", got)
	}
	// The credentialed TURN relay: an array of URLs plus username + credential.
	turn := servers.Index(1)
	if n := turn.Get("urls").Length(); n != 2 {
		t.Fatalf("turn relay should carry two urls, got %d", n)
	}
	if got := turn.Get("urls").Index(0).String(); got != "turn:turn.eu.example:3478" {
		t.Fatalf("turn first url = %q", got)
	}
	if got := turn.Get("username").String(); got != "u" {
		t.Fatalf("turn username = %q", got)
	}
	if got := turn.Get("credential").String(); got != "secret" {
		t.Fatalf("turn credential = %q", got)
	}
	// The credential-free auth entry sets neither username nor credential.
	bare := servers.Index(2)
	if !bare.Get("username").IsUndefined() || !bare.Get("credential").IsUndefined() {
		t.Fatalf("a credential-free auth entry must not set username/credential")
	}
	if got := bare.Get("urls").Index(0).String(); got != "stun:stun.eu.example:3478" {
		t.Fatalf("bare auth url = %q", got)
	}
}

// Without an RTCPeerConnection there is nothing to build on, and a page is told
// so before it offers to connect rather than after.
func TestNewPeerWithoutWebRTC(t *testing.T) {
	saved := js.Global().Get("RTCPeerConnection")
	js.Global().Delete("RTCPeerConnection")
	defer js.Global().Set("RTCPeerConnection", saved)

	if _, err := NewPeer(PeerConfig{}); err == nil {
		t.Fatal("NewPeer should fail where there is no RTCPeerConnection")
	} else if !errors.Is(err, ErrTransport) {
		t.Fatalf("the error should be an ErrTransport: %v", err)
	}
}

// A Peer has one role, taken once. Offering then answering, or answering then
// offering, or offering twice, is a mistake in how the page drove it.
func TestPeerRoleIsTakenOnce(t *testing.T) {
	installFake(t)
	ctx := context.Background()

	offerer, err := NewPeer(PeerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = offerer.Close() }()
	if _, err := offerer.Offer(ctx, "collab"); err != nil {
		t.Fatalf("first offer: %v", err)
	}
	if _, err := offerer.Offer(ctx, "collab"); err == nil {
		t.Fatal("a second offer should fail")
	}
	if _, err := offerer.Answer(ctx, "anything"); err == nil {
		t.Fatal("answering after offering should fail")
	}

	answerer, err := NewPeer(PeerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = answerer.Close() }()
	offer, err := offerer2Offer(t)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := answerer.Answer(ctx, offer); err != nil {
		t.Fatalf("answer: %v", err)
	}
	if _, err := answerer.Offer(ctx, "collab"); err == nil {
		t.Fatal("offering after answering should fail")
	}
}

// offerer2Offer makes a throwaway offer blob for tests that need a valid one to
// answer.
func offerer2Offer(t *testing.T) (string, error) {
	t.Helper()
	p, err := NewPeer(PeerConfig{})
	if err != nil {
		return "", err
	}
	defer func() { _ = p.Close() }()
	return p.Offer(context.Background(), "collab")
}

// An answer is something only an offerer has asked for.
func TestAcceptAnswerNeedsAnOffer(t *testing.T) {
	installFake(t)
	p, err := NewPeer(PeerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Close() }()

	if err := p.AcceptAnswer("anything"); err == nil {
		t.Fatal("accepting an answer without having offered should fail")
	} else if !errors.Is(err, ErrTransport) {
		t.Fatalf("error should be an ErrTransport: %v", err)
	}
}

// The channel cannot be taken before there is a role to give it one.
func TestDataChannelNeedsARole(t *testing.T) {
	installFake(t)
	p, err := NewPeer(PeerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Close() }()
	if _, err := p.DataChannel(context.Background()); err == nil {
		t.Fatal("taking the channel before offering or answering should fail")
	}
}

// AcceptAnswer refuses a blob that is not a description, before it touches the
// connection with it.
func TestAcceptAnswerRejectsRubbish(t *testing.T) {
	installFake(t)
	p, err := NewPeer(PeerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Close() }()
	if _, err := p.Offer(context.Background(), "collab"); err != nil {
		t.Fatal(err)
	}
	if err := p.AcceptAnswer("not a real blob"); err == nil {
		t.Fatal("a rubbish answer should be refused")
	}
}

// Answer refuses a rubbish offer likewise.
func TestAnswerRejectsRubbish(t *testing.T) {
	installFake(t)
	p, err := NewPeer(PeerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Close() }()
	if _, err := p.Answer(context.Background(), "not a real blob"); err == nil {
		t.Fatal("a rubbish offer should be refused")
	}
}

// Closing gives everything back, and closing again is not an error. A closed
// Peer refuses every further step rather than acting on a torn-down connection.
func TestClosedPeerRefusesEverything(t *testing.T) {
	installFake(t)
	p, err := NewPeer(PeerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("closing twice should be a no-op, got %v", err)
	}

	ctx := context.Background()
	if _, err := p.Offer(ctx, "collab"); !errors.Is(err, ErrClosed) {
		t.Fatalf("Offer on a closed peer: %v", err)
	}
	if _, err := p.Answer(ctx, "x"); !errors.Is(err, ErrClosed) {
		t.Fatalf("Answer on a closed peer: %v", err)
	}
	if err := p.AcceptAnswer("x"); !errors.Is(err, ErrClosed) {
		t.Fatalf("AcceptAnswer on a closed peer: %v", err)
	}
	if _, err := p.DataChannel(ctx); !errors.Is(err, ErrClosed) {
		t.Fatalf("DataChannel on a closed peer: %v", err)
	}
}

// A context that is already done ends an offer at the first thing it waits on,
// rather than the offer running to completion regardless.
func TestOfferHonoursACancelledContext(t *testing.T) {
	installFake(t)
	p, err := NewPeer(PeerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := p.Offer(ctx, "collab"); !errors.Is(err, context.Canceled) {
		t.Fatalf("a cancelled offer should return the context's error, got %v", err)
	}
}

// When ICE never finishes gathering, the wait ends on the context rather than
// hanging: the one message this design sends must not depend on a peer that
// stopped answering.
func TestGatherICEHonoursTheContext(t *testing.T) {
	installFake(t)
	setFake("stallICE", true)
	p, err := NewPeer(PeerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, err := p.Offer(ctx, "collab"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("a stalled gathering should time out, got %v", err)
	}
}

// When a channel never arrives on the connection, taking it ends on the context
// rather than waiting for a peer that will not deliver one.
func TestDataChannelHonoursTheContext(t *testing.T) {
	installFake(t)
	setFake("noChannel", true)
	p, err := NewPeer(PeerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Close() }()

	offer, err := offerer2Offer(t)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Answer(context.Background(), offer); err != nil {
		t.Fatalf("answer: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, err := p.DataChannel(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waiting for a channel that never arrives should time out, got %v", err)
	}
}

// A browser that refuses an operation is reported to the caller as an error,
// carrying the reason the browser gave, rather than swallowed.
func TestOfferReportsARejection(t *testing.T) {
	installFake(t)
	setFake("rejectOffer", true)
	p, err := NewPeer(PeerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Close() }()

	_, err = p.Offer(context.Background(), "collab")
	if err == nil {
		t.Fatal("an offer the browser refused should fail")
	}
	if !errors.Is(err, ErrTransport) {
		t.Fatalf("error should be an ErrTransport: %v", err)
	}
	if !strings.Contains(err.Error(), "createOffer refused") {
		t.Fatalf("the error should carry the browser's reason, got %v", err)
	}
}

// makeChannel builds a bare channel object whose state a test drives by hand, so
// that awaitOpen's wait — open, error, close, or a context that ends first — can
// be exercised without a whole connection behind it.
const makeChannel = `
(function (state) {
  const listeners = {};
  return {
    readyState: state,
    addEventListener(event, fn) { (listeners[event] ||= []).push(fn); },
    fire(event) { this.readyState = event === "open" ? "open" : "closed"; for (const fn of listeners[event] || []) fn({}); },
  };
})
`

// awaitOpen resolves on the channel opening, fails on it erroring or closing
// first, and gives up when the context ends — each of the four ways the wait for
// an open channel can finish.
func TestAwaitOpen(t *testing.T) {
	installFake(t)
	p, err := NewPeer(PeerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Close() }()
	factory := js.Global().Call("eval", makeChannel)

	t.Run("already open returns at once", func(t *testing.T) {
		ch := factory.Invoke("open")
		got, err := p.awaitOpen(context.Background(), ch)
		if err != nil {
			t.Fatal(err)
		}
		if !got.Equal(ch) {
			t.Fatal("awaitOpen returned a different channel")
		}
	})

	t.Run("opens after a wait", func(t *testing.T) {
		ch := factory.Invoke("connecting")
		done := make(chan error, 1)
		go func() { _, err := p.awaitOpen(context.Background(), ch); done <- err }()
		time.Sleep(10 * time.Millisecond)
		ch.Call("fire", "open")
		if err := <-done; err != nil {
			t.Fatalf("awaitOpen should have resolved on open, got %v", err)
		}
	})

	t.Run("an error before open fails", func(t *testing.T) {
		ch := factory.Invoke("connecting")
		done := make(chan error, 1)
		go func() { _, err := p.awaitOpen(context.Background(), ch); done <- err }()
		time.Sleep(10 * time.Millisecond)
		ch.Call("fire", "error")
		if err := <-done; !errors.Is(err, ErrTransport) {
			t.Fatalf("an errored channel should fail with ErrTransport, got %v", err)
		}
	})

	t.Run("a close before open fails", func(t *testing.T) {
		ch := factory.Invoke("connecting")
		done := make(chan error, 1)
		go func() { _, err := p.awaitOpen(context.Background(), ch); done <- err }()
		time.Sleep(10 * time.Millisecond)
		ch.Call("fire", "close")
		if err := <-done; !errors.Is(err, ErrClosed) {
			t.Fatalf("a closed channel should fail with ErrClosed, got %v", err)
		}
	})

	t.Run("a cancelled context gives up", func(t *testing.T) {
		ch := factory.Invoke("connecting")
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { _, err := p.awaitOpen(ctx, ch); done <- err }()
		time.Sleep(10 * time.Millisecond)
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("a cancelled wait should return the context's error, got %v", err)
		}
	})
}
