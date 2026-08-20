//go:build js && wasm

package collab

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"syscall/js"
)

// The step before [DataChannel]: getting two browsers to the point where a data
// channel exists at all.
//
// # What is missing, and why it is here rather than there
//
// [DataChannel] and [Server.ServeDataChannel] take a channel that is already
// open. Something has to open it, and in a browser that something is the native
// RTCPeerConnection — a page cannot open a UDP socket, and a Go WebRTC stack
// compiled to wasm would carry a second copy of everything the browser already
// has. So a [Peer] is a thin cover over the browser's own connection: it makes
// the offer or the answer, waits for the addresses to be gathered, and hands
// back the channel the two describe.
//
// # One blob each way
//
// The two browsers still have to swap a block of text, and this is built for the
// case where a person carries it — pasted into the chat of the call they are
// already on. So the whole of it is gathered before the blob is handed back:
// non-trickle ICE, one description each way, no second message with the
// addresses in it. The blob is base64 over the connection description's JSON,
// which is one line and survives a chat box that would fold the newlines in the
// SDP itself.
//
// # Who is who
//
// The one that holds the document offers; it creates the channel and plays the
// [Server]. The one that joins answers; its channel arrives on the connection,
// through ondatachannel, rather than being made. Neither is listening for
// anything — this is the same protocol role as everywhere else in the package,
// not a machine that accepts.
//
//	host, _ := collab.NewPeer(collab.PeerConfig{})
//	offer, _ := host.Offer(ctx, "collab")       // paste offer to the guest
//	host.AcceptAnswer(answer)                    // paste the guest's answer back
//	ch, _ := host.DataChannel(ctx)               // open, ready to serve
//	srv.ServeDataChannel(ctx, ch)
//
//	guest, _ := collab.NewPeer(collab.PeerConfig{})
//	answer, _ := guest.Answer(ctx, offer)        // paste answer to the host
//	ch, _ := guest.DataChannel(ctx)
//	collab.Join(ctx, collab.DataChannel(ch), cfg)

// PeerConfig configures a [Peer]. ICEServers is a list of STUN or TURN URLs,
// each the "urls" of one entry in the RTCPeerConnection configuration — for
// example "stun:stun.l.google.com:19302". Empty is a working configuration for
// two browsers on the same network, where no server is needed to discover an
// address that both can reach.
type PeerConfig struct {
	ICEServers []string
	// ICEServersAuth adds STUN/TURN servers that carry long-term credentials —
	// what a TURN relay needs to authenticate and what the plain ICEServers URL
	// list cannot express. Both lists are used together, ICEServers first.
	ICEServersAuth []ICEServerAuth
}

// ICEServerAuth is one STUN/TURN server with long-term credentials. URLs holds
// one or more server URLs that share the same Username and Credential — the form
// a TURN relay needs. It is the credentialed counterpart of a bare ICEServers
// URL, mirroring one { urls, username, credential } entry of an
// RTCPeerConnection's iceServers.
type ICEServerAuth struct {
	URLs       []string
	Username   string
	Credential string
}

// A Peer is one browser's side of a WebRTC connection, from before a channel
// exists until it is open. It wraps the native RTCPeerConnection and is used
// once, by one goroutine: offer or answer, then take the channel, then close.
type Peer struct {
	pc js.Value

	mu     sync.Mutex
	role   string // "", "offerer", or "answerer"
	ch     js.Value
	closed bool
	funcs  []js.Func // callbacks released together on Close

	onData js.Func
	// incoming carries the answerer's data channel, which it does not make but
	// is handed through ondatachannel once the offer names one.
	incoming chan js.Value
}

// NewPeer creates a Peer over a fresh RTCPeerConnection. It fails only where
// there is no RTCPeerConnection to create — outside a browser, or in one old
// enough not to have it — which a page can report before it offers to connect.
func NewPeer(cfg PeerConfig) (*Peer, error) {
	ctor := js.Global().Get("RTCPeerConnection")
	if !ctor.Truthy() {
		return nil, fmt.Errorf("%w: this environment has no RTCPeerConnection", ErrTransport)
	}
	p := &Peer{
		pc:       ctor.New(iceConfiguration(cfg)),
		incoming: make(chan js.Value, 1),
	}
	// The answerer's channel arrives here. A buffer of one means the callback
	// never blocks the page's thread waiting for DataChannel to be called.
	p.onData = js.FuncOf(func(_ js.Value, args []js.Value) any {
		select {
		case p.incoming <- args[0].Get("channel"):
		default:
		}
		return nil
	})
	p.pc.Call("addEventListener", "datachannel", p.onData)
	return p, nil
}

// Offer plays the holder of the document: it creates the data channel, makes an
// offer, and returns the local description once every address has been gathered.
// Hand the returned blob to the other browser, then give its answer back through
// [Peer.AcceptAnswer].
//
// The channel is created here, before the offer, so that it is part of what the
// offer describes and is ordered — a document that arrived out of order would
// not be the document.
func (p *Peer) Offer(ctx context.Context, label string) (string, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return "", ErrClosed
	}
	if p.role != "" {
		p.mu.Unlock()
		return "", fmt.Errorf("%w: this peer already has a role", ErrTransport)
	}
	p.role = "offerer"
	ch := p.pc.Call("createDataChannel", label, map[string]any{"ordered": true})
	p.ch = ch
	p.mu.Unlock()

	offer, err := await(ctx, p.pc.Call("createOffer"))
	if err != nil {
		return "", err
	}
	if _, err := await(ctx, p.pc.Call("setLocalDescription", offer)); err != nil {
		return "", err
	}
	if err := p.gatherICE(ctx); err != nil {
		return "", err
	}
	return p.localDescription(), nil
}

// AcceptAnswer takes the answer the other browser pasted back and completes the
// offerer's side of the exchange. Only an offerer accepts an answer; a peer that
// has not offered has nothing for the answer to complete.
func (p *Peer) AcceptAnswer(blob string) error {
	p.mu.Lock()
	role, closed := p.role, p.closed
	p.mu.Unlock()
	if closed {
		return ErrClosed
	}
	if role != "offerer" {
		return fmt.Errorf("%w: only a peer that offered can accept an answer", ErrTransport)
	}
	sig, err := decodeSignal(blob)
	if err != nil {
		return err
	}
	// setRemoteDescription settles at once in a browser; it is not waited on with
	// the caller's context because there is no call after it to be blocked.
	_, err = await(context.Background(), p.pc.Call("setRemoteDescription", describe(sig)))
	return err
}

// Answer plays the browser that joins: it takes the offerer's pasted offer, sets
// it, makes an answer, and returns the local description once every address has
// been gathered. Hand the returned blob back to the offerer. The data channel is
// not made here — it arrives on the connection, and [Peer.DataChannel] hands it
// over once it is open.
func (p *Peer) Answer(ctx context.Context, remoteOffer string) (string, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return "", ErrClosed
	}
	if p.role != "" {
		p.mu.Unlock()
		return "", fmt.Errorf("%w: this peer already has a role", ErrTransport)
	}
	p.role = "answerer"
	p.mu.Unlock()

	sig, err := decodeSignal(remoteOffer)
	if err != nil {
		return "", err
	}
	if _, err := await(ctx, p.pc.Call("setRemoteDescription", describe(sig))); err != nil {
		return "", err
	}
	answer, err := await(ctx, p.pc.Call("createAnswer"))
	if err != nil {
		return "", err
	}
	if _, err := await(ctx, p.pc.Call("setLocalDescription", answer)); err != nil {
		return "", err
	}
	if err := p.gatherICE(ctx); err != nil {
		return "", err
	}
	return p.localDescription(), nil
}

// DataChannel returns the open channel, for handing to [DataChannel] or
// [Server.ServeDataChannel]. It waits: for the answerer, until the channel has
// arrived on the connection; for either side, until its readyState is "open".
// Call it after the exchange — after [Peer.AcceptAnswer] on the offerer, after
// [Peer.Answer] on the answerer — since a channel opens only once both
// descriptions are set.
func (p *Peer) DataChannel(ctx context.Context) (js.Value, error) {
	p.mu.Lock()
	closed, role, ch := p.closed, p.role, p.ch
	p.mu.Unlock()
	if closed {
		return js.Value{}, ErrClosed
	}
	if role == "" {
		return js.Value{}, fmt.Errorf("%w: offer or answer before taking the channel", ErrTransport)
	}

	if !ch.Truthy() {
		select {
		case ch = <-p.incoming:
			p.mu.Lock()
			p.ch = ch
			p.mu.Unlock()
		case <-ctx.Done():
			return js.Value{}, ctx.Err()
		}
	}
	return p.awaitOpen(ctx, ch)
}

// awaitOpen resolves once the channel's readyState is "open", or fails if it
// closes or errors first. A channel already open fires no further open event, so
// its state is read before anything is waited for.
func (p *Peer) awaitOpen(ctx context.Context, ch js.Value) (js.Value, error) {
	if ch.Get("readyState").String() == "open" {
		return ch, nil
	}
	settled := make(chan error, 1)
	var once sync.Once
	finish := func(err error) { once.Do(func() { settled <- err }) }

	onOpen := js.FuncOf(func(js.Value, []js.Value) any { finish(nil); return nil })
	onError := js.FuncOf(func(js.Value, []js.Value) any {
		finish(fmt.Errorf("%w: the data channel failed", ErrTransport))
		return nil
	})
	onClose := js.FuncOf(func(js.Value, []js.Value) any { finish(ErrClosed); return nil })
	for event, fn := range map[string]js.Func{"open": onOpen, "error": onError, "close": onClose} {
		ch.Call("addEventListener", event, fn)
	}
	p.track(onOpen, onError, onClose)

	// Read once more, in case it opened between the check above and the listener
	// being wired: the event would already be gone.
	if ch.Get("readyState").String() == "open" {
		return ch, nil
	}
	select {
	case err := <-settled:
		if err != nil {
			return js.Value{}, err
		}
		return ch, nil
	case <-ctx.Done():
		return js.Value{}, ctx.Err()
	}
}

// gatherICE waits for non-trickle ICE to finish, so the description handed back
// carries every address in it and no second message is needed. A connection
// whose gathering is already complete is not waited on.
func (p *Peer) gatherICE(ctx context.Context) error {
	if p.pc.Get("iceGatheringState").String() == "complete" {
		return nil
	}
	done := make(chan struct{})
	var once sync.Once
	fn := js.FuncOf(func(js.Value, []js.Value) any {
		if p.pc.Get("iceGatheringState").String() == "complete" {
			once.Do(func() { close(done) })
		}
		return nil
	})
	p.pc.Call("addEventListener", "icegatheringstatechange", fn)
	p.track(fn)

	// Re-read after wiring the listener, to catch a transition that happened in
	// between and would otherwise never wake the wait.
	if p.pc.Get("iceGatheringState").String() == "complete" {
		return nil
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// localDescription reads the gathered description off the connection and renders
// it as a blob to paste.
func (p *Peer) localDescription() string {
	desc := p.pc.Get("localDescription")
	return encodeSignal(signal{Type: desc.Get("type").String(), SDP: desc.Get("sdp").String()})
}

// track keeps a callback alive until Close, which is the only safe time to
// release it: a js.Func released while an event it listens for might still fire
// panics the moment it does.
func (p *Peer) track(fns ...js.Func) {
	p.mu.Lock()
	p.funcs = append(p.funcs, fns...)
	p.mu.Unlock()
}

// Close ends the connection and gives every callback back. It is safe to call
// more than once, and safe to call on a handshake that never finished.
func (p *Peer) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	funcs := append([]js.Func{p.onData}, p.funcs...)
	p.funcs = nil
	p.mu.Unlock()

	p.pc.Call("close")
	for _, f := range funcs {
		f.Release()
	}
	return nil
}

// A signal is a WebRTC connection description — the {type, sdp} the two browsers
// swap. It is the whole of what one side tells the other, and nothing about the
// document travels in it.
type signal struct {
	Type string `json:"type"`
	SDP  string `json:"sdp"`
}

// encodeSignal renders a description as one line to paste. It is base64 over the
// JSON rather than the JSON itself because the SDP is many lines, and a chat box
// or a mail client is free to reflow those; base64 has no newline to fold.
//
// Marshalling two strings cannot fail, so the error json returns here is one
// that never happens; it is dropped rather than dressed up as something a caller
// could act on.
func encodeSignal(s signal) string {
	raw, _ := json.Marshal(s)
	return base64.StdEncoding.EncodeToString(raw)
}

// decodeSignal reads a blob back. It refuses one that is not base64, that is not
// the JSON of a description, or that is missing a field — a half-pasted blob is
// a mistake to report, not a description to set.
func decodeSignal(blob string) (signal, error) {
	raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(blob))
	if err != nil {
		return signal{}, fmt.Errorf("%w: the pasted description is not valid base64", ErrTransport)
	}
	var s signal
	if err := json.Unmarshal(raw, &s); err != nil {
		return signal{}, fmt.Errorf("%w: the pasted description is not a connection description", ErrTransport)
	}
	if s.Type == "" || s.SDP == "" {
		return signal{}, fmt.Errorf("%w: the pasted description is missing its type or sdp", ErrTransport)
	}
	return s, nil
}

// describe builds the RTCSessionDescriptionInit that setLocalDescription and
// setRemoteDescription take.
func describe(s signal) js.Value {
	d := js.Global().Get("Object").New()
	d.Set("type", s.Type)
	d.Set("sdp", s.SDP)
	return d
}

// iceConfiguration builds the RTCPeerConnection configuration. The plain URL
// list and the credentialed servers are appended in that order; when neither is
// given iceServers is left unset, which is a working configuration on one
// network. A credentialed entry sets "username"/"credential" only when non-empty
// so a bare STUN URL given through ICEServersAuth is not sent empty strings.
func iceConfiguration(cfg PeerConfig) js.Value {
	config := js.Global().Get("Object").New()
	servers := js.Global().Get("Array").New()
	for _, url := range cfg.ICEServers {
		server := js.Global().Get("Object").New()
		server.Set("urls", url)
		servers.Call("push", server)
	}
	for _, s := range cfg.ICEServersAuth {
		server := js.Global().Get("Object").New()
		urls := js.Global().Get("Array").New()
		for _, u := range s.URLs {
			urls.Call("push", u)
		}
		server.Set("urls", urls)
		if s.Username != "" {
			server.Set("username", s.Username)
		}
		if s.Credential != "" {
			server.Set("credential", s.Credential)
		}
		servers.Call("push", server)
	}
	if servers.Length() > 0 {
		config.Set("iceServers", servers)
	}
	return config
}

// await turns a JavaScript promise into a Go return. It resolves on the page's
// thread and hands the result over a channel, so the goroutine that called it
// blocks without blocking the page.
//
// On the context being cancelled it returns at once but does not release its
// callbacks: the promise may still settle and call into them, and a released
// js.Func called is a panic. They are released once it settles, however late.
func await(ctx context.Context, promise js.Value) (js.Value, error) {
	type outcome struct {
		val js.Value
		err error
	}
	ch := make(chan outcome, 1)
	var onOK, onErr js.Func
	onOK = js.FuncOf(func(_ js.Value, args []js.Value) any {
		var v js.Value
		if len(args) > 0 {
			v = args[0]
		}
		ch <- outcome{val: v}
		return nil
	})
	onErr = js.FuncOf(func(_ js.Value, args []js.Value) any {
		msg := "the browser rejected a WebRTC operation"
		if len(args) > 0 && args[0].Truthy() {
			if m := args[0].Get("message"); m.Type() == js.TypeString {
				msg = m.String()
			}
		}
		ch <- outcome{err: fmt.Errorf("%w: %s", ErrTransport, msg)}
		return nil
	})
	promise.Call("then", onOK).Call("catch", onErr)

	release := func() { onOK.Release(); onErr.Release() }
	select {
	case r := <-ch:
		release()
		return r.val, r.err
	case <-ctx.Done():
		go func() { <-ch; release() }()
		return js.Value{}, ctx.Err()
	}
}
