//go:build js && wasm

package collab

import (
	"context"
	"fmt"
	"syscall/js"
)

// Two browsers, editing the same document, with nothing between them.
//
// # What this is for
//
// Two researchers on a video call, one in France and one in the United States,
// editing the same LaTeX source. Neither wants a third party holding it, and
// neither has a server. What they have is a way to send each other a line of
// text — the call itself, a mail, a message — and that is enough.
//
// # What a browser cannot do, and what it does not need to
//
// A browser cannot listen. There is no socket to accept on, so two of them
// cannot connect the way a browser connects to a server: both know only how to
// call, and nobody answers. WebRTC is the way around that, and it works by
// having each side describe itself in a block of text — where it can be
// reached, what it can encrypt — which the two then swap. Once swapped, the
// connection is direct.
//
// The swap needs a channel, and it does not need a server. Paste the block into
// the chat window of the call you are already on. The document never goes near
// whoever carries it; the block says how to reach a browser and nothing about
// what is in it.
//
// So the signalling is not here. It is a product decision — a text box, a QR
// code, a link — and a library that chose one would be choosing for everybody.
// This takes a data channel that is already open.
//
// Opening one is the step before, and [Peer] is a thin cover over the browser's
// own RTCPeerConnection for it: a non-trickle offer or answer, one blob to paste
// each way, and the open channel handed back. It stops short of carrying the
// blob between the two, which is the product decision above.
//
// # Which side is the server
//
// One of them holds the document and answers the other, which is what a Server
// does; it is a role in the protocol and not a machine, and neither browser is
// listening for anything. [DataChannel] is the side that joins;
// [ServeDataChannel] is the side that holds.
//
// The whole session logic compiles for the browser, and costs nothing to have
// available: see the note in sessionerr.go about what it used to cost.

// DataChannel returns a transport over an RTCDataChannel the page has already
// opened. The channel must be open — its readyState "open" — because a
// participant that joins before the connection exists has nothing to join.
func DataChannel(ch js.Value) Transport { return &rtcTransport{ch: ch} }

type rtcTransport struct{ ch js.Value }

func (t *rtcTransport) open(ctx context.Context) (carrierConn, error) {
	return openDataChannel(ctx, t.ch, decodeServer, encodeClient)
}

// ServeDataChannel runs one session over a data channel, with this browser
// holding the document. It returns when the session ends.
//
// It is the counterpart of [Server.ServeWebSocket], for a page rather than a
// listener: there is no request to upgrade and no origin to check, because the
// page decided who it was talking to when it swapped connection descriptions
// with them.
func (s *Server) ServeDataChannel(ctx context.Context, ch js.Value) error {
	conn, err := openDataChannel(ctx, ch, decodeClient, encodeServer)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	return s.session(&rtcCarrier{ctx: ctx, conn: conn})
}

// rtcCarrier presents a data channel to the session logic. It is the same three
// methods a gRPC stream and a WebSocket present, which is all the session has
// ever needed of a transport.
type rtcCarrier struct {
	ctx  context.Context
	conn carrierConn
}

func (c *rtcCarrier) Context() context.Context      { return c.ctx }
func (c *rtcCarrier) Recv() (byte, any, error)      { return c.conn.Recv() }
func (c *rtcCarrier) Send(kind byte, msg any) error { return c.conn.Send(kind, msg) }

// openDataChannel wires the callbacks and hands back a connection, sharing
// everything below with the browser's WebSocket: a data channel and a WebSocket
// are the same shape from here — send, close, and four events.
func openDataChannel(ctx context.Context, ch js.Value,
	decode func([]byte) (byte, any, error), encode func(byte, any) ([]byte, error),
) (carrierConn, error) {
	if !ch.Truthy() {
		return nil, fmt.Errorf("%w: no data channel", ErrTransport)
	}
	c := &jsConn{
		ws:     ch,
		decode: decode,
		encode: encode,
		ready:  make(chan struct{}),
		closed: make(chan struct{}),
		woken:  make(chan struct{}, 1),
	}
	// A message arrives as an ArrayBuffer rather than a Blob, which is what
	// makes reading it synchronous.
	c.ws.Set("binaryType", "arraybuffer")

	c.onOpen = js.FuncOf(func(js.Value, []js.Value) any {
		c.settle(nil)
		return nil
	})
	c.onMessage = js.FuncOf(func(_ js.Value, args []js.Value) any {
		buf := js.Global().Get("Uint8Array").New(args[0].Get("data"))
		raw := make([]byte, buf.Length())
		js.CopyBytesToGo(raw, buf)
		c.push(raw)
		return nil
	})
	c.onError = js.FuncOf(func(js.Value, []js.Value) any {
		// The event carries nothing useful, as it does for a WebSocket: a
		// failed connection must not tell a script why.
		c.settle(fmt.Errorf("%w: the data channel failed", ErrTransport))
		return nil
	})
	c.onClose = js.FuncOf(func(js.Value, []js.Value) any {
		c.settle(ErrClosed)
		return nil
	})
	for event, fn := range map[string]js.Func{
		"open": c.onOpen, "message": c.onMessage, "error": c.onError, "close": c.onClose,
	} {
		c.ws.Call("addEventListener", event, fn)
	}

	// A channel the page has already opened fires no further open event, so its
	// state is read rather than waited for. Waiting would hang on exactly the
	// case this is meant for.
	if ch.Get("readyState").String() == "open" {
		c.settle(nil)
	}

	select {
	case <-c.ready:
		if err := c.reason(); err != nil {
			c.release()
			return nil, err
		}
		return c, nil
	case <-ctx.Done():
		c.release()
		return nil, ctx.Err()
	}
}
