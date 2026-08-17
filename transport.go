package collab

import (
	"context"
	"errors"
)

// A Transport is how a participant reaches a server. [WebSocket] works
// anywhere, a browser included; [GRPC] works outside one.
//
// There are two because of what they cost where they run. Outside a browser a
// carrier costs nothing anybody notices, and gRPC brings deadlines, interceptors
// and everything already built around them. Inside one it is paid for on every
// load: protobuf alone is six times the size of the whole CRDT compiled to
// wasm — see wire.go for the measurements — so the browser gets a framing of
// four message kinds over a plain WebSocket instead.
//
// Both carry the same session, byte for byte in the fields that matter, because
// every field in these messages is something [github.com/go-crdt/crdt] encoded
// and will check on arrival. A participant on one and a participant on the other
// can edit the same document.
type Transport interface {
	// open starts one session. The carrier it returns is used by one goroutine
	// for sending and one for receiving, which is what a WebSocket and a gRPC
	// stream both allow.
	open(ctx context.Context) (carrierConn, error)
}

// A carrierConn is one participant's connection, in messages rather than bytes.
type carrierConn interface {
	// Send writes one client message. It is called from one goroutine at a time.
	Send(kind byte, msg any) error
	// Recv reads one server message, blocking until there is one.
	Recv() (kind byte, msg any, err error)
	// Close ends the session.
	Close() error
}

// ErrTransport reports a carrier that could not be opened or that failed.
var ErrTransport = errors.New("collab: transport")

// WebSocket returns a transport that opens sessions at url, which is "ws://" or
// "wss://" and the path the server's handler is mounted at.
//
// This is the transport a browser uses, and the one to reach for by default:
// it is what the same code compiled to wasm can afford. See [Transport].
func WebSocket(url string, opts ...WebSocketOption) Transport {
	t := &wsTransport{url: url}
	for _, opt := range opts {
		opt(t)
	}
	return t
}

// A WebSocketOption configures [WebSocket].
type WebSocketOption func(*wsTransport)

type wsTransport struct {
	url string
	// header is the opening handshake's headers, kept as the plain map rather
	// than an http.Header so that this file compiles for a browser, where there
	// is no such thing: a page cannot put a header on a WebSocket handshake, and
	// does not need to, because it sends its own cookies. See
	// websocket_notjs.go.
	header map[string][]string
}
