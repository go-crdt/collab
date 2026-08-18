//go:build !js

package collab

import (
	"context"
	"net/http"

	"github.com/coder/websocket"
)

// ServeWebSocket returns an http.Handler that runs sessions over WebSockets —
// the carrier a browser can afford, and the one [WebSocket] dials.
//
// Mount it where the browser will reach it. Everything else is the same server:
// the same documents, the same store, the same [Config.Authorize], and a
// participant here edits the same document as one arriving over gRPC.
//
// origins, when not empty, are the Origin header values allowed to open a
// session, which is the check that stops another site's page from opening one
// with the visitor's cookies. An empty list allows only same-origin requests.
func (s *Server) ServeWebSocket(origins ...string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
			OriginPatterns: origins,
		})
		if err != nil {
			return // Accept has already answered the request
		}
		conn.SetReadLimit(maxMessage)
		// The request's context ends when the handler returns, which is what
		// closes the session; the connection itself is closed either way.
		ctx := r.Context()
		defer func() { _ = conn.CloseNow() }()

		// A participant is told why in the close frame: a WebSocket has no status
		// code to put it in, and a session that simply stops tells them nothing.
		code, reason := closing(s.session(&wsCarrier{ctx: ctx, conn: conn}))
		_ = conn.Close(code, reason)
	})
}

// closing turns why a session ended into a close frame.
//
// A frame allows 123 bytes of reason and is dropped whole if given more, so a
// long refusal is cut rather than lost. The reason is what the participant will
// see, and cutting it keeps the beginning, which is where the refusal is.
func closing(err error) (websocket.StatusCode, string) {
	if err == nil {
		return websocket.StatusNormalClosure, ""
	}
	const limit = 120
	reason := err.Error()
	if len(reason) > limit {
		reason = reason[:limit]
	}
	return websocket.StatusPolicyViolation, reason
}

// A wsCarrier presents a WebSocket to the session logic, which speaks the wire
// format in wire.go — so this is a socket and a framing check, and nothing
// else.
//
// It used to convert every message into a protobuf one and back again, because
// the session logic was written in protobuf. That cost a translation each way
// on the only path a browser takes, and it cost far more than the work: it made
// the server type depend on the generated code, which is what kept the session
// logic out of the browser at all.
type wsCarrier struct {
	ctx  context.Context
	conn *websocket.Conn
}

func (c *wsCarrier) Context() context.Context { return c.ctx }

func (c *wsCarrier) Recv() (byte, any, error) {
	typ, raw, err := c.conn.Read(c.ctx)
	if err != nil {
		return 0, nil, err
	}
	if typ != websocket.MessageBinary {
		return 0, nil, ErrProtocol
	}
	return decodeClient(raw)
}

func (c *wsCarrier) Send(kind byte, msg any) error {
	raw, err := encodeServer(kind, msg)
	if err != nil {
		return err
	}
	return c.conn.Write(c.ctx, websocket.MessageBinary, raw)
}
