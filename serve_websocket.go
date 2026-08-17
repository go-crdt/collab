//go:build !js

package collab

import (
	"context"
	"net/http"

	"github.com/coder/websocket"
	"github.com/go-crdt/collab/collabpb"
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

// A wsCarrier presents a WebSocket to the session logic as the gRPC stream
// presents itself, so the two carriers share every line of that logic.
type wsCarrier struct {
	ctx  context.Context
	conn *websocket.Conn
}

func (c *wsCarrier) Context() context.Context { return c.ctx }

func (c *wsCarrier) Recv() (*collabpb.ClientMessage, error) {
	typ, raw, err := c.conn.Read(c.ctx)
	if err != nil {
		return nil, err
	}
	if typ != websocket.MessageBinary {
		return nil, ErrProtocol
	}
	kind, msg, err := decodeClient(raw)
	if err != nil {
		return nil, err
	}
	out := &collabpb.ClientMessage{}
	switch kind {
	case kindJoin:
		m := msg.(joinMsg)
		out.Body = &collabpb.ClientMessage_Join{Join: &collabpb.Join{
			Document: m.Document, Site: m.Site, Have: m.Have,
		}}
	case kindOperation:
		out.Body = &collabpb.ClientMessage_Operations{
			Operations: &collabpb.Operations{Operations: msg.(opsMsg).Operations},
		}
	default:
		out.Body = &collabpb.ClientMessage_Presence{
			Presence: &collabpb.Presence{Update: msg.(presenceMsg).Update},
		}
	}
	return out, nil
}

func (c *wsCarrier) Send(msg *collabpb.ServerMessage) error {
	var raw []byte
	var err error
	switch body := msg.GetBody().(type) {
	case *collabpb.ServerMessage_Welcome:
		w := body.Welcome
		raw, err = encodeServer(kindWelcome, welcomeMsg{
			Snapshot:   w.GetSnapshot(),
			Operations: w.GetOperations(),
			Version:    w.GetVersion(),
			Presence:   w.GetPresence(),
		})
	case *collabpb.ServerMessage_Operations:
		raw, err = encodeServer(kindOperation, opsMsg{Operations: body.Operations.GetOperations()})
	case *collabpb.ServerMessage_Presence:
		raw, err = encodeServer(kindPresence, presenceMsg{Update: body.Presence.GetUpdate()})
	default:
		err = ErrProtocol
	}
	if err != nil {
		return err
	}
	return c.conn.Write(c.ctx, websocket.MessageBinary, raw)
}
