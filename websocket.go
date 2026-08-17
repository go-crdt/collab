//go:build !js

package collab

import (
	"context"
	"fmt"
	"net/http"

	"github.com/coder/websocket"
)

func (t *wsTransport) open(ctx context.Context) (carrierConn, error) {
	conn, _, err := websocket.Dial(ctx, t.url, &websocket.DialOptions{HTTPHeader: http.Header(t.header)})
	if err != nil {
		return nil, fmt.Errorf("%w: dialling %s: %w", ErrTransport, t.url, err)
	}
	// A snapshot of a large document is one message, and the default limit is
	// meant for chat traffic.
	conn.SetReadLimit(maxMessage)
	return &wsConn{ctx: ctx, conn: conn}, nil
}

// maxMessage bounds one message. A document's whole snapshot travels in one, so
// this is the size of the largest document a session will open, not the size of
// an edit.
const maxMessage = 1 << 30

type wsConn struct {
	ctx  context.Context
	conn *websocket.Conn
}

func (c *wsConn) Send(kind byte, msg any) error {
	raw, err := encodeClient(kind, msg)
	if err != nil {
		return err
	}
	return c.conn.Write(c.ctx, websocket.MessageBinary, raw)
}

func (c *wsConn) Recv() (byte, any, error) {
	typ, raw, err := c.conn.Read(c.ctx)
	if err != nil {
		return 0, nil, err
	}
	if typ != websocket.MessageBinary {
		return 0, nil, ErrProtocol
	}
	return decodeServer(raw)
}

func (c *wsConn) Close() error {
	return c.conn.Close(websocket.StatusNormalClosure, "")
}
