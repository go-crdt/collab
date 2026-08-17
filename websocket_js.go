//go:build js

package collab

import (
	"context"
	"fmt"
	"sync"
	"syscall/js"
)

// In a browser the WebSocket is spoken to directly rather than through a
// library, and the reason is size. Every Go WebSocket package reaches for
// net/http — for its header type if nothing else — and net/http compiled to
// wasm brings TLS, HTTP/2 and the rest of a stack a page already has and cannot
// use. Measured on the browser test client, gzipped: 1 500 KB through a library
// against 919 KB through this. The browser's own WebSocket is four callbacks, so
// the saving costs a hundred lines rather than a dependency.
//
// The non-browser build uses a library, in websocket_notjs.go, where none of
// this matters.

func (t *wsTransport) open(ctx context.Context) (carrierConn, error) {
	ctor := js.Global().Get("WebSocket")
	if !ctor.Truthy() {
		return nil, fmt.Errorf("%w: this environment has no WebSocket", ErrTransport)
	}
	c := &jsConn{
		ws:     ctor.New(t.url),
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
		// The event carries nothing a page is allowed to see, by design: a
		// failed handshake must not tell a script why it failed.
		c.settle(fmt.Errorf("%w: the connection to %s failed", ErrTransport, t.url))
		return nil
	})
	c.onClose = js.FuncOf(func(_ js.Value, args []js.Value) any {
		err := ErrClosed
		if reason := args[0].Get("reason").String(); reason != "" {
			err = fmt.Errorf("collab: the server ended the session: %s", reason)
		}
		c.settle(err)
		return nil
	})
	for event, fn := range map[string]js.Func{
		"open": c.onOpen, "message": c.onMessage, "error": c.onError, "close": c.onClose,
	} {
		c.ws.Call("addEventListener", event, fn)
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

// A jsConn is one browser WebSocket presented as a carrier.
//
// The callbacks run on the page's single thread and the session's goroutines do
// not, so everything they touch is behind mu. Messages are queued rather than
// handed over on a channel: a callback that blocked would block the page, which
// is not a thing a page may do.
type jsConn struct {
	ws     js.Value
	ready  chan struct{}
	closed chan struct{}
	woken  chan struct{}

	onOpen, onMessage, onError, onClose js.Func

	mu       sync.Mutex
	queue    [][]byte
	err      error
	settled  bool
	released bool
}

// settle records the first thing that ended the wait for a connection, or ended
// the connection itself, and wakes whoever is waiting for either.
func (c *jsConn) settle(err error) {
	c.mu.Lock()
	if c.err == nil {
		c.err = err
	}
	first := !c.settled
	c.settled = true
	c.mu.Unlock()
	if first {
		close(c.ready)
	}
	if err != nil {
		c.finish()
	}
	c.wake()
}

// finish marks the connection as over, once.
func (c *jsConn) finish() {
	select {
	case <-c.closed:
	default:
		close(c.closed)
	}
}

func (c *jsConn) reason() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}

// push queues a received message.
func (c *jsConn) push(raw []byte) {
	c.mu.Lock()
	c.queue = append(c.queue, raw)
	c.mu.Unlock()
	c.wake()
}

// wake nudges a waiting Recv without ever blocking the page's thread.
func (c *jsConn) wake() {
	select {
	case c.woken <- struct{}{}:
	default:
	}
}

func (c *jsConn) Send(kind byte, msg any) error {
	raw, err := encodeClient(kind, msg)
	if err != nil {
		return err
	}
	if err := c.reason(); err != nil {
		return err
	}
	buf := js.Global().Get("Uint8Array").New(len(raw))
	js.CopyBytesToJS(buf, raw)
	c.ws.Call("send", buf)
	return nil
}

// Recv hands over the next message, or waits for one. What is queued is drained
// before the reason the connection ended is reported, so a session that ends
// mid-flight still delivers what had already arrived.
func (c *jsConn) Recv() (byte, any, error) {
	for {
		c.mu.Lock()
		if len(c.queue) > 0 {
			raw := c.queue[0]
			c.queue = c.queue[1:]
			c.mu.Unlock()
			return decodeServer(raw)
		}
		err := c.err
		c.mu.Unlock()
		if err != nil {
			return 0, nil, err
		}
		select {
		case <-c.woken:
		case <-c.closed:
		}
	}
}

func (c *jsConn) Close() error {
	c.settle(ErrClosed)
	c.ws.Call("close")
	c.release()
	return nil
}

// release drops the callbacks. A js.Func that is never released keeps its Go
// closure alive for as long as the page, and a session is opened per document.
func (c *jsConn) release() {
	c.mu.Lock()
	if c.released {
		c.mu.Unlock()
		return
	}
	c.released = true
	c.mu.Unlock()
	c.finish()
	for _, f := range []js.Func{c.onOpen, c.onMessage, c.onError, c.onClose} {
		f.Release()
	}
}
