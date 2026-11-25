//go:build !js

package collab

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// Guards `mutsweep -sites all` found nothing holding. Each test carries its
// control first.

// backoff.go:114. A sleep that ended with an error ends the wait, and the
// interval does not double for a wait that never finished.
func TestABackoffThatWasInterruptedReportsItAndDoesNotGrow(t *testing.T) {
	stop := errors.New("interrupted")
	b := newBackoff(RetryPolicy{Wait: time.Second, Ceiling: time.Minute})
	b.random = func() float64 { return 1 }
	b.sleep = func(context.Context, time.Duration) error { return nil }
	// The control: a wait that finishes doubles the interval.
	if err := b.down(t.Context(), errors.New("x")); err != nil || b.wait != 2*time.Second {
		t.Fatalf("control: down = %v, wait %v, want nil and 2s", err, b.wait)
	}
	b.sleep = func(context.Context, time.Duration) error { return stop }
	if err := b.down(t.Context(), errors.New("x")); !errors.Is(err, stop) {
		t.Errorf("down = %v, want what ended the wait", err)
	}
	if b.wait != 2*time.Second {
		t.Errorf("the interval grew to %v for a wait that was interrupted", b.wait)
	}
}

// reconnect.go:262. What ends the backoff ends the loop: with the guard gone the
// loop makes one more attempt, a dial nobody asked for, before the context says
// the same thing at the top.
func TestALinkStopsWhenItsWaitIsInterruptedWithoutDialling(t *testing.T) {
	srv := NewServer(Config{Store: NewMemoryStore()})
	t.Cleanup(func() { _ = srv.Close(context.Background()) })
	var dials atomic.Int32
	link, err := srv.reconnecting(func(context.Context) (Transport, error) {
		dials.Add(1)
		return nil, errors.New("not reachable")
	}, "doc", 999, RetryPolicy{Wait: time.Millisecond, Ceiling: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	stop := errors.New("interrupted")
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var sleeps int
	link.back.sleep = func(context.Context, time.Duration) error {
		sleeps++
		if sleeps == 1 {
			return stop
		}
		// Reached only if the loop went on after the first one: end it, loudly.
		t.Error("the loop slept again after its wait was interrupted")
		cancel()
		return ctx.Err()
	}
	if err := link.run(ctx); !errors.Is(err, stop) {
		t.Errorf("run = %v, want what ended the wait", err)
	}
	if n := dials.Load(); n != 1 {
		t.Errorf("dialled %d times, want 1", n)
	}
}

// serve_websocket.go:27. A request that is not a WebSocket upgrade is answered
// by Accept and ends there; with the guard gone the handler goes on with a nil
// connection.
func TestARequestThatIsNotAnUpgradeIsAnsweredAndDropped(t *testing.T) {
	srv := NewServer(Config{})
	h := srv.ServeWebSocket("*")
	rec := httptest.NewRecorder()
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("the handler went on with a connection that was never made: %v", r)
			}
		}()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	}()
	if rec.Code == http.StatusSwitchingProtocols || rec.Code == 0 {
		t.Errorf("status %d for a request that is not an upgrade", rec.Code)
	}
}

// serve_websocket.go:81 and websocket.go:47. A TEXT frame is refused even when
// its bytes are a perfectly good message: the carrier is binary, and the text
// "hello" the older tests send is refused by the decoder whether or not this
// check is there.
func TestAValidMessageInATextFrameIsRefusedByBothCarriers(t *testing.T) {
	join, err := encodeClient(kindJoin, joinMsg{Document: "d", Site: 1})
	if err != nil {
		t.Fatal(err)
	}
	welcome, err := encodeServer(kindWelcome, welcomeMsg{})
	if err != nil {
		t.Fatal(err)
	}

	t.Run("client side", func(t *testing.T) {
		for _, tc := range []struct {
			typ  websocket.MessageType
			want bool
		}{{websocket.MessageBinary, false}, {websocket.MessageText, true}} {
			url := serveRaw(t, func(ctx context.Context, conn *websocket.Conn) {
				_ = conn.Write(ctx, tc.typ, welcome)
			})
			conn, err := WebSocket(url).open(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			_, _, err = conn.Recv()
			conn.Close()
			if got := errors.Is(err, ErrProtocol); got != tc.want {
				t.Errorf("a welcome in a %v frame: ErrProtocol = %v (%v), want %v", tc.typ, got, err, tc.want)
			}
		}
	})

	t.Run("server side", func(t *testing.T) {
		got := make(chan error, 2)
		front := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{OriginPatterns: []string{"*"}})
			if err != nil {
				return
			}
			defer conn.CloseNow()
			_, _, err = (&wsCarrier{ctx: r.Context(), conn: conn}).Recv()
			got <- err
		}))
		t.Cleanup(front.Close)
		url := "ws" + strings.TrimPrefix(front.URL, "http")
		for _, tc := range []struct {
			typ  websocket.MessageType
			want bool
		}{{websocket.MessageBinary, false}, {websocket.MessageText, true}} {
			conn, _, err := websocket.Dial(t.Context(), url, nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := conn.Write(t.Context(), tc.typ, join); err != nil {
				t.Fatal(err)
			}
			err = <-got
			_ = conn.CloseNow()
			if gotP := errors.Is(err, ErrProtocol); gotP != tc.want {
				t.Errorf("a join in a %v frame: ErrProtocol = %v (%v), want %v", tc.typ, gotP, err, tc.want)
			}
		}
	})
}

// serve_websocket.go:78 and websocket.go:44. A read that FAILED answers with
// the failure. With the guard gone the zero message type that comes with it is
// not binary, and the connection reports a protocol error for a peer that
// merely went away, which is what a caller deciding whether to retry reads.
func TestAPeerThatWentAwayIsNotAProtocolError(t *testing.T) {
	url := serveRaw(t, func(ctx context.Context, conn *websocket.Conn) {
		_ = conn.Close(websocket.StatusNormalClosure, "")
	})
	conn, err := WebSocket(url).open(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, _, err := conn.Recv(); err == nil || errors.Is(err, ErrProtocol) {
		t.Errorf("client Recv after the peer closed = %v, want the close, not a protocol error", err)
	}

	got := make(chan error, 1)
	front := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, &websocket.AcceptOptions{OriginPatterns: []string{"*"}})
		if err != nil {
			return
		}
		defer c.CloseNow()
		_, _, err = (&wsCarrier{ctx: r.Context(), conn: c}).Recv()
		got <- err
	}))
	t.Cleanup(front.Close)
	peer, _, err := websocket.Dial(t.Context(), "ws"+strings.TrimPrefix(front.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = peer.Close(websocket.StatusNormalClosure, "")
	if err := <-got; err == nil || errors.Is(err, ErrProtocol) {
		t.Errorf("server Recv after the peer closed = %v, want the close, not a protocol error", err)
	}
}
