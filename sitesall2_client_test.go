//go:build !js

package collab

import (
	"context"
	"errors"
	"sync"
	"testing"
)

// A transport whose connections remember everything written to them and then
// wait for the context, the way a quiet server does.
type recTransport struct {
	mu       sync.Mutex
	welcome  welcomeMsg
	sendErr  error
	closed   int
	sentKind []byte
	ctx      context.Context
}

func (r *recTransport) open(ctx context.Context) (carrierConn, error) {
	r.ctx = ctx
	return &recConn{r: r}, nil
}

func (r *recTransport) count(kind byte) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, k := range r.sentKind {
		if k == kind {
			n++
		}
	}
	return n
}

type recConn struct {
	r    *recTransport
	sent bool
}

func (c *recConn) Send(kind byte, _ any) error {
	c.r.mu.Lock()
	defer c.r.mu.Unlock()
	if c.r.sendErr != nil {
		return c.r.sendErr
	}
	c.r.sentKind = append(c.r.sentKind, kind)
	return nil
}

func (c *recConn) Recv() (byte, any, error) {
	if !c.sent {
		c.sent = true
		return kindWelcome, c.r.welcome, nil
	}
	<-c.r.ctx.Done()
	return 0, nil, c.r.ctx.Err()
}

func (c *recConn) Close() error {
	c.r.mu.Lock()
	c.r.closed++
	c.r.mu.Unlock()
	return nil
}

func recClient(t *testing.T) (*Client, *recTransport) {
	t.Helper()
	tp := &recTransport{}
	ctx, cancel := context.WithCancel(context.Background())
	c, err := joinOn(ctx, cancel, tp, ClientConfig{Document: "d", Site: 2})
	if err != nil {
		cancel()
		t.Fatalf("joinOn: %v", err)
	}
	go c.receive(c.conn, c.finished)
	t.Cleanup(func() { _ = c.Close() })
	return c, tp
}

// client.go:540. An edit that produced nothing sends nothing: not an empty batch
// of operations the server then has to read.
func TestAnEditThatChangedNothingSendsNothing(t *testing.T) {
	c, tp := recClient(t)

	// Control: a send while the participant is up reaches the carrier.
	if err := c.transmit(kindOperation, opsMsg{}); err != nil {
		t.Fatal(err)
	}
	if got := tp.count(kindOperation); got != 1 {
		t.Fatalf("a send while up wrote %d operations, want 1", got)
	}

	if err := c.publish(nil); err != nil {
		t.Fatalf("publish(nil) = %v", err)
	}
	if got := tp.count(kindOperation); got != 1 {
		t.Fatalf("an edit that changed nothing sent operations: %d in all, want 1", got)
	}
}

// client.go:587. A supervised client with no carrier does not fail an edit and
// does not write to the carrier it no longer has.
func TestAnEditWhileThereIsNoCarrierIsNotSent(t *testing.T) {
	c, tp := recClient(t)

	// Control: while up, presence is written.
	if err := c.transmit(kindPresence, presenceMsg{}); err != nil {
		t.Fatal(err)
	}
	if got := tp.count(kindPresence); got != 1 {
		t.Fatalf("a send while up wrote %d presence messages, want 1", got)
	}

	c.markDown(true)
	if err := c.transmit(kindPresence, presenceMsg{}); err != nil {
		t.Fatalf("a send while down = %v, want nil", err)
	}
	if got := tp.count(kindPresence); got != 1 {
		t.Fatalf("a send while down reached the carrier: %d presence messages, want 1", got)
	}
}

// rejoin.go:244 and 255. A carrier whose join cannot be sent, or whose welcome
// cannot be absorbed, is closed and the failure is reported as it was.
func TestAnAttachThatFailsClosesItsCarrier(t *testing.T) {
	c, _ := recClient(t)
	boom := errors.New("boom")

	t.Run("the join cannot be sent", func(t *testing.T) {
		tp := &recTransport{sendErr: boom}
		if _, _, err := c.attach(context.Background(), tp); !errors.Is(err, boom) {
			t.Fatalf("attach = %v, want boom", err)
		}
		if tp.closed != 1 {
			t.Fatalf("the carrier was closed %d times, want 1", tp.closed)
		}
	})
	t.Run("the welcome cannot be read", func(t *testing.T) {
		tp := &recTransport{welcome: welcomeMsg{Snapshot: []byte("junk")}}
		if _, _, err := c.attach(context.Background(), tp); err == nil {
			t.Fatal("attach accepted a snapshot that is not one")
		}
		if tp.closed != 1 {
			t.Fatalf("the carrier was closed %d times, want 1", tp.closed)
		}
	})
	// Control: a welcome that can be read keeps the carrier open.
	t.Run("a welcome that is fine", func(t *testing.T) {
		tp := &recTransport{}
		conn, _, err := c.attach(context.Background(), tp)
		if err != nil {
			t.Fatalf("attach = %v", err)
		}
		if tp.closed != 0 {
			t.Fatal("a successful attach closed its carrier")
		}
		_ = conn.Close()
	})
}
