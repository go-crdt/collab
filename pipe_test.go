//go:build !js

package collab

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// pipeSettle bounds how long a test waits for a change to cross the process.
// Everything here runs in one process, half of it over Go channels, so anything
// near this bound is a failure rather than a slow machine.
const pipeSettle = 10 * time.Second

// awaitPipe waits for want to hold, watching a client's wake-ups so it does not
// spin, and fails with what it saw instead.
func awaitPipe(t *testing.T, c *Client, what string, want func() bool) {
	t.Helper()
	deadline := time.After(pipeSettle)
	for !want() {
		select {
		case <-c.Changes():
		case <-c.Done():
			if !want() {
				t.Fatalf("session ended before %s: %v", what, c.Err())
			}
		case <-deadline:
			t.Fatalf("timed out waiting for %s", what)
		}
	}
}

func pipeText(t *testing.T, c *Client) string {
	t.Helper()
	h, err := c.Text("body")
	if err != nil {
		t.Fatalf("Text(\"body\"): %v", err)
	}
	return h.String()
}

func pipeInsert(t *testing.T, c *Client, at int, s string) {
	t.Helper()
	h, err := c.Text("body")
	if err != nil {
		t.Fatalf("Text(\"body\"): %v", err)
	}
	if err := h.Insert(at, s); err != nil {
		t.Fatalf("Insert(%d, %q): %v", at, s, err)
	}
}

// A participant that reaches its server over a Pipe is a participant like any
// other: it converges with one that arrives over a real WebSocket, in both
// directions, and its end and the server's both close cleanly when it leaves —
// which is the whole point, since a Pipe exists so a peer serving a document can
// edit it locally without a loopback carrier back to itself.
func TestAPipeParticipantIsFirstClass(t *testing.T) {
	srv := NewServer(Config{})

	// The server end of the Pipe is served in its own goroutine; the error it
	// returns is captured so the test can prove it returned at all.
	serveCtx, cancelServe := context.WithCancel(context.Background())
	defer cancelServe()
	local, server := Pipe()
	served := make(chan error, 1)
	go func() { served <- srv.ServePipe(serveCtx, server) }()

	// The local editor joins over the Pipe.
	here, err := Join(t.Context(), local, ClientConfig{Document: "shared", Site: 1})
	if err != nil {
		t.Fatalf("Join over the pipe: %v", err)
	}

	// A second participant arrives over a real WebSocket, the carrier a browser
	// far away would use — nothing about it knows the first is in-process.
	front := httptest.NewServer(srv.ServeWebSocket("*"))
	defer front.Close()
	away, err := Join(t.Context(), WebSocket("ws"+strings.TrimPrefix(front.URL, "http")),
		ClientConfig{Document: "shared", Site: 2})
	if err != nil {
		t.Fatalf("Join over the websocket: %v", err)
	}
	defer away.Close()

	// The pipe participant's edit reaches the networked one.
	pipeInsert(t, here, 0, "hello")
	awaitPipe(t, away, "the pipe edit to arrive", func() bool { return pipeText(t, away) == "hello" })

	// And the networked one's edit reaches the pipe participant.
	pipeInsert(t, away, 5, " world")
	awaitPipe(t, here, "the websocket edit to arrive", func() bool { return pipeText(t, here) == "hello world" })

	if got := pipeText(t, here); got != "hello world" {
		t.Fatalf("the pipe participant holds %q", got)
	}

	// Leaving closes both ends: the client reports the deliberate close, and the
	// server's ServePipe returns rather than leaking its goroutine.
	if err := here.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := here.Err(); !errors.Is(err, ErrClosed) {
		t.Fatalf("after Close, Err() = %v, want ErrClosed", err)
	}
	select {
	case err := <-served:
		if !errors.Is(err, ErrPipeClosed) {
			t.Fatalf("ServePipe returned %v, want ErrPipeClosed once the client left", err)
		}
	case <-time.After(pipeSettle):
		t.Fatal("ServePipe did not return after the client closed: its goroutine leaked")
	}

	// The document is intact for whoever is still in it.
	awaitPipe(t, away, "the text to survive the pipe leaving", func() bool { return pipeText(t, away) == "hello world" })

	if err := srv.Close(context.Background()); err != nil {
		t.Fatalf("server Close: %v", err)
	}
}

// Cancelling the context ServePipe was given ends the session, so a server that
// wants to stop serving one end has a way to that does not depend on the client.
func TestServePipeStopsWhenItsContextIsCancelled(t *testing.T) {
	srv := NewServer(Config{})
	ctx, cancel := context.WithCancel(context.Background())
	local, server := Pipe()

	served := make(chan error, 1)
	go func() { served <- srv.ServePipe(ctx, server) }()

	here, err := Join(t.Context(), local, ClientConfig{Document: "d", Site: 1})
	if err != nil {
		t.Fatalf("Join: %v", err)
	}
	defer here.Close()

	cancel()
	select {
	case err := <-served:
		if !errors.Is(err, ErrPipeClosed) {
			t.Fatalf("ServePipe returned %v, want ErrPipeClosed on cancellation", err)
		}
	case <-time.After(pipeSettle):
		t.Fatal("ServePipe did not return when its context was cancelled")
	}
}

// A closed pipe channel is one struct{} channel that is already closed; these
// build the ends a session never reaches on its own, so every refusal branch is
// exercised where it can be made to happen on purpose.
func closedSignal() chan struct{} {
	c := make(chan struct{})
	close(c)
	return c
}

func newPipeEnd(ctx context.Context, send chan wireMsg) *pipeEnd {
	return &pipeEnd{
		ctx:       ctx,
		send:      send,
		recv:      make(chan wireMsg),
		localDone: make(chan struct{}),
		peerDone:  make(chan struct{}),
		closeOnce: &sync.Once{},
	}
}

// Send refuses rather than blocks or panics once there is nobody to deliver to:
// this end closed, the other end gone, or the session's context ended.
func TestPipeSendRefusesWhenThereIsNobodyToReach(t *testing.T) {
	t.Run("after this end is closed", func(t *testing.T) {
		e := newPipeEnd(context.Background(), make(chan wireMsg, 1))
		if err := e.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		if err := e.Send(kindOperation, opsMsg{}); !errors.Is(err, ErrPipeClosed) {
			t.Fatalf("Send after Close = %v, want ErrPipeClosed", err)
		}
	})

	t.Run("after the other end is gone", func(t *testing.T) {
		// An unbuffered send with no reader would block for ever; the peer being
		// gone is what turns that into a refusal.
		e := newPipeEnd(context.Background(), make(chan wireMsg))
		e.peerDone = closedSignal()
		if err := e.Send(kindOperation, opsMsg{}); !errors.Is(err, ErrPipeClosed) {
			t.Fatalf("Send to a gone peer = %v, want ErrPipeClosed", err)
		}
	})

	t.Run("after the context is cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		e := newPipeEnd(ctx, make(chan wireMsg))
		if err := e.Send(kindOperation, opsMsg{}); !errors.Is(err, ErrPipeClosed) {
			t.Fatalf("Send on a cancelled context = %v, want ErrPipeClosed", err)
		}
	})
}

// A message that can be delivered is, and arrives unchanged.
func TestPipeSendDeliversToAReader(t *testing.T) {
	send := make(chan wireMsg, 1)
	e := newPipeEnd(context.Background(), send)
	if err := e.Send(kindOperation, opsMsg{Operations: []byte("op")}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	got := <-send
	if got.kind != kindOperation {
		t.Fatalf("kind = %d, want %d", got.kind, kindOperation)
	}
	ops, ok := got.msg.(opsMsg)
	if !ok || string(ops.Operations) != "op" {
		t.Fatalf("msg = %#v, want opsMsg{\"op\"}", got.msg)
	}
}

// Recv hands over a message already waiting before it reports any close, so an
// edit sent the instant before a peer leaves is not lost to the close racing it.
func TestPipeRecvDeliversAWaitingMessageFirst(t *testing.T) {
	recv := make(chan wireMsg, 1)
	recv <- wireMsg{kind: kindPresence, msg: presenceMsg{Update: []byte("cursor")}}
	e := newPipeEnd(context.Background(), make(chan wireMsg))
	e.recv = recv
	// Close both ends' signals: even so, the buffered message comes out first.
	_ = e.Close()
	e.peerDone = closedSignal()

	kind, msg, err := e.Recv()
	if err != nil {
		t.Fatalf("Recv of a waiting message = %v", err)
	}
	if kind != kindPresence {
		t.Fatalf("kind = %d, want %d", kind, kindPresence)
	}
	if p, ok := msg.(presenceMsg); !ok || string(p.Update) != "cursor" {
		t.Fatalf("msg = %#v, want presenceMsg{\"cursor\"}", msg)
	}
}

// A message that arrives while Recv is already blocked is delivered.
func TestPipeRecvDeliversAMessageThatArrivesWhileBlocked(t *testing.T) {
	recv := make(chan wireMsg)
	e := newPipeEnd(context.Background(), make(chan wireMsg))
	e.recv = recv
	// The pause lets Recv get past the check for a message already waiting and
	// block, so this exercises the message arriving on a Recv that is committed
	// to waiting rather than one that finds it in hand.
	go func() {
		time.Sleep(20 * time.Millisecond)
		recv <- wireMsg{kind: kindOperation, msg: opsMsg{Operations: []byte("late")}}
	}()

	kind, msg, err := e.Recv()
	if err != nil {
		t.Fatalf("Recv = %v", err)
	}
	if kind != kindOperation {
		t.Fatalf("kind = %d, want %d", kind, kindOperation)
	}
	if ops, ok := msg.(opsMsg); !ok || string(ops.Operations) != "late" {
		t.Fatalf("msg = %#v, want opsMsg{\"late\"}", msg)
	}
}

// With nothing waiting, Recv unblocks with the sentinel on every way a session
// can end: this end closed, the other closed, or the context cancelled.
func TestPipeRecvUnblocksWhenTheSessionEnds(t *testing.T) {
	t.Run("this end closed", func(t *testing.T) {
		e := newPipeEnd(context.Background(), make(chan wireMsg))
		_ = e.Close()
		if _, _, err := e.Recv(); !errors.Is(err, ErrPipeClosed) {
			t.Fatalf("Recv after Close = %v, want ErrPipeClosed", err)
		}
	})

	t.Run("the other end closed", func(t *testing.T) {
		e := newPipeEnd(context.Background(), make(chan wireMsg))
		e.peerDone = closedSignal()
		if _, _, err := e.Recv(); !errors.Is(err, ErrPipeClosed) {
			t.Fatalf("Recv after the peer left = %v, want ErrPipeClosed", err)
		}
	})

	t.Run("the context cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		e := newPipeEnd(ctx, make(chan wireMsg))
		if _, _, err := e.Recv(); !errors.Is(err, ErrPipeClosed) {
			t.Fatalf("Recv on a cancelled context = %v, want ErrPipeClosed", err)
		}
	})
}

// Closing an end twice is harmless, and Context hands back what the session was
// given — the two small guarantees the surrounding machinery relies on.
func TestPipeEndCloseIsIdempotentAndContextIsWhatItWasGiven(t *testing.T) {
	ctx := context.WithValue(context.Background(), pipeCtxKey{}, "marker")
	e := newPipeEnd(ctx, make(chan wireMsg))
	if e.Context() != ctx {
		t.Fatal("Context did not return the context the end was built with")
	}
	if err := e.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := e.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

type pipeCtxKey struct{}
