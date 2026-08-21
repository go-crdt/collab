//go:build (js && wasm) || !js

package collab

import (
	"context"
	"errors"
	"sync"
)

// Pipe returns the two ends of an in-process session: a [Transport] to hand to
// [Join], and a handle to hand to [Server.ServePipe]. Nothing leaves the
// process — messages are carried on Go channels rather than a socket — so it is
// how a peer that both serves a document and edits it locally binds its own
// editor to that document without opening a loopback connection to itself.
//
// # What it is for
//
// A browser tab holding a document for others over a data channel is also a
// place someone is typing. Its own editor is a participant like any other, and
// the honest way to say so is to [Join] the document it is serving. Without this
// that means a second, real carrier looping back to the same page — a WebRTC
// data channel to itself, dialled, answered and encrypted, to carry bytes that
// never cross a wire. Pipe is that participant with the carrier taken out: the
// same session, the same merge, over two channels and nothing else.
//
// The same holds for a native server that wants a replica of a document it
// hosts — a headless editor, a linter, an exporter — without the cost of a
// second server or the round trip of a real link.
//
// # Shape
//
// The two ends are one session and share its fate: closing either, or
// cancelling the context [Join] or [Server.ServePipe] was given, ends both. A
// [Recv] blocked when that happens returns [ErrPipeClosed], and a [Send] after
// it does the same, so neither end can be left waiting on a peer that has gone.
//
// A participant here edits the same document as one arriving over [WebSocket] or
// [GRPC]; it is a first-class participant, not a shortcut with less to it.
func Pipe() (Transport, *PipeConn) {
	w := &pipeWires{
		c2s:        make(chan wireMsg, pipeBuffer),
		s2c:        make(chan wireMsg, pipeBuffer),
		clientDone: make(chan struct{}),
		serverDone: make(chan struct{}),
	}
	return &pipeTransport{w: w}, &PipeConn{w: w}
}

// ErrPipeClosed is why a [Pipe] carrier's [Recv] or [Send] returned: either end
// was closed, or the session's context was cancelled. It is the in-process
// counterpart of the error a dropped socket reports.
var ErrPipeClosed = errors.New("collab: pipe closed")

// pipeBuffer is how many messages may sit in flight in one direction before a
// send waits for the other end to read. A session's two ends each have a
// dedicated reader, so this is slack rather than a queue anything relies on: it
// keeps a burst — a welcome followed at once by operations — from making the
// sender wait, and it is small because nothing here is meant to accumulate.
const pipeBuffer = 16

// pipeWires is the two ends' shared state: a channel each way, and a signal each
// way for a closed end. Both are held by value where they are one thing that is
// created once and never reassigned; the sync.Once guards each side's single
// close of its signal.
type pipeWires struct {
	c2s, s2c               chan wireMsg
	clientDone, serverDone chan struct{}
	closeClient            sync.Once
	closeServer            sync.Once
}

// pipeTransport is the client end of a [Pipe], the side [Join] opens.
type pipeTransport struct{ w *pipeWires }

func (t *pipeTransport) open(ctx context.Context) (carrierConn, error) {
	return &pipeEnd{
		ctx:       ctx,
		send:      t.w.c2s,
		recv:      t.w.s2c,
		localDone: t.w.clientDone,
		peerDone:  t.w.serverDone,
		closeOnce: &t.w.closeClient,
	}, nil
}

// PipeConn is the server end of a [Pipe], the side [Server.ServePipe] serves. It
// is an opaque handle: the session is spoken through it, not by the caller.
type PipeConn struct{ w *pipeWires }

// ServePipe runs one session over the server end of a [Pipe], with this server
// holding the document. It returns when the session ends — because the client
// end closed, because ctx was cancelled, or because the session itself failed.
//
// It is the in-process counterpart of [Server.ServeWebSocket] and, in a browser,
// [Server.ServeDataChannel]: there is no request to upgrade and no origin to
// check, because there is no boundary to cross. Give it the [PipeConn] that
// [Pipe] returned beside the [Transport] the local editor joined over.
func (s *Server) ServePipe(ctx context.Context, sc *PipeConn) error {
	end := &pipeEnd{
		ctx:       ctx,
		send:      sc.w.s2c,
		recv:      sc.w.c2s,
		localDone: sc.w.serverDone,
		peerDone:  sc.w.clientDone,
		closeOnce: &sc.w.closeServer,
	}
	// Closing this end tells the client the server has gone, the same way a
	// dropped socket would, whatever ended the session.
	defer func() { _ = end.Close() }()
	return s.session(end)
}

// pipeEnd is one end of a [Pipe]. It is both a [carrierConn], for the client
// that joins over it, and a carrier, for the server that serves the other end:
// the three methods a session has ever needed of a transport, plus the Close a
// client calls and the Context a server reads.
//
// send is the direction this end writes; recv is the direction it reads.
// localDone is closed when this end closes, peerDone when the other does. The
// two are separate so that a message already in flight is delivered before this
// end reports the peer gone — an edit sent the instant before a tab closes is
// not lost to the close racing past it.
type pipeEnd struct {
	ctx       context.Context
	send      chan wireMsg
	recv      chan wireMsg
	localDone chan struct{}
	peerDone  chan struct{}
	closeOnce *sync.Once
}

// Send writes one message, or reports why it could not: this end is closed
// ([ErrPipeClosed]), the other end is gone ([ErrPipeClosed]), or the session's
// context ended (the same). The check for this end being closed comes first, so
// that a send after Close is refused rather than left to a race with the buffer
// having room.
func (e *pipeEnd) Send(kind byte, msg any) error {
	select {
	case <-e.localDone:
		return ErrPipeClosed
	default:
	}
	select {
	case e.send <- wireMsg{kind: kind, msg: msg}:
		return nil
	case <-e.peerDone:
		return ErrPipeClosed
	case <-e.ctx.Done():
		return ErrPipeClosed
	}
}

// Recv reads one message, blocking until there is one, and returns
// [ErrPipeClosed] once neither end can send another: this end closed, the other
// closed, or the context ended. A message already waiting is returned before any
// of those, so nothing sent before a close is dropped for the close arriving on
// its heels.
func (e *pipeEnd) Recv() (byte, any, error) {
	select {
	case m := <-e.recv:
		return m.kind, m.msg, nil
	default:
	}
	select {
	case m := <-e.recv:
		return m.kind, m.msg, nil
	case <-e.localDone:
		return 0, nil, ErrPipeClosed
	case <-e.peerDone:
		return 0, nil, ErrPipeClosed
	case <-e.ctx.Done():
		return 0, nil, ErrPipeClosed
	}
}

// Close ends this end of the session. It is idempotent, and it never blocks:
// closing the signal is all it does, and the other end learns of it the next
// time it sends or receives.
func (e *pipeEnd) Close() error {
	e.closeOnce.Do(func() { close(e.localDone) })
	return nil
}

// Context is the context this session ends when it is cancelled — the one the
// server was given for it. Only the server side is read through this; the client
// side drives its own session through Close instead.
func (e *pipeEnd) Context() context.Context { return e.ctx }
