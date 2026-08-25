//go:build (js && wasm) || !js

package collab

import (
	"context"
	"encoding/binary"
	"errors"
	"sync"
	"time"
)

// A BroadcastChannel is how two tabs of one browser edit a document together
// with nothing between them — no WebRTC, no ICE, no server, and nothing for a
// person to copy from one window to the other. Every tab on the same origin that
// opens a channel of the same name is on one shared bus: a message posted by any
// of them reaches all the others. That is the whole carrier, and it costs a page
// nothing it does not already have.
//
// # A shared bus, not a wire
//
// [WebSocket], [GRPC] and a WebRTC data channel are each a point-to-point link:
// one participant at each end, and every byte is for the other. A
// BroadcastChannel is not. A post goes to everyone, so a message has to say who
// it is from and who it is for, and each tab has to route what it hears. That
// routing is what this file is: an envelope with an address on it, a client that
// speaks only to its host, and a host that keeps one session per tab it hears
// from.
//
// # Which tab is the host
//
// One tab holds the document and answers the others, which is what a [Server]
// does; it is a role in the protocol and not a machine, and no tab is listening
// for anything. The tabs decide the role between themselves when they open, by
// [electRole]: each announces itself, and the one that finds no host already
// serving and no lower-numbered tab still deciding becomes the host. The rule is
// deterministic — the lowest identifier among tabs opening together wins — so two
// tabs racing to open never both host. A tab that opens later finds the host
// already answering and joins it. See [electRole] for the exact rule and the one
// timing assumption it rests on.
//
// # The split, and why
//
// Everything here is the routing and the handshake over an abstract [bus], with
// no reference to the browser at all, so it is exercised natively against an
// in-memory bus rather than only in a headless browser. bcast_js.go is the thin
// adapter that makes a real BroadcastChannel look like a [bus]; it holds no logic
// of its own. This mirrors pipe.go, whose in-process carrier is testable for the
// same reason.

// ErrBroadcastClosed is why a BroadcastChannel carrier's [Recv] or [Send]
// returned: this end was closed, or the bus it spoke over was. It is the
// shared-bus counterpart of the error a dropped socket reports.
var ErrBroadcastClosed = errors.New("collab: broadcast channel closed")

// A bus is a shared channel every endpoint on one origin and name is joined to.
// A post reaches every other endpoint on the same bus, never the sender itself,
// which is exactly what a BroadcastChannel does — see bcast_js.go, and the
// in-memory bus the tests drive this with.
type bus interface {
	// post broadcasts one frame to every other endpoint on the bus.
	post(frame []byte)
	// onFrame registers the callback each received frame is handed to. It
	// replaces any previous one, and the callback must not block: it runs on the
	// page's single thread, where blocking is not a thing a page may do.
	onFrame(func([]byte))
	// close leaves the bus. After it, nothing further is delivered here.
	close()
}

// The control kinds a frame carries. A data frame's payload is the wire framing
// from wire.go; the rest carry the handshake and have no payload.
const (
	// ctlHello announces a tab: broadcast on opening and on dialling, so a host
	// knows to answer it. from is the sender; to is zero.
	ctlHello byte = 1
	// ctlWelcome is a host's answer to a hello: from is the host, to is the tab
	// it is answering. It establishes which tab to speak to, nothing more — the
	// session's own join and welcome travel inside data frames.
	ctlWelcome byte = 2
	// ctlData carries one session message: from the sender, to the one endpoint
	// it is for, payload the bytes wire.go framed.
	ctlData byte = 3
	// ctlBye announces a tab leaving. It is broadcast so its host drops the
	// session and its participants stop waiting on it.
	ctlBye byte = 4
	// ctlHost is a host's beacon: from is the host, to is zero. A host broadcasts
	// it the instant it is elected and then periodically for as long as it holds
	// the room. It is what makes the election race-free at any timing: a tab that
	// opens while a host is still wiring its [Server] — the window that once let a
	// second host be elected — hears the beacon and joins instead. And if two tabs
	// ever both believe they host, the beacon settles it deterministically: the one
	// with the higher identifier hears the lower's beacon and steps down.
	ctlHost byte = 5
)

// helloRetry is how often a dialling tab repeats its hello until its host
// answers. A host that is a hair slow to start serving — two tabs opening
// together, the host-elect a moment behind — would otherwise leave the dialler
// waiting on a hello nobody was yet listening for. It is small because a post on
// a same-origin bus is delivered in well under a millisecond.
const helloRetry = 30 * time.Millisecond

// hostBeacon is how often a host re-announces itself with a [ctlHost] frame. It
// is short next to the election window so a tab electing during a former
// serve-gap always hears a beacon within its window and joins rather than
// electing a second host, and so two hosts that somehow coexist settle which one
// steps down within a couple of ticks. Like [helloRetry], it is cheap because a
// post on a same-origin bus is delivered in well under a millisecond.
const hostBeacon = 40 * time.Millisecond

// ErrHostSuperseded is why a host's serve loop returned: another tab with a lower
// identifier — the one the tie-break gives priority — announced itself as host,
// so this tab steps down to let that one hold the room. It is the self-healing
// half of the election: even if two tabs ever both reach [RoleHost], exactly one
// keeps the document and the other yields, rather than the two drifting apart as
// separate documents. A caller that hosts should, on this error, re-join the room
// (it will now find the surviving host answering) rather than treat it as a
// failure.
var ErrHostSuperseded = errors.New("collab: another tab is hosting this room")

// A Role is which side of the protocol a tab took, decided by [electRole].
type Role int

const (
	// RoleClient is a tab that joins a document another tab is holding.
	RoleClient Role = iota + 1
	// RoleHost is the tab holding the document and answering the others.
	RoleHost
)

// An envelope is one frame on the bus: who it is from, who it is for (zero for
// everyone), which control kind it is, and the session bytes it carries, if any.
type envelope struct {
	ctl     byte
	from    uint64
	to      uint64
	payload []byte
}

// encode lays the envelope out as a control byte, the two identifiers as
// varints, and the payload as the remaining bytes — no length prefix, because a
// bus delivers one frame as one message and its end is the message's end.
func (e envelope) encode() []byte {
	out := make([]byte, 0, 1+2*binary.MaxVarintLen64+len(e.payload))
	out = append(out, e.ctl)
	out = binary.AppendUvarint(out, e.from)
	out = binary.AppendUvarint(out, e.to)
	return append(out, e.payload...)
}

// decodeEnvelope reads one frame, refusing rather than trusting a truncated
// header. The payload is copied, since the buffer a bus hands over may be reused.
func decodeEnvelope(raw []byte) (envelope, bool) {
	if len(raw) == 0 {
		return envelope{}, false
	}
	ctl, rest := raw[0], raw[1:]
	from, n := binary.Uvarint(rest)
	if n <= 0 {
		return envelope{}, false
	}
	rest = rest[n:]
	to, m := binary.Uvarint(rest)
	if m <= 0 {
		return envelope{}, false
	}
	rest = rest[m:]
	var payload []byte
	if len(rest) > 0 {
		payload = append([]byte(nil), rest...)
	}
	return envelope{ctl: ctl, from: from, to: to, payload: payload}, true
}

// An inbox is a queue of received envelopes with a single reader, filled from a
// bus callback that must never block. It is the same shape as the browser
// WebSocket's queue in websocket_js.go, and for the same reason: a callback runs
// on the page's thread, so it hands the message over and returns rather than
// waiting for a reader.
type inbox struct {
	mu     sync.Mutex
	queue  []envelope
	err    error
	woken  chan struct{}
	closed chan struct{}
	once   sync.Once
}

func newInbox() *inbox {
	return &inbox{woken: make(chan struct{}, 1), closed: make(chan struct{})}
}

// push queues a received envelope and nudges a waiting reader.
func (q *inbox) push(e envelope) {
	q.mu.Lock()
	q.queue = append(q.queue, e)
	q.mu.Unlock()
	q.wake()
}

// fail records why the inbox is over — the first reason wins — and wakes the
// reader. What is already queued is still delivered before the reason is
// reported, so a message that arrived the instant before a close is not lost.
func (q *inbox) fail(err error) {
	q.mu.Lock()
	if q.err == nil {
		q.err = err
	}
	q.mu.Unlock()
	q.once.Do(func() { close(q.closed) })
	q.wake()
}

// wake nudges a waiting reader without ever blocking the caller.
func (q *inbox) wake() {
	select {
	case q.woken <- struct{}{}:
	default:
	}
}

// reason reports why the inbox is over, or nil while it is open.
func (q *inbox) reason() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.err
}

// recv hands over the next envelope, waiting for one, and returns why the inbox
// is over once there is nothing queued and it has failed, or once ctx ends.
func (q *inbox) recv(ctx context.Context) (envelope, error) {
	for {
		q.mu.Lock()
		if len(q.queue) > 0 {
			e := q.queue[0]
			q.queue = q.queue[1:]
			q.mu.Unlock()
			return e, nil
		}
		err := q.err
		q.mu.Unlock()
		if err != nil {
			return envelope{}, err
		}
		select {
		case <-q.woken:
		case <-q.closed:
		case <-ctx.Done():
			return envelope{}, ctx.Err()
		}
	}
}

// A busConn is one endpoint on a bus: the received frames all land in one inbox,
// and posting stamps this endpoint's identity on the frame.
type busConn struct {
	b      bus
	selfID uint64
	in     *inbox
}

// attach joins the bus and starts routing every well-formed frame into one
// inbox. A frame that does not decode is dropped, the way a corrupt datagram is:
// a shared bus is a place a stray post can land, and one is not worth ending a
// session over.
func attach(b bus, selfID uint64) *busConn {
	bc := &busConn{b: b, selfID: selfID, in: newInbox()}
	b.onFrame(func(raw []byte) {
		if e, ok := decodeEnvelope(raw); ok {
			bc.in.push(e)
		}
	})
	return bc
}

// post broadcasts one frame from this endpoint.
func (bc *busConn) post(ctl byte, to uint64, payload []byte) {
	bc.b.post(envelope{ctl: ctl, from: bc.selfID, to: to, payload: payload}.encode())
}

// electRole decides whether this tab hosts the document or joins one already
// being held, and does it so that tabs opening at any timing never both host.
//
// Each tab broadcasts a hello and then watches the bus for the length of the
// election window, re-broadcasting the hello each [helloRetry] so a tab that
// opened after this one's first hello — its bus not yet existing then — still
// hears this tab and defers to the lower identifier. (A single hello was how two
// tabs could both reach [RoleHost]: the later one never heard the earlier's one
// hello, and the earlier was not yet answering.) What it hears decides it:
//
//   - A host beacon ([ctlHost]) from any tab means a host holds the room — even
//     one still wiring its [Server] and not yet welcoming: this tab joins.
//   - A welcome addressed to it means a host answered its hello: this tab joins.
//   - A hello from a lower identifier means a tab with priority is still
//     deciding and will host: this tab joins.
//   - None of these, before the window elapses, means there is no host and no tab
//     with priority: this tab hosts.
//
// Because the tie is broken by the lowest identifier and every tab re-announces
// itself through the window, exactly one tab among any set that overlaps hosts,
// and every other joins it. A tab opening outside the window finds the host's
// beacon or a welcome at once. ctx ending before the window returns its error.
func electRole(ctx context.Context, bc *busConn, window time.Duration) (Role, error) {
	ectx, cancel := context.WithTimeout(ctx, window)
	defer cancel()
	stop := reannounce(ectx, bc, ctlHello)
	defer stop()
	for {
		e, err := bc.in.recv(ectx)
		if err != nil {
			if ctx.Err() != nil {
				return 0, ctx.Err()
			}
			return RoleHost, nil
		}
		switch {
		case e.ctl == ctlHost:
			return RoleClient, nil
		case e.ctl == ctlWelcome && e.to == bc.selfID:
			return RoleClient, nil
		case e.ctl == ctlHello && e.from < bc.selfID:
			return RoleClient, nil
		}
	}
}

// reannounce broadcasts ctl once now and again every helloRetry until ctx ends or
// the returned stop is called, so a tab keeps announcing itself for as long as it
// is deciding or holding the room. It is how an election hello reaches a tab that
// opened late and how a host's beacon keeps reaching the room.
func reannounce(ctx context.Context, bc *busConn, ctl byte) (stop func()) {
	bc.post(ctl, 0, nil)
	done := make(chan struct{})
	go func() {
		t := time.NewTicker(helloRetry)
		if ctl == ctlHost {
			t.Reset(hostBeacon)
		}
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-t.C:
				bc.post(ctl, 0, nil)
			}
		}
	}()
	var once sync.Once
	return func() { once.Do(func() { close(done) }) }
}

// dialBus joins the document a host on the bus is holding. It broadcasts a hello,
// repeats it until the host answers with a welcome, and hands back the carrier
// the joined session speaks over. It returns when ctx ends without an answer.
func dialBus(ctx context.Context, bc *busConn) (carrierConn, error) {
	bc.post(ctlHello, 0, nil)
	stop := make(chan struct{})
	go func() {
		t := time.NewTicker(helloRetry)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				bc.post(ctlHello, 0, nil)
			case <-stop:
				return
			}
		}
	}()
	defer close(stop)

	for {
		e, err := bc.in.recv(ctx)
		if err != nil {
			return nil, err
		}
		if e.ctl == ctlWelcome && e.to == bc.selfID {
			return &bcastClient{bc: bc, hostID: e.from, ctx: ctx}, nil
		}
	}
}

// A bcastClient is a joined tab's carrier: it speaks only to its host, and hears
// only what the host addresses to it, filtering the rest of the shared bus out.
type bcastClient struct {
	bc     *busConn
	hostID uint64
	ctx    context.Context
	once   sync.Once
}

// Send frames one client message and posts it to the host.
func (c *bcastClient) Send(kind byte, msg any) error {
	payload, err := encodeClient(kind, msg)
	if err != nil {
		return err
	}
	if err := c.bc.in.reason(); err != nil {
		return err
	}
	c.bc.post(ctlData, c.hostID, payload)
	return nil
}

// Recv hands over the next message the host sent this tab, skipping the traffic
// on the bus that is for other tabs, and reports the host leaving as a close.
func (c *bcastClient) Recv() (byte, any, error) {
	for {
		e, err := c.bc.in.recv(c.ctx)
		if err != nil {
			return 0, nil, err
		}
		switch {
		case e.ctl == ctlData && e.from == c.hostID && e.to == c.bc.selfID:
			return decodeServer(e.payload)
		case e.ctl == ctlBye && e.from == c.hostID:
			return 0, nil, ErrClosed
		}
	}
}

// Close tells the host this tab is leaving, ends its own reads, and drops off the
// bus. It is idempotent.
func (c *bcastClient) Close() error {
	c.once.Do(func() {
		c.bc.post(ctlBye, c.hostID, nil)
		c.bc.in.fail(ErrBroadcastClosed)
		c.bc.b.close()
	})
	return nil
}

// serveBus is the host over a bus whose Server is known up front: it starts a
// host answerer, attaches the server at once, and blocks until the room is torn
// down, closing every session it held. It is the direct-role entry
// ([Server.ServeBroadcastChannel]) writes in terms of the same [bcastHost] the
// zero-config path uses, so both share one answering loop.
func serveBus(ctx context.Context, s *Server, bc *busConn) error {
	h := newBcastHost(ctx, bc)
	h.attachServer(s)
	defer h.close()
	return h.wait()
}

// A bcastHost is the host side of a room: it hears every tab on the bus, answers
// each hello with a welcome, beacons its presence, and runs one session per tab.
//
// It answers from the instant it is created — before its [Server] is attached —
// which is what closes the serve-gap. A tab that opens while the host is still
// wiring its Server is welcomed immediately and its session buffered, so it never
// mistakes the wiring for an empty room and elects a second host; its buffered
// messages replay into a real session the moment [bcastHost.attachServer] runs.
type bcastHost struct {
	bc   *busConn
	done chan error

	mu   sync.Mutex
	s    *Server              // nil until attachServer wires it; sessions buffer until then
	ends map[uint64]*bcastEnd // one per tab heard from
}

// newBcastHost starts a host answerer on bc under ctx: it welcomes hellos, routes
// data and beacons at once, buffering each tab's session until a Server is
// attached. Its loop returns — reported through [bcastHost.wait] — when ctx ends
// or a lower-identifier host beacon supersedes it.
func newBcastHost(ctx context.Context, bc *busConn) *bcastHost {
	h := &bcastHost{bc: bc, done: make(chan error, 1), ends: map[uint64]*bcastEnd{}}
	go func() { h.done <- h.run(ctx) }()
	return h
}

// run reads the bus, answering hellos, routing data and stepping down on a
// rightful host's beacon, until ctx ends. It beacons this host's presence
// throughout.
func (h *bcastHost) run(ctx context.Context) error {
	stop := reannounce(ctx, h.bc, ctlHost)
	defer stop()
	for {
		e, err := h.bc.in.recv(ctx)
		if err != nil {
			return err
		}
		switch {
		case e.ctl == ctlHost && e.from < h.bc.selfID:
			// A host with priority exists: step down so the room keeps one document.
			return ErrHostSuperseded
		case e.ctl == ctlHello:
			h.onHello(ctx, e.from)
		case e.ctl == ctlData && e.to == h.bc.selfID:
			h.onData(e.from, e.payload)
		case e.ctl == ctlBye:
			h.onBye(e.from)
		}
	}
}

// onHello welcomes a tab. A hello from a tab not yet known opens its session (or
// buffers it until a Server is attached); one from a tab already known is a
// repeat from a dialler that has not heard the welcome yet, answered again
// without a second session.
func (h *bcastHost) onHello(ctx context.Context, from uint64) {
	h.mu.Lock()
	if _, ok := h.ends[from]; !ok {
		end := &bcastEnd{ctx: ctx, bc: h.bc, clientID: from, in: newInbox()}
		h.ends[from] = end
		if h.s != nil {
			h.startSession(h.s, end)
		}
	}
	h.mu.Unlock()
	h.bc.post(ctlWelcome, from, nil)
}

// onData routes a tab's message into its session, dropping data from a tab it
// does not know.
func (h *bcastHost) onData(from uint64, payload []byte) {
	h.mu.Lock()
	end, ok := h.ends[from]
	h.mu.Unlock()
	if ok {
		end.in.push(envelope{ctl: ctlData, payload: payload})
	}
}

// onBye ends a tab's session, ignoring a bye for one never seen.
func (h *bcastHost) onBye(from uint64) {
	h.mu.Lock()
	end, ok := h.ends[from]
	if ok {
		delete(h.ends, from)
	}
	h.mu.Unlock()
	if ok {
		_ = end.Close()
	}
}

// startSession runs one tab's session against the server. The caller holds h.mu.
func (h *bcastHost) startSession(s *Server, end *bcastEnd) {
	go func() {
		_ = s.session(end)
		_ = end.Close()
	}()
}

// attachServer wires the Server in and starts a session for every tab already
// welcomed and buffered while there was none. After it, a new tab's session
// starts the moment its hello is heard.
func (h *bcastHost) attachServer(s *Server) {
	h.mu.Lock()
	h.s = s
	for _, end := range h.ends {
		h.startSession(s, end)
	}
	h.mu.Unlock()
}

// wait blocks until the answer loop returns and reports why — ctx ended, or a
// rightful host superseded this one ([ErrHostSuperseded]).
func (h *bcastHost) wait() error { return <-h.done }

// close ends the loop, if it has not already, and closes every session it held.
// It is idempotent, and the reason it hands the loop makes the reader on each
// session end return so its goroutine stops.
func (h *bcastHost) close() {
	h.bc.in.fail(ErrBroadcastClosed)
	h.mu.Lock()
	ends := h.ends
	h.ends = map[uint64]*bcastEnd{}
	h.mu.Unlock()
	for _, e := range ends {
		_ = e.Close()
	}
}

// hostOrJoinBus runs the same-browser election on bc and hands back a live
// endpoint, closing the serve-gap the old two-call shape ([HostOrJoin] then a
// fresh bus for serving) left open. The bus and identity that elected are the
// ones that go on to serve or dial — nothing is reopened — so an elected host
// answers hellos from the instant it wins, and a client dials on the same
// carrier it elected over.
//
// It returns exactly one of a running [bcastHost] (this tab is host, already
// answering — attach a Server with [bcastHost.attachServer] and block on
// [bcastHost.wait]) or a dialled [carrierConn] (this tab joined — hand it to a
// session). ctx ending during the election or the dial returns its error.
func hostOrJoinBus(ctx context.Context, bc *busConn, window time.Duration) (Role, *bcastHost, carrierConn, error) {
	role, err := electRole(ctx, bc, window)
	if err != nil {
		return 0, nil, nil, err
	}
	if role == RoleHost {
		return RoleHost, newBcastHost(ctx, bc), nil, nil
	}
	conn, err := dialBus(ctx, bc)
	if err != nil {
		return 0, nil, nil, err
	}
	return RoleClient, nil, conn, nil
}

// An openedCarrier is a [Transport] over a carrier that is already open — the one
// [hostOrJoinBus] dialled for a client — so a joined tab reuses that connection
// rather than dialling a second time.
type openedCarrier struct{ c carrierConn }

func (t *openedCarrier) open(context.Context) (carrierConn, error) { return t.c, nil }

// A bcastEnd is the host's side of one tab's session: a carrier like the one a
// gRPC stream or a pipe presents, with its received messages fed in by serveBus
// rather than read from the bus directly, since the host reads the bus once for
// everyone.
type bcastEnd struct {
	ctx      context.Context
	bc       *busConn
	clientID uint64
	in       *inbox
	once     sync.Once
}

func (e *bcastEnd) Context() context.Context { return e.ctx }

// Send frames one server message and posts it to this tab.
func (e *bcastEnd) Send(kind byte, msg any) error {
	payload, err := encodeServer(kind, msg)
	if err != nil {
		return err
	}
	if err := e.in.reason(); err != nil {
		return err
	}
	e.bc.post(ctlData, e.clientID, payload)
	return nil
}

// Recv hands over the next message this tab sent, as serveBus routed it here.
func (e *bcastEnd) Recv() (byte, any, error) {
	ev, err := e.in.recv(e.ctx)
	if err != nil {
		return 0, nil, err
	}
	return decodeClient(ev.payload)
}

// Close ends this tab's session. It is idempotent, and closing it is what makes
// its Recv return so the session goroutine holding it stops.
func (e *bcastEnd) Close() error {
	e.once.Do(func() { e.in.fail(ErrBroadcastClosed) })
	return nil
}
