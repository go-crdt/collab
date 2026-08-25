//go:build !js

package collab

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// bcastSettle bounds how long a test waits for a change to cross the in-memory
// bus. Everything here is one process over channels, so anything near this bound
// is a failure rather than a slow machine.
const bcastSettle = 10 * time.Second

// A memBus is the in-memory shared bus the tests drive the BroadcastChannel
// logic with. Every endpoint that joins one hub is on one channel: a post
// reaches every other endpoint on the hub and never the sender, which is exactly
// what a real BroadcastChannel does. Delivery is synchronous because the
// receiving end only enqueues, so a post never blocks on a reader.
type memHub struct {
	mu      sync.Mutex
	members []*memBus
}

func (h *memHub) join() *memBus {
	b := &memBus{hub: h}
	h.mu.Lock()
	h.members = append(h.members, b)
	h.mu.Unlock()
	return b
}

type memBus struct {
	hub    *memHub
	mu     sync.Mutex
	cb     func([]byte)
	closed bool
}

func (b *memBus) post(frame []byte) {
	cp := append([]byte(nil), frame...)
	b.hub.mu.Lock()
	members := append([]*memBus(nil), b.hub.members...)
	b.hub.mu.Unlock()
	for _, m := range members {
		if m == b {
			continue
		}
		m.mu.Lock()
		cb, closed := m.cb, m.closed
		m.mu.Unlock()
		if cb != nil && !closed {
			cb(cp)
		}
	}
}

func (b *memBus) onFrame(fn func([]byte)) {
	b.mu.Lock()
	b.cb = fn
	b.mu.Unlock()
}

func (b *memBus) close() {
	b.mu.Lock()
	b.closed = true
	b.cb = nil
	b.mu.Unlock()
	b.hub.mu.Lock()
	for i, m := range b.hub.members {
		if m == b {
			b.hub.members = append(b.hub.members[:i], b.hub.members[i+1:]...)
			break
		}
	}
	b.hub.mu.Unlock()
}

// connTransport hands a session an already-open carrier, the way the js
// transport hands [Join] the connection dialBus opened.
type connTransport struct{ c carrierConn }

func (t *connTransport) open(context.Context) (carrierConn, error) { return t.c, nil }

func awaitBcast(t *testing.T, c *Client, what string, want func() bool) {
	t.Helper()
	deadline := time.After(bcastSettle)
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

func bcastText(t *testing.T, c *Client) string {
	t.Helper()
	h, err := c.Text("body")
	if err != nil {
		t.Fatalf("Text(\"body\"): %v", err)
	}
	return h.String()
}

func bcastInsert(t *testing.T, c *Client, at int, s string) {
	t.Helper()
	h, err := c.Text("body")
	if err != nil {
		t.Fatalf("Text(\"body\"): %v", err)
	}
	if err := h.Insert(at, s); err != nil {
		t.Fatalf("Insert(%d, %q): %v", at, s, err)
	}
}

// dial joins the host over the hub, returning the client's Client.
func dialClient(t *testing.T, ctx context.Context, hub *memHub, id uint64, cfg ClientConfig) *Client {
	t.Helper()
	conn, err := dialBus(ctx, attach(hub.join(), id))
	if err != nil {
		t.Fatalf("dialBus(%d): %v", id, err)
	}
	c, err := Join(ctx, &connTransport{c: conn}, cfg)
	if err != nil {
		t.Fatalf("Join(%d): %v", id, err)
	}
	return c
}

// The envelope survives a round trip through its own encoding, and a truncated
// one is refused rather than half-read.
func TestBcastEnvelopeRoundTripAndRefusal(t *testing.T) {
	want := envelope{ctl: ctlData, from: 7, to: 9, payload: []byte("ops")}
	got, ok := decodeEnvelope(want.encode())
	if !ok {
		t.Fatal("a well-formed envelope did not decode")
	}
	if got.ctl != want.ctl || got.from != want.from || got.to != want.to || string(got.payload) != "ops" {
		t.Fatalf("decoded %#v, want %#v", got, want)
	}

	// A hello carries no payload; its decode yields a nil one, not an empty
	// slice standing in for a message.
	hello := envelope{ctl: ctlHello, from: 3}
	if e, ok := decodeEnvelope(hello.encode()); !ok || e.payload != nil {
		t.Fatalf("empty payload decoded to %#v (ok=%v)", e.payload, ok)
	}

	for _, bad := range [][]byte{
		nil,                            // empty
		{ctlHello},                     // no from
		append([]byte{ctlHello}, 0x80), // from is a truncated varint
	} {
		if _, ok := decodeEnvelope(bad); ok {
			t.Fatalf("a malformed frame %v decoded", bad)
		}
	}

	// The to identifier being a truncated varint is refused too: a valid ctl and
	// from, then a byte that opens a varint and never closes it.
	trunc := []byte{ctlHello}
	trunc = append(trunc, encodeUvarintForTest(5)...)
	trunc = append(trunc, 0x80)
	if _, ok := decodeEnvelope(trunc); ok {
		t.Fatal("a frame with a truncated to identifier decoded")
	}
}

func encodeUvarintForTest(v uint64) []byte {
	var buf [10]byte
	n := 0
	for v >= 0x80 {
		buf[n] = byte(v) | 0x80
		v >>= 7
		n++
	}
	buf[n] = byte(v)
	return buf[:n+1]
}

// The inbox delivers in order, drains what is queued before reporting it is
// over, and unblocks on a cancelled context.
func TestBcastInbox(t *testing.T) {
	q := newInbox()
	q.push(envelope{ctl: ctlData, from: 1})
	q.push(envelope{ctl: ctlData, from: 2})
	// The first reason to fail wins, and a message already queued still comes
	// out before it.
	q.fail(errors.New("first"))
	q.fail(errors.New("second"))

	for _, want := range []uint64{1, 2} {
		e, err := q.recv(context.Background())
		if err != nil {
			t.Fatalf("recv of queued %d: %v", want, err)
		}
		if e.from != want {
			t.Fatalf("recv from = %d, want %d", e.from, want)
		}
	}
	if _, err := q.recv(context.Background()); err == nil || err.Error() != "first" {
		t.Fatalf("after draining, recv = %v, want the first failure", err)
	}
	if err := q.reason(); err == nil || err.Error() != "first" {
		t.Fatalf("reason = %v, want the first failure", err)
	}

	// A recv that finds nothing queued and no failure waits, then delivers what
	// arrives while it waits.
	q2 := newInbox()
	go func() {
		time.Sleep(20 * time.Millisecond)
		q2.push(envelope{ctl: ctlData, from: 9})
	}()
	if e, err := q2.recv(context.Background()); err != nil || e.from != 9 {
		t.Fatalf("recv of a late message = (%#v, %v)", e, err)
	}

	// A recv with nothing to deliver unblocks when its context is cancelled.
	q3 := newInbox()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := q3.recv(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("recv on a cancelled context = %v, want context.Canceled", err)
	}

	// wake never blocks even when its buffer is already full.
	q4 := newInbox()
	q4.wake()
	q4.wake()
}

// A dropped frame on the bus is not worth ending a session over: attach routes
// what decodes and lets the rest fall.
func TestBcastAttachDropsAMalformedFrame(t *testing.T) {
	hub := &memHub{}
	a := attach(hub.join(), 1)
	other := hub.join()

	other.post([]byte{}) // decodes to nothing
	other.post(envelope{ctl: ctlHello, from: 2}.encode())

	e, err := a.in.recv(context.Background())
	if err != nil {
		t.Fatalf("recv after a dropped frame: %v", err)
	}
	if e.ctl != ctlHello || e.from != 2 {
		t.Fatalf("recv = %#v, want the hello that followed the dropped frame", e)
	}
}

// electRole settles the host between tabs opening together, and finds an
// existing host for a tab opening later.
func TestBcastElection(t *testing.T) {
	t.Run("two tabs racing pick one host by the lower id", func(t *testing.T) {
		hub := &memHub{}
		low := attach(hub.join(), 10)
		high := attach(hub.join(), 20)

		ctx := context.Background()
		var lowRole, highRole Role
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); lowRole, _ = electRole(ctx, low, bcastSettle) }()
		go func() { defer wg.Done(); highRole, _ = electRole(ctx, high, bcastSettle) }()
		wg.Wait()

		if lowRole != RoleHost {
			t.Fatalf("the lower id took %v, want RoleHost", lowRole)
		}
		if highRole != RoleClient {
			t.Fatalf("the higher id took %v, want RoleClient", highRole)
		}
	})

	t.Run("a tab opening alone hosts", func(t *testing.T) {
		hub := &memHub{}
		role, err := electRole(context.Background(), attach(hub.join(), 1), 50*time.Millisecond)
		if err != nil {
			t.Fatalf("electRole: %v", err)
		}
		if role != RoleHost {
			t.Fatalf("a lone tab took %v, want RoleHost", role)
		}
	})

	t.Run("a tab finding a host already answering joins", func(t *testing.T) {
		hub := &memHub{}
		joiner := attach(hub.join(), 5)
		// A host already serving answers the joiner's hello with a welcome.
		host := hub.join()
		host.onFrame(func(raw []byte) {
			if e, ok := decodeEnvelope(raw); ok && e.ctl == ctlHello {
				host.post(envelope{ctl: ctlWelcome, from: 99, to: e.from}.encode())
			}
		})
		role, err := electRole(context.Background(), joiner, bcastSettle)
		if err != nil {
			t.Fatalf("electRole: %v", err)
		}
		if role != RoleClient {
			t.Fatalf("a tab that was welcomed took %v, want RoleClient", role)
		}
	})

	t.Run("a cancelled context ends the election", func(t *testing.T) {
		hub := &memHub{}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := electRole(ctx, attach(hub.join(), 1), bcastSettle); !errors.Is(err, context.Canceled) {
			t.Fatalf("electRole on a cancelled context = %v, want context.Canceled", err)
		}
	})

	t.Run("a higher hello is not mistaken for a host", func(t *testing.T) {
		// A hello from a higher id arriving mid-window must not make this tab
		// join; it hosts once the window elapses.
		hub := &memHub{}
		self := attach(hub.join(), 10)
		other := hub.join()
		other.post(envelope{ctl: ctlHello, from: 20}.encode())
		role, err := electRole(context.Background(), self, 50*time.Millisecond)
		if err != nil {
			t.Fatalf("electRole: %v", err)
		}
		if role != RoleHost {
			t.Fatalf("a tab that saw only a higher hello took %v, want RoleHost", role)
		}
	})
}

// dialBus repeats its hello until a host answers, so a host a moment slow to
// serve does not strand the dialler.
func TestBcastDialRepeatsUntilWelcomed(t *testing.T) {
	hub := &memHub{}
	dialer := attach(hub.join(), 7)

	host := hub.join()
	var mu sync.Mutex
	hellos := 0
	host.onFrame(func(raw []byte) {
		e, ok := decodeEnvelope(raw)
		if !ok || e.ctl != ctlHello {
			return
		}
		mu.Lock()
		hellos++
		n := hellos
		mu.Unlock()
		// Answer only the second hello, which the retry has to produce.
		if n >= 2 {
			host.post(envelope{ctl: ctlWelcome, from: 42, to: e.from}.encode())
		}
	})

	conn, err := dialBus(context.Background(), dialer)
	if err != nil {
		t.Fatalf("dialBus: %v", err)
	}
	if c, ok := conn.(*bcastClient); !ok || c.hostID != 42 {
		t.Fatalf("dialled host = %#v, want hostID 42", conn)
	}
	mu.Lock()
	defer mu.Unlock()
	if hellos < 2 {
		t.Fatalf("host heard %d hellos, want the retry to have sent at least 2", hellos)
	}
}

// dialBus that is never answered returns when its context ends, and a stray
// frame while dialling is ignored rather than taken for a welcome.
func TestBcastDialGivesUpAndIgnoresStrayFrames(t *testing.T) {
	hub := &memHub{}
	dialer := attach(hub.join(), 7)
	other := hub.join()
	// A welcome addressed to a different tab is not this tab's welcome.
	other.post(envelope{ctl: ctlWelcome, from: 1, to: 999}.encode())

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	if _, err := dialBus(ctx, dialer); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("dialBus with no host = %v, want DeadlineExceeded", err)
	}
}

// The client carrier speaks only to its host and refuses once closed.
func TestBcastClientCarrier(t *testing.T) {
	hub := &memHub{}
	self := attach(hub.join(), 5)
	c := &bcastClient{bc: self, hostID: 8, ctx: context.Background()}

	peer := hub.join()
	got := make(chan envelope, 4)
	peer.onFrame(func(raw []byte) {
		if e, ok := decodeEnvelope(raw); ok {
			got <- e
		}
	})

	// A bad kind is refused before anything is posted.
	if err := c.Send(0xFF, nil); !errors.Is(err, ErrProtocol) {
		t.Fatalf("Send of a bad kind = %v, want ErrProtocol", err)
	}
	// A good message is posted to the host, framed.
	if err := c.Send(kindOperation, opsMsg{Operations: []byte("x")}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	select {
	case e := <-got:
		if e.ctl != ctlData || e.to != 8 || e.from != 5 {
			t.Fatalf("posted %#v, want data from 5 to 8", e)
		}
	case <-time.After(bcastSettle):
		t.Fatal("the host never heard the message")
	}

	// Traffic for other tabs, and the host answering, are told apart by Recv.
	self.in.push(envelope{ctl: ctlData, from: 999, to: 5, payload: nil}) // not our host
	self.in.push(envelope{ctl: ctlData, from: 8, to: 5, payload: encodeOps(opsMsg{Operations: []byte("y")})})
	kind, msg, err := c.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if kind != kindOperation || string(msg.(opsMsg).Operations) != "y" {
		t.Fatalf("Recv = (%d, %#v), want the host's operation", kind, msg)
	}

	// The host leaving is a close.
	self.in.push(envelope{ctl: ctlBye, from: 8})
	if _, _, err := c.Recv(); !errors.Is(err, ErrClosed) {
		t.Fatalf("Recv after the host's bye = %v, want ErrClosed", err)
	}

	// Close is idempotent, and a send after it is refused.
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if err := c.Send(kindOperation, opsMsg{}); !errors.Is(err, ErrBroadcastClosed) {
		t.Fatalf("Send after Close = %v, want ErrBroadcastClosed", err)
	}
}

// The host's per-tab carrier frames what the server sends and refuses once
// closed.
func TestBcastHostEnd(t *testing.T) {
	hub := &memHub{}
	self := attach(hub.join(), 1)
	end := &bcastEnd{ctx: context.Background(), bc: self, clientID: 2, in: newInbox()}

	if end.Context() != context.Background() {
		t.Fatal("Context did not return what the end was built with")
	}
	if err := end.Send(0xFF, nil); !errors.Is(err, ErrProtocol) {
		t.Fatalf("Send of a bad kind = %v, want ErrProtocol", err)
	}
	if err := end.Send(kindOperation, opsMsg{Operations: []byte("z")}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	// A message routed in by the host loop is decoded as a client message.
	end.in.push(envelope{ctl: ctlData, payload: encodeJoin(joinMsg{Document: "d", Site: 2})})
	kind, msg, err := end.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if kind != kindJoin || msg.(joinMsg).Document != "d" {
		t.Fatalf("Recv = (%d, %#v), want the client's join", kind, msg)
	}

	if err := end.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := end.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if err := end.Send(kindOperation, opsMsg{}); !errors.Is(err, ErrBroadcastClosed) {
		t.Fatalf("Send after Close = %v, want ErrBroadcastClosed", err)
	}
	if _, _, err := end.Recv(); !errors.Is(err, ErrBroadcastClosed) {
		t.Fatalf("Recv after Close = %v, want ErrBroadcastClosed", err)
	}
}

// serveBus answers a repeated hello without opening a second session, and
// ignores data addressed to a tab it does not know.
func TestBcastServeHandshakeEdges(t *testing.T) {
	hub := &memHub{}
	srv := NewServer(Config{Store: NewMemoryStore()})
	defer func() { _ = srv.Close(context.Background()) }()

	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan error, 1)
	go func() { served <- serveBus(ctx, srv, attach(hub.join(), 1)) }()

	probe := hub.join()
	welcomes := make(chan envelope, 8)
	probe.onFrame(func(raw []byte) {
		if e, ok := decodeEnvelope(raw); ok && e.ctl == ctlWelcome {
			welcomes <- e
		}
	})

	// The first hello opens tab 2's session. It is repeated until answered so the
	// test does not race serveBus attaching to the bus — the same repeat dialBus
	// makes for the same reason.
	awaitWelcome := func(what string) {
		t.Helper()
		tick := time.NewTicker(5 * time.Millisecond)
		defer tick.Stop()
		deadline := time.After(bcastSettle)
		probe.post(envelope{ctl: ctlHello, from: 2}.encode())
		for {
			select {
			case w := <-welcomes:
				if w.to != 2 {
					t.Fatalf("welcome addressed to %d, want 2", w.to)
				}
				return
			case <-tick.C:
				probe.post(envelope{ctl: ctlHello, from: 2}.encode())
			case <-deadline:
				t.Fatalf("%s went unanswered", what)
			}
		}
	}
	awaitWelcome("the first hello")

	// A hello from the tab already known is answered again, without a second
	// session: the known-tab branch of the handshake.
	drainWelcomes := func() {
		for {
			select {
			case <-welcomes:
			default:
				return
			}
		}
	}
	drainWelcomes()
	awaitWelcome("a repeated hello")

	// Data for an unknown tab, and a bye for one never seen, are both no-ops.
	probe.post(envelope{ctl: ctlData, from: 404, to: 1, payload: encodeOps(opsMsg{Operations: []byte("x")})}.encode())
	probe.post(envelope{ctl: ctlBye, from: 404}.encode())
	// The known tab then leaves cleanly.
	probe.post(envelope{ctl: ctlBye, from: 2}.encode())

	cancel()
	select {
	case err := <-served:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("serveBus returned %v, want context.Canceled", err)
		}
	case <-time.After(bcastSettle):
		t.Fatal("serveBus did not return when its context was cancelled")
	}
}

// The whole point, proven over the in-memory bus: a host tab holding a document
// and other tabs joining it converge through the real session logic — edits flow
// both ways, a third tab catches up, and a tab leaving does not disturb the
// rest.
func TestBcastTabsShareADocument(t *testing.T) {
	hub := &memHub{}
	srv := NewServer(Config{Store: NewMemoryStore()})
	defer func() { _ = srv.Close(context.Background()) }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	served := make(chan error, 1)
	go func() { served <- serveBus(ctx, srv, attach(hub.join(), 1)) }()

	// The first tab joins and types.
	a := dialClient(t, ctx, hub, 2, ClientConfig{Document: "shared", Site: 2})
	defer a.Close()
	bcastInsert(t, a, 0, "hello")

	// A second tab joins the document already being edited and catches up.
	b := dialClient(t, ctx, hub, 3, ClientConfig{Document: "shared", Site: 3})
	defer b.Close()
	awaitBcast(t, b, "the first tab's text to arrive", func() bool { return bcastText(t, b) == "hello" })

	// The second tab's edit reaches the first: co-editing, not publishing.
	bcastInsert(t, b, 5, " world")
	awaitBcast(t, a, "the second tab's edit to arrive", func() bool { return bcastText(t, a) == "hello world" })

	// A third tab joins, then the first leaves; the rest are undisturbed.
	c := dialClient(t, ctx, hub, 4, ClientConfig{Document: "shared", Site: 4})
	defer c.Close()
	awaitBcast(t, c, "the third tab to catch up", func() bool { return bcastText(t, c) == "hello world" })

	if err := a.Close(); err != nil {
		t.Fatalf("first tab Close: %v", err)
	}
	bcastInsert(t, c, 11, "!")
	awaitBcast(t, b, "an edit after a tab left", func() bool { return bcastText(t, b) == "hello world!" })

	cancel()
	select {
	case err := <-served:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("serveBus returned %v, want context.Canceled", err)
		}
	case <-time.After(bcastSettle):
		t.Fatal("serveBus did not return after its context was cancelled")
	}
}
