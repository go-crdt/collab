//go:build !js

package collab

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/go-crdt/crdt"
)

// breakable hands out carriers it keeps a hold of, so a test can cut a session
// the way a network does rather than by asking anybody politely.
type breakable struct {
	srv *Server
	ctx context.Context

	mu    sync.Mutex
	conns []carrierConn
	opens int
}

func (b *breakable) open(ctx context.Context) (carrierConn, error) {
	transport, server := Pipe()
	go func() { _ = b.srv.ServePipe(b.ctx, server) }()
	conn, err := transport.open(ctx)
	if err != nil {
		return nil, err
	}
	b.mu.Lock()
	b.conns = append(b.conns, conn)
	b.opens++
	b.mu.Unlock()
	return conn, nil
}

func (b *breakable) dial(context.Context) (Transport, error) { return b, nil }

// cut ends every session it has handed out.
func (b *breakable) cut() {
	b.mu.Lock()
	conns := b.conns
	b.conns = nil
	b.mu.Unlock()
	for _, c := range conns {
		_ = c.Close()
	}
}

func (b *breakable) sessions() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.opens
}

func waitFor2(t *testing.T, what string, want func() bool) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for !want() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// A participant whose session ends comes back, and what it wrote while there
// was nowhere to send it comes back with it.
//
// This is the half of Config.Backlog that the documentation promised and
// nothing did: "it rejoins and is caught up from its version vector".
func TestAParticipantComesBackAndBringsWhatItWroteWhileItWasGone(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	srv := NewServer(Config{Store: NewMemoryStore()})
	defer func() { _ = srv.Close(context.Background()) }()

	// Somebody else, to watch what the server ends up holding.
	watch, wconn := Pipe()
	go func() { _ = srv.ServePipe(ctx, wconn) }()
	other, err := Join(ctx, watch, ClientConfig{Document: "d", Site: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = other.Close() }()
	watched, err := other.Text("body")
	if err != nil {
		t.Fatal(err)
	}

	link := &breakable{srv: srv, ctx: ctx}
	var ups, downs int
	var mu sync.Mutex
	c, err := JoinWithRetry(ctx, link.dial, ClientConfig{Document: "d", Site: 1}, RetryPolicy{
		Wait:    time.Millisecond,
		Ceiling: 20 * time.Millisecond,
		Notify: func(s LinkStatus) {
			mu.Lock()
			if s.Up {
				ups++
			} else {
				downs++
			}
			mu.Unlock()
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	body, err := c.Text("body")
	if err != nil {
		t.Fatal(err)
	}
	if err := body.Insert(0, "before"); err != nil {
		t.Fatal(err)
	}
	waitFor2(t, "the first edit to reach the other participant", func() bool {
		return watched.String() == "before"
	})

	// The session is cut, and this participant goes on typing into a document
	// with nowhere to send it.
	link.cut()
	waitFor2(t, "the outage to be noticed", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return downs > 0
	})
	// An edit made now must not look like a failure to whoever made it.
	if err := body.Insert(body.Len(), ", and while away"); err != nil {
		t.Fatalf("an edit made during an outage was reported as a failure: %v", err)
	}

	// It comes back on its own, and brings the edit.
	waitFor2(t, "what was written during the outage to arrive", func() bool {
		return watched.String() == "before, and while away"
	})
	mu.Lock()
	gotUps := ups
	mu.Unlock()
	if gotUps == 0 {
		t.Fatal("nobody was told the session came back")
	}
	// The handle taken before the outage is the handle still being used.
	if got := body.String(); got != "before, and while away" {
		t.Fatalf("the handle from before the outage reads %q", got)
	}
}

// Closing a participant stops it, including while it is waiting to try again.
func TestClosingAParticipantStopsItEvenWhileItIsWaiting(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	srv := NewServer(Config{Store: NewMemoryStore()})
	defer func() { _ = srv.Close(context.Background()) }()

	// A dialler that works once and then never again, so the participant is
	// certainly inside its backoff when it is closed.
	link := &breakable{srv: srv, ctx: ctx}
	var opened int
	var mu sync.Mutex
	dial := func(ctx context.Context) (Transport, error) {
		mu.Lock()
		defer mu.Unlock()
		opened++
		if opened > 1 {
			return nil, errors.New("nothing is answering")
		}
		return link, nil
	}
	waiting := make(chan struct{})
	var once sync.Once
	c, err := JoinWithRetry(ctx, dial, ClientConfig{Document: "d", Site: 1}, RetryPolicy{
		// A wait long enough that it certainly has not finished: what is being
		// tested is that closing ends the wait rather than sitting it out.
		Wait:    30 * time.Second,
		Ceiling: 30 * time.Second,
		// Notify is called before the sleep, so this fires exactly when the
		// participant is about to wait — waiting for a second dial instead
		// would be waiting for the far side of the sleep.
		Notify: func(s LinkStatus) {
			if !s.Up {
				once.Do(func() { close(waiting) })
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	link.cut()
	select {
	case <-waiting:
	case <-time.After(10 * time.Second):
		t.Fatal("the participant never noticed the outage")
	}

	done := make(chan error, 1)
	go func() { done <- c.Close() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Close waited for a backoff instead of ending it")
	}
	select {
	case <-c.Done():
	default:
		t.Fatal("a closed participant is not done")
	}
}

// A failure the operator calls permanent ends the participant rather than
// being retried until somebody notices.
func TestAPermanentFailureEndsTheParticipant(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	srv := NewServer(Config{Store: NewMemoryStore()})
	defer func() { _ = srv.Close(context.Background()) }()

	link := &breakable{srv: srv, ctx: ctx}
	c, err := JoinWithRetry(ctx, link.dial, ClientConfig{Document: "d", Site: 1}, RetryPolicy{
		Wait:      time.Millisecond,
		Ceiling:   time.Millisecond,
		Permanent: func(error) bool { return true },
	})
	if err != nil {
		t.Fatal(err)
	}
	link.cut()
	select {
	case <-c.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("a permanent failure did not end the participant")
	}
	if c.Err() == nil {
		t.Fatal("a participant that ended reports no reason")
	}
}

// What JoinWithRetry refuses before it starts anything.
func TestJoinWithRetryRefusesWhatItCannotDo(t *testing.T) {
	ctx := context.Background()
	srv := NewServer(Config{Store: NewMemoryStore()})
	defer func() { _ = srv.Close(ctx) }()
	link := &breakable{srv: srv, ctx: ctx}

	for _, c := range []struct {
		name   string
		dial   Dialer
		cfg    ClientConfig
		policy RetryPolicy
		says   string
	}{
		{"no dialler", nil, ClientConfig{Document: "d"}, RetryPolicy{}, "dialler"},
		{"no document", link.dial, ClientConfig{}, RetryPolicy{}, "document"},
		{"a negative wait", link.dial, ClientConfig{Document: "d"}, RetryPolicy{Wait: -time.Second}, "negative"},
		{"a wait past the ceiling", link.dial, ClientConfig{Document: "d"}, RetryPolicy{Wait: time.Minute, Ceiling: time.Second}, "capped at"},
	} {
		t.Run(c.name, func(t *testing.T) {
			got, err := JoinWithRetry(ctx, c.dial, c.cfg, c.policy)
			if err == nil {
				_ = got.Close()
				t.Fatal("it was accepted")
			}
			if !contains(err.Error(), c.says) {
				t.Fatalf("the refusal does not say why: %v", err)
			}
		})
	}
}

// A dialler that cannot reach anybody at all is an error the caller gets,
// rather than an outage they have to watch for.
func TestJoinWithRetryFailsIfItCannotJoinAtAll(t *testing.T) {
	nothing := errors.New("nothing is answering")
	_, err := JoinWithRetry(context.Background(),
		func(context.Context) (Transport, error) { return nil, nothing },
		ClientConfig{Document: "d", Site: 1}, RetryPolicy{})
	if !errors.Is(err, nothing) {
		t.Fatalf("got %v", err)
	}
}

// The whole of the audit's finding, answered: a burst that disconnects
// everybody, and everybody comes back.
//
// One edit by each of P participants is P-1 messages into every other queue, so
// a document busier than Config.Backlog disconnects everyone in it at once.
// Measured before this existed: 998 of 1000 sessions ended and stayed ended.
func TestABurstThatDisconnectsEverybodyIsSurvived(t *testing.T) {
	const editors = 60
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// A backlog far under the number editing at once, so the burst certainly
	// overflows it.
	srv := NewServer(Config{Store: NewMemoryStore(), Backlog: 4})
	defer func() { _ = srv.Close(context.Background()) }()

	clients := make([]*Client, 0, editors)
	for i := 0; i < editors; i++ {
		link := &breakable{srv: srv, ctx: ctx}
		c, err := JoinWithRetry(ctx, link.dial,
			ClientConfig{Document: "one", Site: crdt.SiteID(i + 1)},
			RetryPolicy{Wait: time.Millisecond, Ceiling: 50 * time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}
		clients = append(clients, c)
	}
	defer func() {
		for _, c := range clients {
			_ = c.Close()
		}
	}()

	var wg sync.WaitGroup
	for _, c := range clients {
		wg.Add(1)
		go func(c *Client) {
			defer wg.Done()
			if body, err := c.Text("body"); err == nil {
				_ = body.Insert(0, "x")
			}
		}(c)
	}
	wg.Wait()

	// Everybody ends up holding everybody's edit, however many sessions it took.
	waitFor2(t, "every participant to hold every edit", func() bool {
		for _, c := range clients {
			body, err := c.Text("body")
			if err != nil || body.Len() != editors {
				return false
			}
		}
		return true
	})
	for _, c := range clients {
		select {
		case <-c.Done():
			t.Fatal("a participant gave up")
		default:
		}
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// awkward is a transport that fails wherever it was told to, so that every way
// an attempt can go wrong is a test rather than a branch nobody reaches.
type awkward struct {
	srv *Server
	ctx context.Context

	mu     sync.Mutex
	failAt string // "open", "send", "recv", "welcome", "absorb", or ""
	stage  int    // how many opens have happened
	failOn int    // which open to fail at; 0 means every one after the first
	conns  []carrierConn
}

func (a *awkward) dial(context.Context) (Transport, error) { return a, nil }

func (a *awkward) open(ctx context.Context) (carrierConn, error) {
	a.mu.Lock()
	a.stage++
	stage, at := a.stage, a.failAt
	a.mu.Unlock()
	if stage <= 1 || at == "" {
		// The first session is a real one, so the client exists to be broken.
		transport, server := Pipe()
		go func() { _ = a.srv.ServePipe(a.ctx, server) }()
		conn, err := transport.open(ctx)
		if err != nil {
			return nil, err
		}
		a.mu.Lock()
		a.conns = append(a.conns, conn)
		a.mu.Unlock()
		return conn, nil
	}
	if at == "open" {
		return nil, errors.New("nothing is listening")
	}
	return &bad{at: at}, nil
}

func (a *awkward) cut() {
	a.mu.Lock()
	conns := a.conns
	a.conns = nil
	a.mu.Unlock()
	for _, c := range conns {
		_ = c.Close()
	}
}

// bad is a carrier that misbehaves in one named way.
type bad struct {
	at    string
	sends int
}

func (b *bad) Send(byte, any) error {
	b.sends++
	switch {
	case b.at == "send":
		return errors.New("it would not go")
	case b.at == "push" && b.sends > 1:
		// The join went; the push behind it does not.
		return errors.New("it would not go")
	}
	return nil
}

func (b *bad) Recv() (byte, any, error) {
	switch b.at {
	case "recv":
		return 0, nil, errors.New("nothing came back")
	case "welcome":
		return kindOperation, opsMsg{}, nil
	case "absorb":
		return kindWelcome, welcomeMsg{Snapshot: []byte("not a snapshot")}, nil
	}
	// "push": a welcome that asks for everything this replica has, so there is
	// something to push, and a Send that refuses it.
	return kindWelcome, welcomeMsg{}, nil
}

func (b *bad) Close() error { return nil }

// Every way one attempt can fail, and none of them ends the participant.
func TestEveryWayAnAttemptFailsIsTriedAgain(t *testing.T) {
	for _, at := range []string{"open", "send", "recv", "welcome", "absorb", "push"} {
		t.Run(at, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			srv := NewServer(Config{Store: NewMemoryStore()})
			defer func() { _ = srv.Close(context.Background()) }()

			link := &awkward{srv: srv, ctx: ctx, failAt: at}
			tries := make(chan struct{}, 64)
			c, err := JoinWithRetry(ctx, link.dial, ClientConfig{Document: "d", Site: 1}, RetryPolicy{
				Wait:    time.Millisecond,
				Ceiling: 2 * time.Millisecond,
				Notify: func(s LinkStatus) {
					if !s.Up {
						select {
						case tries <- struct{}{}:
						default:
						}
					}
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = c.Close() }()

			link.cut()
			// It keeps trying: three reports of being down is enough to say the
			// loop is a loop rather than one attempt.
			for i := 0; i < 3; i++ {
				select {
				case <-tries:
				case <-time.After(10 * time.Second):
					t.Fatalf("only %d attempts after a failure at %s", i, at)
				}
			}
			select {
			case <-c.Done():
				t.Fatalf("a failure at %s ended the participant", at)
			default:
			}
		})
	}
}

// An edit made while there is genuinely no carrier is kept, not failed.
func TestAnEditWithNoCarrierIsKeptRatherThanFailed(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	srv := NewServer(Config{Store: NewMemoryStore()})
	defer func() { _ = srv.Close(context.Background()) }()

	link := &awkward{srv: srv, ctx: ctx, failAt: "open"}
	down := make(chan struct{})
	var once sync.Once
	c, err := JoinWithRetry(ctx, link.dial, ClientConfig{Document: "d", Site: 1}, RetryPolicy{
		Wait:    time.Second,
		Ceiling: time.Second,
		Notify: func(s LinkStatus) {
			if !s.Up {
				once.Do(func() { close(down) })
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	link.cut()
	<-down

	body, err := c.Text("body")
	if err != nil {
		t.Fatal(err)
	}
	// There is no carrier at all now, so this is the swallowed path.
	if err := body.Insert(0, "written into the dark"); err != nil {
		t.Fatalf("an edit with no carrier was reported as a failure: %v", err)
	}
	if got := body.String(); got != "written into the dark" {
		t.Fatalf("the edit did not stand locally: %q", got)
	}
}

// A participant whose context ends stops, whether it is connected or waiting.
func TestAParticipantStopsWhenItsContextDoes(t *testing.T) {
	for _, c := range []struct {
		name string
		cut  bool
	}{
		{"while it is connected", false},
		{"while it is waiting", true},
	} {
		t.Run(c.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			srv := NewServer(Config{Store: NewMemoryStore()})
			defer func() { _ = srv.Close(context.Background()) }()

			link := &awkward{srv: srv, ctx: ctx, failAt: "open"}
			down := make(chan struct{})
			var once sync.Once
			client, err := JoinWithRetry(ctx, link.dial, ClientConfig{Document: "d", Site: 1}, RetryPolicy{
				Wait:    30 * time.Second,
				Ceiling: 30 * time.Second,
				Notify: func(s LinkStatus) {
					if !s.Up {
						once.Do(func() { close(down) })
					}
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			if c.cut {
				link.cut()
				<-down
			}
			cancel()
			select {
			case <-client.Done():
			case <-time.After(10 * time.Second):
				t.Fatal("a cancelled participant did not stop")
			}
			if client.Err() == nil {
				t.Fatal("a participant that stopped reports no reason")
			}
		})
	}
}

// A dialler that cannot produce a transport at all is tried again, not given up
// on: it is the shape of a name that does not resolve for a minute.
func TestADiallerThatFailsIsTriedAgain(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	srv := NewServer(Config{Store: NewMemoryStore()})
	defer func() { _ = srv.Close(context.Background()) }()

	link := &awkward{srv: srv, ctx: ctx}
	var opens int
	var mu sync.Mutex
	dial := func(ctx context.Context) (Transport, error) {
		mu.Lock()
		defer mu.Unlock()
		opens++
		if opens > 1 {
			return nil, errors.New("that name does not resolve")
		}
		return link, nil
	}
	tries := make(chan struct{}, 64)
	c, err := JoinWithRetry(ctx, dial, ClientConfig{Document: "d", Site: 1}, RetryPolicy{
		Wait:    time.Millisecond,
		Ceiling: 2 * time.Millisecond,
		Notify: func(s LinkStatus) {
			if !s.Up {
				select {
				case tries <- struct{}{}:
				default:
				}
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	link.cut()
	for i := 0; i < 3; i++ {
		select {
		case <-tries:
		case <-time.After(10 * time.Second):
			t.Fatalf("only %d attempts after a dialler that failed", i)
		}
	}
}

// A first join that is refused is an error the caller gets.
func TestJoinWithRetryFailsIfTheFirstJoinIsRefused(t *testing.T) {
	_, err := JoinWithRetry(context.Background(),
		func(context.Context) (Transport, error) { return &alwaysBad{}, nil },
		ClientConfig{Document: "d", Site: 1}, RetryPolicy{})
	if err == nil {
		t.Fatal("a join that could not complete was reported as a success")
	}
}

// alwaysBad opens carriers that never answer.
type alwaysBad struct{}

func (alwaysBad) open(context.Context) (carrierConn, error) { return &bad{at: "recv"}, nil }

// sticky is a carrier that can be made to fail its sends while its reads are
// still blocked, so that an edit meets a broken carrier before the supervisor
// has noticed anything is wrong. That window is a real one — it is the instant
// a connection dies — and it is the only way to reach the swallow in transmit
// that is about a send rather than about there being no carrier at all.
type sticky struct {
	inner carrierConn
	mu    sync.Mutex
	broke bool
	held  chan struct{}
}

func (s *sticky) Send(kind byte, msg any) error {
	s.mu.Lock()
	broke := s.broke
	s.mu.Unlock()
	if broke {
		return errors.New("the connection is gone")
	}
	return s.inner.Send(kind, msg)
}

func (s *sticky) Recv() (byte, any, error) {
	kind, msg, err := s.inner.Recv()
	if err != nil {
		// Hold the session open so the supervisor does not notice yet.
		<-s.held
	}
	return kind, msg, err
}

func (s *sticky) Close() error { return s.inner.Close() }

func (s *sticky) breakSends() {
	s.mu.Lock()
	s.broke = true
	s.mu.Unlock()
}

type stickyTransport struct {
	srv  *Server
	ctx  context.Context
	made chan *sticky
}

func (st *stickyTransport) open(ctx context.Context) (carrierConn, error) {
	transport, server := Pipe()
	go func() { _ = st.srv.ServePipe(st.ctx, server) }()
	conn, err := transport.open(ctx)
	if err != nil {
		return nil, err
	}
	s := &sticky{inner: conn, held: make(chan struct{})}
	select {
	case st.made <- s:
	default:
	}
	return s, nil
}

func (st *stickyTransport) dial(context.Context) (Transport, error) { return st, nil }

// An edit that meets a carrier failing under it is kept, not failed.
func TestAnEditThatMeetsADyingCarrierIsKept(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	srv := NewServer(Config{Store: NewMemoryStore()})
	defer func() { _ = srv.Close(context.Background()) }()

	st := &stickyTransport{srv: srv, ctx: ctx, made: make(chan *sticky, 4)}
	c, err := JoinWithRetry(ctx, st.dial, ClientConfig{Document: "d", Site: 1}, RetryPolicy{
		Wait:    time.Millisecond,
		Ceiling: 2 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	first := <-st.made
	// The carrier's sends start failing while its reads are still held, so the
	// supervisor has not marked the client down.
	first.breakSends()

	body, err := c.Text("body")
	if err != nil {
		t.Fatal(err)
	}
	if err := body.Insert(0, "into a carrier that is already gone"); err != nil {
		t.Fatalf("an edit that met a dying carrier was reported as a failure: %v", err)
	}
	if got := body.String(); got != "into a carrier that is already gone" {
		t.Fatalf("the edit did not stand locally: %q", got)
	}
	close(first.held)
}
