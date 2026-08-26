//go:build !js

package collab

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-crdt/crdt"
)

// A mesh of servers that all follow each other, with people editing on each and
// the links between them being cut while they do.
//
// It asserts two things, and the second is the one nothing else here asserts.
//
// Convergence: every participant on every server ends up with every character
// anybody typed, and with the same document rather than merely the same length.
//
// And termination. Three servers following each other in a mesh is a topology
// where an operation can go round for ever: it reaches a peer, is passed on to
// everything but the link it arrived on — which includes the link back — and
// returns to where it started. What stops it is that a server passes on only
// what it learned, so the second time round it learns nothing and says nothing.
// A test that only checks convergence would pass on a server that never stops
// talking, so this one counts what crosses the links and requires it to stop
// when the typing does.
func TestAMeshOfServersConvergesAndThenGoesQuiet(t *testing.T) {
	if os.Getenv("COLLAB_CHAOS") == "" {
		t.Skip("set COLLAB_CHAOS=1 to run the chaos harness")
	}
	servers := envInt("CHAOS_SERVERS", 3)
	editors := envInt("CHAOS_EDITORS_PER_SERVER", 4)
	rounds := envInt("CHAOS_ROUNDS", 20)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srvs := make([]*Server, servers)
	for i := range srvs {
		srvs[i] = NewServer(Config{Store: NewMemoryStore(), Backlog: 64})
		defer func(s *Server) { _ = s.Close(context.Background()) }(srvs[i])
	}

	// Every server follows every other one. A mesh rather than a chain,
	// because a chain cannot produce the loop this is about.
	var crossings atomic.Int64
	links := make([]*countingLink, 0, servers*(servers-1))
	for i := range srvs {
		for j := range srvs {
			if i == j {
				continue
			}
			link := &countingLink{peer: srvs[j], ctx: ctx, crossings: &crossings}
			links = append(links, link)
			// Links take a range of their own. A link joins a peer as a
			// participant, and two sessions claiming one replica identity
			// displace each other by design — so a link whose site happened to
			// be a participant's would take turns evicting somebody for as long
			// as the test ran. The old numbering gave links 901..1032 and the
			// people on server 1 sites 1001..1020, which is twenty identities
			// shared between the two.
			go func(follower *Server, l *countingLink, as crdt.SiteID) {
				_ = follower.FollowWithRetry(ctx, l.dial, "paper", as,
					RetryPolicy{Wait: time.Millisecond, Ceiling: 20 * time.Millisecond})
			}(srvs[i], link, crdt.SiteID(900000+len(links)))
		}
	}

	// People, spread across the servers, and staying in the document the way an
	// application would: a room this size is well over Config.Backlog once the
	// mesh's own traffic is on top of it, so everybody is disconnected sooner
	// or later and everybody has to come back. A harness that used Join here
	// would be measuring how long a backlog lasts rather than whether a
	// federation converges.
	clients := make([]*Client, 0, servers*editors)
	for i, s := range srvs {
		for k := 0; k < editors; k++ {
			s := s
			dial := func(ctx context.Context) (Transport, error) {
				transport, conn := Pipe()
				go func() { _ = s.ServePipe(ctx, conn) }()
				return transport, nil
			}
			c, err := JoinWithRetry(ctx, dial, ClientConfig{
				Document: "paper",
				Site:     crdt.SiteID(i*1000 + k + 1),
			}, RetryPolicy{Wait: time.Millisecond, Ceiling: 20 * time.Millisecond})
			if err != nil {
				t.Fatalf("a participant on server %d could not join: %v", i, err)
			}
			clients = append(clients, c)
		}
	}
	defer func() {
		for _, c := range clients {
			_ = c.Close()
		}
	}()

	// Everybody types, and the links are cut while they do.
	var typed atomic.Int64
	var wg sync.WaitGroup
	for i, c := range clients {
		wg.Add(1)
		go func(i int, c *Client) {
			defer wg.Done()
			r := rand.New(rand.NewPCG(uint64(i), 3))
			body, err := c.Text("body")
			if err != nil {
				t.Errorf("participant %d has no body: %v", i, err)
				return
			}
			for k := 0; k < rounds; k++ {
				at := 0
				if n := body.Len(); n > 0 {
					at = r.IntN(n + 1)
				}
				if err := body.Insert(at, "x"); err != nil {
					t.Errorf("participant %d: %v", i, err)
					return
				}
				typed.Add(1)
				time.Sleep(time.Duration(r.IntN(3000)) * time.Microsecond)
			}
		}(i, c)
	}
	stop := make(chan struct{})
	var wgb sync.WaitGroup
	wgb.Add(1)
	go func() {
		defer wgb.Done()
		r := rand.New(rand.NewPCG(11, 4))
		for {
			select {
			case <-stop:
				return
			case <-time.After(7 * time.Millisecond):
			}
			links[r.IntN(len(links))].cut()
		}
	}()
	wg.Wait()
	close(stop)
	wgb.Wait()

	want := int(typed.Load())
	if want != len(clients)*rounds {
		t.Fatalf("only %d of %d edits were made", want, len(clients)*rounds)
	}

	// Everybody, on every server, ends up with everything.
	settleStart := time.Now()
	deadline := time.Now().Add(time.Duration(envInt("CHAOS_SETTLE_SECONDS", 90)) * time.Second)
	for {
		short := 0
		for _, c := range clients {
			if b, err := c.Text("body"); err != nil || b.Len() != want {
				short++
			}
		}
		if short == 0 {
			t.Logf("settled in %v", time.Since(settleStart))
			break
		}
		if time.Now().After(deadline) {
			// What is each straggler missing, and does anybody hold it all?
			best, bestAt := 0, -1
			for i, c := range clients {
				if b, err := c.Text("body"); err == nil && b.Len() > best {
					best, bestAt = b.Len(), i
				}
			}
			t.Logf("  the fullest participant is %d with %d of %d", bestAt, best, want)
			shown := 0
			for i, c := range clients {
				b, err := c.Text("body")
				if err != nil || b.Len() == want || shown >= 5 {
					continue
				}
				shown++
				c.mu.Lock()
				parked := c.doc.DropPending()
				c.mu.Unlock()
				state := "connected"
				select {
				case <-c.Done():
					state = "gave up"
				default:
				}
				t.Logf("  participant %d (server %d): %d of %d, %s, %d parked",
					i, i/editors, b.Len(), want, state, parked)
			}
			t.Fatalf("%d of %d participants are short of %d characters after %ds",
				short, len(clients), want, envInt("CHAOS_SETTLE_SECONDS", 90))
		}
		time.Sleep(5 * time.Millisecond)
	}
	first := clients[0].Snapshot()
	for i, c := range clients {
		if got := c.Snapshot(); string(got) != string(first) {
			t.Fatalf("participant %d holds a different document (%d bytes against %d)",
				i, len(got), len(first))
		}
	}

	// And now the mesh has to go quiet. An operation that has been round the
	// loop teaches nobody anything the second time, so nothing should be
	// crossing a link once the typing has stopped and everyone agrees.
	settleDown := crossings.Load()
	time.Sleep(500 * time.Millisecond)
	after := crossings.Load()
	t.Logf("%d participants across %d servers, %d characters, %d messages crossed links",
		len(clients), servers, want, after)
	if grew := after - settleDown; grew > 0 {
		t.Fatalf("the mesh is still talking after everyone agreed: %d more messages crossed links in half a second", grew)
	}
	if after == 0 {
		t.Fatal("nothing ever crossed a link; the mesh was not a mesh")
	}
}

// countingLink is a transport for one link, which counts what crosses it and
// can be cut.
type countingLink struct {
	peer      *Server
	ctx       context.Context
	crossings *atomic.Int64

	mu    sync.Mutex
	conns []carrierConn
}

func (l *countingLink) dial(context.Context) (Transport, error) { return l, nil }

func (l *countingLink) open(ctx context.Context) (carrierConn, error) {
	transport, server := Pipe()
	go func() { _ = l.peer.ServePipe(l.ctx, server) }()
	conn, err := transport.open(ctx)
	if err != nil {
		return nil, err
	}
	counted := &countedConn{inner: conn, crossings: l.crossings}
	l.mu.Lock()
	l.conns = append(l.conns, counted)
	l.mu.Unlock()
	return counted, nil
}

func (l *countingLink) cut() {
	l.mu.Lock()
	conns := l.conns
	l.conns = nil
	l.mu.Unlock()
	for _, c := range conns {
		_ = c.Close()
	}
}

type countedConn struct {
	inner     carrierConn
	crossings *atomic.Int64
}

func (c *countedConn) Send(kind byte, msg any) error {
	if kind == kindOperation {
		c.crossings.Add(1)
	}
	return c.inner.Send(kind, msg)
}

func (c *countedConn) Recv() (byte, any, error) {
	kind, msg, err := c.inner.Recv()
	if err == nil && kind == kindOperation {
		c.crossings.Add(1)
	}
	return kind, msg, err
}

func (c *countedConn) Close() error { return c.inner.Close() }

var _ = fmt.Sprintf
