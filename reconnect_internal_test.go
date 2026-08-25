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

// A backoff is a sequence of delays, and the sequence is the property: that it
// grows, that it stops growing where it was told to, and that the jitter moves
// the delay without moving the state the next delay is computed from.
//
// None of it is waited for. The policy here reaches eight seconds and the whole
// test takes microseconds, because the loop's clock, its sleep and its source of
// randomness are fields — which is the difference between asserting a backoff
// and asserting that something eventually happened again.
func TestTheBackoffGrowsIsCappedAndIsJittered(t *testing.T) {
	// The link never comes up: every attempt fails, which is what makes the
	// sequence visible in the first place.
	unreachable := errors.New("no route to the peer")

	for _, tt := range []struct {
		name   string
		random float64
		want   []time.Duration
	}{
		// The bottom of the band, the middle, and the top. Doubling is the same
		// in all three, which is the assertion that the jitter is applied to the
		// delay and not fed back into the interval it was drawn from.
		{"the bottom of the band", 0, []time.Duration{
			500 * time.Millisecond, time.Second, 2 * time.Second,
			4 * time.Second, 4 * time.Second, 4 * time.Second,
		}},
		{"the middle of the band", 0.5, []time.Duration{
			750 * time.Millisecond, 1500 * time.Millisecond, 3 * time.Second,
			6 * time.Second, 6 * time.Second, 6 * time.Second,
		}},
		{"the top of the band", 1, []time.Duration{
			time.Second, 2 * time.Second, 4 * time.Second,
			8 * time.Second, 8 * time.Second, 8 * time.Second,
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s := NewServer(Config{Store: NewMemoryStore()})
			t.Cleanup(func() { _ = s.Close(context.Background()) })

			var reported []LinkStatus
			link, err := s.reconnecting(
				func(context.Context) (Transport, error) { return nil, unreachable },
				"doc", 42,
				RetryPolicy{
					Wait:    time.Second,
					Ceiling: 8 * time.Second,
					Notify:  func(st LinkStatus) { reported = append(reported, st) },
				})
			if err != nil {
				t.Fatal(err)
			}

			// A clock that only moves when the link sleeps, so "down for" is
			// exactly the time the link spent waiting and nothing else.
			clock := time.Date(2026, 8, 24, 17, 0, 0, 0, time.UTC)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var slept []time.Duration
			link.back.random = func() float64 { return tt.random }
			link.back.now = func() time.Time { return clock }
			link.back.sleep = func(_ context.Context, d time.Duration) error {
				slept = append(slept, d)
				clock = clock.Add(d)
				if len(slept) == len(tt.want) {
					cancel()
					return ctx.Err()
				}
				return nil
			}

			if err := link.run(ctx); !errors.Is(err, context.Canceled) {
				t.Fatalf("the link ended with %v, want the cancellation", err)
			}

			if len(slept) != len(tt.want) {
				t.Fatalf("the link waited %d times, want %d", len(slept), len(tt.want))
			}
			for i, want := range tt.want {
				if slept[i] != want {
					t.Fatalf("wait %d was %v, want %v (whole sequence %v)", i+1, slept[i], want, slept)
				}
			}

			// And what the operator was told, which has to agree with what
			// actually happened: every report names the failure, counts the
			// attempts since the link was last up, says what it is about to
			// wait, and says how long the outage has run.
			if len(reported) != len(tt.want) {
				t.Fatalf("the operator was told %d times, want %d", len(reported), len(tt.want))
			}
			var down time.Duration
			for i, st := range reported {
				if st.Up {
					t.Fatalf("report %d says the link is up; it never was", i+1)
				}
				if !errors.Is(st.Err, unreachable) {
					t.Fatalf("report %d blames %v, want the dial failure", i+1, st.Err)
				}
				if st.Attempt != i+1 {
					t.Fatalf("report %d counts attempt %d", i+1, st.Attempt)
				}
				if st.RetryIn != tt.want[i] {
					t.Fatalf("report %d promises to retry in %v, and waited %v", i+1, st.RetryIn, tt.want[i])
				}
				if st.DownFor != down {
					t.Fatalf("report %d says down for %v, want %v", i+1, st.DownFor, down)
				}
				down += tt.want[i]
			}
		})
	}
}

// The jitter of the real source, rather than of a fake that returns what the
// test wants: every delay inside the band, and not every delay the same.
//
// The band matters in both directions. Its ceiling is what stops two links
// waiting the same interval; its floor is what stops a draw near zero turning a
// backoff into the hammering it exists to prevent.
func TestTheJitterStaysInsideItsBand(t *testing.T) {
	s := NewServer(Config{Store: NewMemoryStore()})
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	link, err := s.reconnecting(
		func(context.Context) (Transport, error) { return nil, errors.New("no") },
		"doc", 42, RetryPolicy{})
	if err != nil {
		t.Fatal(err)
	}

	const interval = 4 * time.Second
	seen := make(map[time.Duration]bool)
	for range 1000 {
		got := link.back.jittered(interval)
		if got < interval/2 || got >= interval {
			t.Fatalf("a delay of %v is outside [%v, %v)", got, interval/2, interval)
		}
		seen[got] = true
	}
	// A source that always returned the same number would satisfy the band and
	// leave every link in the datacentre in step, which is the whole point.
	if len(seen) < 100 {
		t.Fatalf("1000 draws produced only %d distinct delays", len(seen))
	}
}

// A gated peer: a dialler onto a real server, which can be told to refuse and
// whose established session can be pulled out from under the link.
//
// The sessions are real — a pipe, a welcome, the peer's operations — so what
// this drops is a link that was genuinely working, and what comes back is a
// session that genuinely re-established.
type gatedPeer struct {
	mu      sync.Mutex
	server  *Server
	refuse  int // how many more dials to turn away
	dials   int
	dropped context.CancelFunc
}

func (g *gatedPeer) dial(ctx context.Context) (Transport, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.dials++
	if g.refuse > 0 {
		g.refuse--
		return nil, errors.New("the peer is not reachable")
	}
	peer, serverEnd := Pipe()
	session, drop := context.WithCancel(ctx)
	g.dropped = drop
	go func() { _ = g.server.ServePipe(session, serverEnd) }()
	return peer, nil
}

// drop ends the session the peer is currently serving, the way a peer that goes
// away ends it.
func (g *gatedPeer) drop() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.dropped()
}

// participant joins a server in process, as somebody typing would.
func participant(t *testing.T, s *Server, cfg ClientConfig) *Client {
	t.Helper()
	transport, serverEnd := Pipe()
	ctx, cancel := context.WithCancel(t.Context())
	go func() { _ = s.ServePipe(ctx, serverEnd) }()
	c, err := Join(t.Context(), transport, cfg)
	if err != nil {
		t.Fatalf("Join(%q, site %d): %v", cfg.Document, cfg.Site, err)
	}
	t.Cleanup(func() { _ = c.Close(); cancel() })
	return c
}

// awaitText waits for a participant to hold exactly this text. Everything here
// is in one process over channels, so the bound is a failure and not a slow
// machine.
func awaitText(t *testing.T, c *Client, want string) {
	t.Helper()
	deadline := time.After(10 * time.Second)
	for {
		h, err := c.Text("body")
		if err != nil {
			t.Fatalf("Text(\"body\"): %v", err)
		}
		if h.String() == want {
			return
		}
		select {
		case <-c.Changes():
		case <-c.Done():
			t.Fatalf("the session ended before the text was %q: %v", want, c.Err())
		case <-deadline:
			t.Fatalf("timed out waiting for %q; the participant holds %q", want, h.String())
		}
	}
}

// awaited reads what the link is expected to say next, with a bound.
//
// Everything here is channels in one process, so a wait of any length is a
// failure. It is bounded rather than left to block because the failure this
// test exists to catch — a link that does not come back — would otherwise be a
// package that hangs until the whole run times out and prints a goroutine dump,
// instead of a line saying what was being waited for.
func awaited[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
		var zero T
		return zero
	}
}

// released lets the waiting link try again, with the same bound and for the
// same reason.
func released(t *testing.T, resume chan<- struct{}) {
	t.Helper()
	select {
	case resume <- struct{}{}:
	case <-time.After(10 * time.Second):
		t.Fatal("the link was not waiting to be let go")
	}
}

// The whole thing, as an operator meets it: a link that cannot be made at first,
// then is; a link that is working and is pulled out; and a replica that has
// fallen behind while it was down catching up when it comes back.
//
// The last part is the assertion that matters. What Ada types during the outage
// cannot reach Grace over a link that does not exist, so Grace holding it
// afterwards is proof that a new session was established and was caught up from
// the version this replica already had — not that a retry loop went round.
func TestALinkComesBackAndTheDocumentsConvergeAgain(t *testing.T) {
	paris := NewServer(Config{Store: NewMemoryStore()})
	lyon := NewServer(Config{Store: NewMemoryStore()})
	t.Cleanup(func() { _ = paris.Close(context.Background()); _ = lyon.Close(context.Background()) })

	ada := participant(t, paris, ClientConfig{Document: "doc", Site: 1})
	grace := participant(t, lyon, ClientConfig{Document: "doc", Site: 2})

	// The peer is unreachable for the first two attempts, so the link has to
	// back off before it ever comes up once.
	peer := &gatedPeer{server: paris, refuse: 2}

	up := make(chan LinkStatus, 16)
	down := make(chan LinkStatus, 16)
	link, err := lyon.reconnecting(peer.dial, "doc", 999, RetryPolicy{
		Wait:    time.Second,
		Ceiling: time.Minute,
		Notify: func(st LinkStatus) {
			if st.Up {
				up <- st
				return
			}
			down <- st
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// The link hands over each delay and then waits to be let go, so the test
	// says when a reconnection happens rather than hoping one has by now.
	slept := make(chan time.Duration)
	resume := make(chan struct{})
	link.back.random = func() float64 { return 1 }
	link.back.sleep = func(ctx context.Context, d time.Duration) error {
		select {
		case slept <- d:
		case <-ctx.Done():
			return ctx.Err()
		}
		select {
		case <-resume:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	ended := make(chan error, 1)
	go func() { ended <- link.run(ctx) }()

	// Two refusals, one second then two, and the operator hears about both.
	for i, want := range []time.Duration{time.Second, 2 * time.Second} {
		st := awaited(t, down, "the link to report the failed dial")
		if st.Attempt != i+1 {
			t.Fatalf("the first outage reported attempt %d as %d", i+1, st.Attempt)
		}
		if d := awaited(t, slept, "the link to start waiting"); d != want {
			t.Fatalf("attempt %d waited %v, want %v", i+1, d, want)
		}
		released(t, resume)
	}

	// The third dial reaches Paris, and the two servers are linked.
	if st := awaited(t, up, "the link to come up"); !st.Up {
		t.Fatal("the link reported itself up with Up false")
	}
	body, err := ada.Text("body")
	if err != nil {
		t.Fatal(err)
	}
	if err := body.Insert(0, "avant"); err != nil {
		t.Fatal(err)
	}
	awaitText(t, grace, "avant")

	// Now pull the session out from under it, the way a peer that goes away
	// does.
	peer.drop()
	st := awaited(t, down, "the link to report the dropped session")
	if st.Attempt != 1 {
		t.Fatalf("the new outage started at attempt %d; a link that had been up should have reset its backoff", st.Attempt)
	}
	if st.Err == nil {
		t.Fatal("the dropped link was reported without saying why")
	}
	if d := awaited(t, slept, "the link to start waiting again"); d != time.Second {
		t.Fatalf("the link waited %v after a session that had worked, want the policy's first wait", d)
	}

	// The link is down and is waiting to be let go, so this is typed into a
	// Paris that has no way of telling Lyon about it.
	if err := body.Insert(body.Len(), " pendant"); err != nil {
		t.Fatal(err)
	}
	released(t, resume)
	if st := awaited(t, up, "the link to come back"); !st.Up {
		t.Fatal("the link reported itself up with Up false")
	}

	// And Grace, on the other server, has it.
	awaitText(t, grace, "avant pendant")

	// A link taken down on purpose says so, rather than reporting whatever the
	// session happened to fail with on its way out.
	cancel()
	select {
	case err := <-ended:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("the cancelled link ended with %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("a cancelled link did not end")
	}
}

// Cancellation is answered while waiting, not only between attempts, which is
// where a link spends nearly all of an outage. The real sleep is used here: an
// hour of it, and the test takes no time at all.
func TestALinkStopsPromptlyWhileItIsWaiting(t *testing.T) {
	s := NewServer(Config{Store: NewMemoryStore()})
	t.Cleanup(func() { _ = s.Close(context.Background()) })

	waiting := make(chan struct{}, 1)
	link, err := s.reconnecting(
		func(context.Context) (Transport, error) { return nil, errors.New("no route") },
		"doc", 42,
		RetryPolicy{Wait: time.Hour, Ceiling: time.Hour, Notify: func(LinkStatus) {
			waiting <- struct{}{}
		}})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	ended := make(chan error, 1)
	go func() { ended <- link.run(ctx) }()

	<-waiting
	started := time.Now()
	cancel()
	select {
	case err := <-ended:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("the link ended with %v, want the cancellation", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the link sat out its hour rather than noticing it had been cancelled")
	}
	if took := time.Since(started); took > time.Second {
		t.Fatalf("the link took %v to notice it had been cancelled", took)
	}
}

// And the sleep returns of its own accord when nothing cancels it.
func TestTheSleepEndsWhenTheTimeIsUp(t *testing.T) {
	if err := waitFor(t.Context(), time.Millisecond); err != nil {
		t.Fatalf("waiting a millisecond reported %v", err)
	}
}

// What a reconnecting link refuses before it dials anything. These are the
// failures that would otherwise be retried forever: a loop cannot fix a
// document with no name by asking again.
func TestFollowWithRetryRefusesWhatItCannotDo(t *testing.T) {
	s := NewServer(Config{Store: NewMemoryStore()})
	t.Cleanup(func() { _ = s.Close(context.Background()) })

	dials := 0
	dial := func(context.Context) (Transport, error) {
		dials++
		return nil, errors.New("should never be reached")
	}

	for _, tt := range []struct {
		name     string
		dial     Dialer
		document string
		as       crdt.SiteID
		policy   RetryPolicy
	}{
		{"no dialler", nil, "doc", 42, RetryPolicy{}},
		{"no document name", dial, "", 42, RetryPolicy{}},
		{"the server's own replica", dial, "doc", serverSite, RetryPolicy{}},
		{"a negative wait", dial, "doc", 42, RetryPolicy{Wait: -time.Second}},
		{"a negative ceiling", dial, "doc", 42, RetryPolicy{Ceiling: -time.Second}},
		{"a wait longer than the ceiling", dial, "doc", 42, RetryPolicy{
			Wait: 2 * time.Minute, Ceiling: time.Minute,
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if err := s.FollowWithRetry(t.Context(), tt.dial, tt.document, tt.as, tt.policy); err == nil {
				t.Fatal("FollowWithRetry returned no error")
			}
		})
	}
	if dials != 0 {
		t.Fatalf("a link that could not be made dialled %d times", dials)
	}
}

// A policy that says an error is not worth another attempt ends the link, and
// hands back the error it was asked about — so a caller learns why it stopped
// rather than that it did.
func TestAPermanentFailureEndsTheLink(t *testing.T) {
	s := NewServer(Config{Store: NewMemoryStore()})
	t.Cleanup(func() { _ = s.Close(context.Background()) })

	t.Run("the second failure is judged permanent", func(t *testing.T) {
		refused := errors.New("the peer will not have this link")
		dials := 0
		asked := 0
		link, err := s.reconnecting(
			func(context.Context) (Transport, error) { dials++; return nil, refused },
			"doc", 42,
			RetryPolicy{Permanent: func(err error) bool {
				asked++
				return asked > 1 && errors.Is(err, refused)
			}})
		if err != nil {
			t.Fatal(err)
		}
		link.back.sleep = func(context.Context, time.Duration) error { return nil }

		if err := link.run(t.Context()); !errors.Is(err, refused) {
			t.Fatalf("the link ended with %v, want the refusal", err)
		}
		if dials != 2 {
			t.Fatalf("the link dialled %d times; it should have retried once and then stopped", dials)
		}
	})

	// A dialler that reports neither a transport nor a failure is a caller's
	// mistake, and it is named rather than left to panic on a goroutine nobody
	// owns.
	t.Run("a dialler that returns nothing at all", func(t *testing.T) {
		link, err := s.reconnecting(
			func(context.Context) (Transport, error) { return nil, nil },
			"doc", 42,
			RetryPolicy{Permanent: func(error) bool { return true }})
		if err != nil {
			t.Fatal(err)
		}
		err = link.run(t.Context())
		if err == nil {
			t.Fatal("a dialler that returned nothing was treated as a working link")
		}
		if got := err.Error(); got != "collab: the dialler returned no transport and no error" {
			t.Fatalf("the link reported %q", got)
		}
	})
}

// FollowWithRetry itself, rather than the loop underneath it, and the defaults
// a policy that says nothing gets.
func TestFollowWithRetryRunsTheLoopWithWorkingDefaults(t *testing.T) {
	s := NewServer(Config{Store: NewMemoryStore()})
	t.Cleanup(func() { _ = s.Close(context.Background()) })

	unreachable := errors.New("no route")
	err := s.FollowWithRetry(t.Context(),
		func(context.Context) (Transport, error) { return nil, unreachable },
		"doc", 42,
		RetryPolicy{Permanent: func(error) bool { return true }})
	if !errors.Is(err, unreachable) {
		t.Fatalf("FollowWithRetry returned %v", err)
	}

	// The zero policy is a working policy, which is the claim its documentation
	// makes.
	link, err := s.reconnecting(
		func(context.Context) (Transport, error) { return nil, unreachable },
		"doc", 42, RetryPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	if link.policy.Wait != DefaultRetryWait {
		t.Fatalf("a policy that said nothing waits %v", link.policy.Wait)
	}
	if link.policy.Ceiling != DefaultRetryCeiling {
		t.Fatalf("a policy that said nothing is capped at %v", link.policy.Ceiling)
	}
}
