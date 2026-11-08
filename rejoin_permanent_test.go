//go:build !js

package collab

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// unreachable serves one session and then refuses every attempt to replace it
// with an error of its own.
//
// The distinction from [breakable] is the whole reason it exists: breakable
// refuses a redial with ErrPipeClosed, which is also what ending its session
// looks like, so a policy asked about one cannot be told apart from a policy
// asked about the other. Here the session ends one way and the attempts fail
// another, which is the shape the real cases have — an endpoint that has been
// decommissioned, a credential that has been revoked, a peer that answers
// something this build cannot read.
//
// Where it fails is chosen, because the rejoin loop has two places that can:
// the dial, and the open inside [Client.attach].
type unreachable struct {
	srv    *Server
	ctx    context.Context
	gone   error
	atOpen bool // fail in attach's open rather than in the dial
	// onRefuse runs just before a refusal is returned, which is how a test
	// arranges something to happen DURING an attempt rather than between two.
	// Set it before the client is built; it is read on the supervisor's
	// goroutine.
	onRefuse func()

	mu       sync.Mutex
	conns    []carrierConn
	refusals int
	off      bool
}

func (v *unreachable) dial(context.Context) (Transport, error) {
	v.mu.Lock()
	refuse := v.off && !v.atOpen
	if refuse {
		v.refusals++
	}
	v.mu.Unlock()
	if refuse {
		if v.onRefuse != nil {
			v.onRefuse()
		}
		return nil, v.gone
	}
	return v, nil
}

func (v *unreachable) open(ctx context.Context) (carrierConn, error) {
	v.mu.Lock()
	refuse := v.off && v.atOpen
	if refuse {
		v.refusals++
	}
	v.mu.Unlock()
	if refuse {
		return nil, v.gone
	}
	transport, server := Pipe()
	go func() { _ = v.srv.ServePipe(v.ctx, server) }()
	conn, err := transport.open(ctx)
	if err != nil {
		return nil, err
	}
	v.mu.Lock()
	v.conns = append(v.conns, conn)
	v.mu.Unlock()
	return conn, nil
}

// vanish cuts the session that exists and refuses the ones that would replace
// it. The session therefore ends with the carrier breaking, and every attempt
// after that ends with gone — two different errors, which is the point.
func (v *unreachable) vanish() {
	v.mu.Lock()
	v.off = true
	conns := v.conns
	v.conns = nil
	v.mu.Unlock()
	for _, c := range conns {
		_ = c.Close()
	}
}

// reappear lets the attempts succeed again.
func (v *unreachable) reappear() {
	v.mu.Lock()
	v.off = false
	v.mu.Unlock()
}

func (v *unreachable) refused() int {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.refusals
}

// A policy that calls a failed rejoin permanent stops the client, and the
// client says why it stopped.
//
// Before this, RetryPolicy.Permanent was asked only about the error that ended
// the session and never about an error that ended an attempt to replace it —
// so a peer that was never coming back was retried for ever and no operator
// policy could stop it. The documentation on Permanent said the opposite ("is
// asked about the error that ended each attempt"), and Server.FollowWithRetry
// had always done what it said; this loop had not.
func TestPermanentIsAskedAboutAFailedAttemptAndStopsTheClient(t *testing.T) {
	for _, tc := range []struct {
		name   string
		atOpen bool
	}{
		{"a dial that is refused", false},
		{"a rejoin the peer will not accept", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			srv := NewServer(Config{Store: NewMemoryStore()})
			defer func() { _ = srv.Close(context.Background()) }()

			gone := errors.New("this endpoint is not coming back")
			link := &unreachable{srv: srv, ctx: ctx, gone: gone, atOpen: tc.atOpen}

			var mu sync.Mutex
			var asked []error
			c, err := JoinWithRetry(ctx, link.dial, ClientConfig{Document: "d", Site: 1}, RetryPolicy{
				Wait:    time.Millisecond,
				Ceiling: 2 * time.Millisecond,
				Permanent: func(err error) bool {
					mu.Lock()
					asked = append(asked, err)
					mu.Unlock()
					return errors.Is(err, gone)
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = c.Close() }()

			link.vanish()

			select {
			case <-c.Done():
			case <-time.After(15 * time.Second):
				t.Fatal("the policy called the failure permanent and the client kept retrying anyway")
			}

			// Err names the reason it gave up, not the carrier that broke a
			// moment earlier. Those are the same error on the first attempt and
			// different ones after it, and this is the case where they differ.
			if !errors.Is(c.Err(), gone) {
				t.Errorf("Err() is %v; it gave up because of %v", c.Err(), gone)
			}

			// And the policy was asked about the attempt at all, which is the
			// defect itself: the session's own error is not enough, because a
			// policy that stopped on that would stop on every ordinary outage.
			mu.Lock()
			defer mu.Unlock()
			for _, err := range asked {
				if errors.Is(err, gone) {
					return
				}
			}
			t.Errorf("the policy was asked about %v and never about the failed attempt", asked)
		})
	}
}

// Notify is told why the attempt that just failed failed, not why the session
// ended several attempts ago.
//
// The same discarded error caused this: a failed attempt went back to the top
// of the loop, read the dead session's error again, and handed that to the
// backoff — so an operator watching LinkStatus saw a rising Attempt count
// against an error that had stopped being the reason by the second one.
func TestNotifyNamesTheAttemptThatFailedNotTheSessionThatEnded(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	srv := NewServer(Config{Store: NewMemoryStore()})
	defer func() { _ = srv.Close(context.Background()) }()

	gone := errors.New("this endpoint is not coming back")
	link := &unreachable{srv: srv, ctx: ctx, gone: gone}

	var mu sync.Mutex
	var told []error
	c, err := JoinWithRetry(ctx, link.dial, ClientConfig{Document: "d", Site: 1}, RetryPolicy{
		Wait:    time.Millisecond,
		Ceiling: 2 * time.Millisecond,
		Notify: func(s LinkStatus) {
			if s.Up {
				return
			}
			mu.Lock()
			told = append(told, s.Err)
			mu.Unlock()
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	link.vanish()

	// No policy here, so it retries for ever: what is being measured is what it
	// says while it does.
	waitFor2(t, "the operator to be told about the refused dial", func() bool {
		mu.Lock()
		defer mu.Unlock()
		for _, err := range told {
			if errors.Is(err, gone) {
				return true
			}
		}
		return false
	})

	// The first report is still the session, which is right: that is what ended
	// the attempt that was running.
	mu.Lock()
	defer mu.Unlock()
	if len(told) < 2 {
		t.Fatalf("only %d reports; the test cannot say which error belongs to which attempt", len(told))
	}
	if errors.Is(told[0], gone) {
		t.Errorf("the first report blames the dial, but what ended that attempt was the session: %v", told[0])
	}
}

// A policy that does not call it permanent keeps the client retrying, and the
// participant comes back when the peer does.
//
// This is the half the fix must not break: being asked about an attempt's error
// is not the same as giving up on it, and the default — no policy at all, or
// one that says false — is still to retry everything for ever.
func TestAnAttemptErrorTheCallerDoesNotCallPermanentKeepsRetrying(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	srv := NewServer(Config{Store: NewMemoryStore()})
	defer func() { _ = srv.Close(context.Background()) }()

	gone := errors.New("this endpoint is busy")
	link := &unreachable{srv: srv, ctx: ctx, gone: gone}

	c, err := JoinWithRetry(ctx, link.dial, ClientConfig{Document: "d", Site: 1}, RetryPolicy{
		Wait:      time.Millisecond,
		Ceiling:   2 * time.Millisecond,
		Permanent: func(error) bool { return false },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	link.vanish()
	waitFor2(t, "several attempts to be refused", func() bool { return link.refused() >= 3 })

	select {
	case <-c.Done():
		t.Fatalf("the client gave up on an error the policy did not call permanent: %v", c.Err())
	default:
	}

	link.reappear()
	waitFor2(t, "the participant to be back on the air", func() bool {
		body, err := c.Text("body")
		if err != nil {
			return false
		}
		return body.Insert(0, "back") == nil && c.Err() == nil
	})
}

// A context that ended during an attempt is a shutdown, and the policy is not
// asked about it.
//
// This is why the attempt loop answers cancellation at its top instead of
// leaving it to the backoff, and reconnect.go's run() gives the reason at the
// same point: "a link torn down on purpose has not failed, and the error the
// attempt happened to return on its way out says less than the reason it was
// told to stop". Without the check, the error a cancelled dial returns would be
// handed to Permanent as though the peer had refused.
//
// It is the only way to reach that check — the context has to end after the
// backoff's wait has already finished and before the next pass begins, which is
// to say DURING the dial — so the fake cancels it itself rather than a second
// goroutine racing to. A gate that is satisfied by a race is satisfied by
// luck.
func TestAContextThatEndedDuringAnAttemptIsAShutdownNotAFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	srv := NewServer(Config{Store: NewMemoryStore()})
	defer func() { _ = srv.Close(context.Background()) }()

	interrupted := errors.New("the dial was interrupted")
	link := &unreachable{srv: srv, ctx: ctx, gone: interrupted}
	// The refusal cancels the context on its way out, so the loop arrives at
	// the top of the next pass with the context already finished.
	link.onRefuse = cancel

	var mu sync.Mutex
	var asked []error
	c, err := JoinWithRetry(ctx, link.dial, ClientConfig{Document: "d", Site: 1}, RetryPolicy{
		Wait:    time.Millisecond,
		Ceiling: 2 * time.Millisecond,
		Permanent: func(err error) bool {
			mu.Lock()
			asked = append(asked, err)
			mu.Unlock()
			return false
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	link.vanish()

	select {
	case <-c.Done():
	case <-time.After(15 * time.Second):
		t.Fatal("the context ended during an attempt and the client did not stop")
	}

	// Err is not asserted here. fail() is first-writer-wins and the session's
	// own error is already recorded, so a client torn down between attempts
	// reports why the session ended — which is this loop's behaviour before
	// this change and after it, and not what this test is about.
	mu.Lock()
	defer mu.Unlock()
	for _, err := range asked {
		if errors.Is(err, interrupted) {
			t.Errorf("the policy was asked about %v, which was really a shutdown", err)
		}
	}
}

// The error a failed Recv reports reaches the policy as itself, and not as the
// ErrProtocol the next few lines would have produced without it.
//
// This closes the loop on how #247 was found. The check on Recv's error in
// Client.attach survived a mutation sweep, and the reason it survived was that
// deleting it only swapped one error for another on a path where the caller
// threw the error away: the carrier closed either way, the loop retried either
// way, and nothing could observe the difference. Now something can — the
// policy — so the line is load-bearing and a sweep can say so.
//
// Measured with go-fleettools' mutate against the unmutated baseline, which is
// the only thing that makes a verdict mean anything.
func TestARecvThatFailedReachesThePolicyAsItself(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	srv := NewServer(Config{Store: NewMemoryStore()})
	defer func() { _ = srv.Close(context.Background()) }()

	link := &awkward{srv: srv, ctx: ctx, failAt: "recv"}

	var mu sync.Mutex
	var asked []error
	c, err := JoinWithRetry(ctx, link.dial, ClientConfig{Document: "d", Site: 1}, RetryPolicy{
		Wait:    time.Millisecond,
		Ceiling: 2 * time.Millisecond,
		Permanent: func(err error) bool {
			mu.Lock()
			asked = append(asked, err)
			mu.Unlock()
			// This error and no other. ErrProtocol must not do.
			return errors.Is(err, errNothingCameBack)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	link.cut()

	select {
	case <-c.Done():
	case <-time.After(15 * time.Second):
		t.Fatal("the policy never saw the Recv error itself, only something else")
	}
	if !errors.Is(c.Err(), errNothingCameBack) {
		t.Errorf("Err() is %v; the rejoin failed with %v", c.Err(), errNothingCameBack)
	}

	mu.Lock()
	defer mu.Unlock()
	for _, err := range asked {
		if errors.Is(err, ErrProtocol) {
			t.Errorf("the policy was told ErrProtocol; a Recv that failed is not a peer talking nonsense")
		}
	}
}

// stopped has the last word over the error that ended the session, and no word
// at all over a deliberate close.
//
// Err's documentation promises that "a session that was closed deliberately
// reports ErrClosed", and a close racing the moment the policy gives up must
// not cost the caller that promise. The race is not reproducible on demand, so
// the rule is tested where it is written.
func TestStoppedReplacesTheSessionErrorButNeverADeliberateClose(t *testing.T) {
	gone := errors.New("this endpoint is not coming back")

	retrying := &Client{err: ErrPipeClosed}
	retrying.stopped(gone)
	if !errors.Is(retrying.Err(), gone) {
		t.Errorf("a client that gave up reports %v; it gave up because of %v", retrying.Err(), gone)
	}

	closed := &Client{err: ErrClosed}
	closed.stopped(gone)
	if !errors.Is(closed.Err(), ErrClosed) {
		t.Errorf("a client closed deliberately reports %v, not ErrClosed", closed.Err())
	}
}
