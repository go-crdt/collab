//go:build !js

package collab

import (
	"context"
	"fmt"
	"math/rand/v2"
	"time"
)

// checked fills in a policy's defaults and refuses one that cannot be honoured.
func (p RetryPolicy) checked() (RetryPolicy, error) {
	if p.Wait < 0 || p.Ceiling < 0 {
		return p, fmt.Errorf("collab: a retry policy cannot wait for a negative time (wait %v, ceiling %v)", p.Wait, p.Ceiling)
	}
	if p.Wait == 0 {
		p.Wait = DefaultRetryWait
	}
	if p.Ceiling == 0 {
		p.Ceiling = DefaultRetryCeiling
	}
	// Refused rather than clamped: a first wait longer than the ceiling is
	// somebody who has misread one of the two fields, and silently doing what
	// the shorter one says would hide that from them for as long as it runs.
	if p.Wait > p.Ceiling {
		return p, fmt.Errorf("collab: a retry policy cannot start at %v and be capped at %v", p.Wait, p.Ceiling)
	}
	return p, nil
}

// A backoff is the waiting done between attempts, and the counting that makes
// a [LinkStatus] worth reading.
//
// There is one of these because there are two things here that reconnect — a
// link between servers and a participant in a document — and they wait in
// exactly the same way. Two copies of a backoff would be two places for a
// ceiling to stop being honoured, and only one of them would have a test.
//
// Its clock, its sleep and its jitter are fields so that a test can assert the
// sequence of delays without waiting out any of them.
type backoff struct {
	policy RetryPolicy
	sleep  func(ctx context.Context, d time.Duration) error
	random func() float64
	now    func() time.Time

	wait    time.Duration
	attempt int
	// downSince is when the current outage began, and being zero is how it
	// knows there is not one: something that has never been up is down from its
	// first failure, not from the moment it started.
	downSince time.Time
}

func newBackoff(policy RetryPolicy) *backoff {
	return &backoff{
		policy: policy,
		sleep:  waitFor,
		random: rand.Float64,
		now:    time.Now,
		wait:   policy.Wait,
	}
}

// up records that a session was really established, which is the only thing
// that resets the waiting.
//
// Not "the attempt returned": a peer that accepts a session and drops it in the
// same breath would reset it every time, which is a hot loop written by
// somebody who thought they had written a backoff.
func (b *backoff) up() {
	b.wait, b.attempt, b.downSince = b.policy.Wait, 0, time.Time{}
	b.tell(LinkStatus{Up: true})
}

// down records a failed attempt, says so, and waits. What it returns is what
// stopped the wait, which is nil unless the context ended.
func (b *backoff) down(ctx context.Context, err error) error {
	if b.downSince.IsZero() {
		b.downSince = b.now()
	}
	b.attempt++
	in := b.jittered(b.wait)
	b.tell(LinkStatus{Err: err, Attempt: b.attempt, RetryIn: in, DownFor: b.now().Sub(b.downSince)})

	// Waiting is where nearly all of an outage is spent, so this is where
	// cancellation has to be answered rather than at the top of a loop: a
	// process shutting down must not have to sit out a ceiling.
	if err := b.sleep(ctx, in); err != nil {
		return err
	}
	b.wait = min(2*b.wait, b.policy.Ceiling)
	return nil
}

// jittered draws the real delay for an interval: somewhere in the half-open
// band between half of it and all of it.
//
// Without it everything that dropped together comes back together, and hammers
// the peer it is waiting for hardest at the moment that peer is least able to
// take it.
func (b *backoff) jittered(d time.Duration) time.Duration {
	half := d / 2
	return half + time.Duration(b.random()*float64(half))
}

// tell tells the operator, if they asked to be told.
func (b *backoff) tell(status LinkStatus) {
	if b.policy.Notify != nil {
		b.policy.Notify(status)
	}
}
