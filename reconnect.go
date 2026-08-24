//go:build !js

package collab

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/go-crdt/crdt"
)

// A Dialer produces a route to the peer, one per attempt.
//
// It is a function rather than a [Transport] because a [Transport] is a way to
// reach a peer and an attempt needs a fresh one: a gRPC client connection whose
// server has gone stays broken, a WebSocket that closed cannot be reopened, and
// a link that held on to the first one would redial nothing. What is stable
// across attempts is the knowledge of where the peer is and how to authenticate
// to it, and that is what a closure holds.
//
// It is called on the link's own goroutine, once per attempt, with the context
// the link was given, so a dialler that blocks blocks the retry loop and a
// dialler that respects ctx is what makes cancellation prompt during a dial.
// Returning a transport it has already returned is allowed where the transport
// itself redials, as [WebSocket] does.
type Dialer func(ctx context.Context) (Transport, error)

// DefaultRetryWait is how long a link waits before its first attempt at coming
// back, when [RetryPolicy.Wait] does not say. It is short because most drops
// are brief — a process restarted, a route reconverging — and a link that is
// back within a second has lost nothing anybody typed.
const DefaultRetryWait = 250 * time.Millisecond

// DefaultRetryCeiling is the longest a link waits between attempts, when
// [RetryPolicy.Ceiling] does not say. It bounds two things at once: how much
// work a peer that has been down for a day is asked to do, and how stale a
// replica can be once that peer comes back, since nothing crosses the link
// until the next attempt. Half a minute is the compromise, and an operator who
// knows their outages can say better.
const DefaultRetryCeiling = 30 * time.Second

// A RetryPolicy is how a link comes back, which [Server.Follow] says is the
// operator's to decide. This is where they decide it.
//
// The zero value is a working policy: [DefaultRetryWait] doubling to
// [DefaultRetryCeiling], jittered, retrying everything, telling nobody.
type RetryPolicy struct {
	// Wait is how long to wait before the first attempt after a link drops.
	// Each further failure doubles it, up to Ceiling. Defaults to
	// [DefaultRetryWait]; it may not be negative, and may not exceed Ceiling.
	Wait time.Duration

	// Ceiling is the longest the link will ever wait between attempts.
	// Defaults to [DefaultRetryCeiling]; it may not be negative.
	Ceiling time.Duration

	// Permanent, when set, is asked about the error that ended each attempt and
	// stops the link by returning true — the error it was asked about is then
	// what [Server.FollowWithRetry] returns.
	//
	// It exists because this package cannot answer the question honestly for
	// everybody. The errors that are genuinely permanent are refused before the
	// loop is ever entered, and everything that can then end an attempt came
	// off a network or off a peer, where "permanent" is a judgement about a
	// deployment and not about an error value. A peer refusing the link is the
	// case that decides the shape: it is a policy decision, policy is edited
	// and credentials are rotated, so a link that gave up on it would need
	// somebody to notice and restart a process — while a link that keeps asking
	// costs one attempt per Ceiling, which is nothing. So the default is to
	// retry it, and an operator who disagrees says so here, with
	// [errors.Is] over whatever their peer returns.
	Permanent func(error) bool

	// Notify, when set, is told every time the link changes state: down, with
	// why and for how long, and up again. See [LinkStatus].
	//
	// A library has no business choosing where that goes. Writing to stdout
	// would put a federation link's troubles into the middle of whatever the
	// process's own output is, in a format nobody asked for, and a link that
	// says nothing at all is a link nobody can operate — an outage would be
	// visible only as a replica that had quietly stopped converging. So it is
	// handed over instead, and the operator's logger, metric or health check
	// decides.
	//
	// It is called on the link's own goroutine, in order, and blocking in it
	// blocks the link — including the moment it is trying to come back. It must
	// not call back into the link.
	Notify func(LinkStatus)
}

// A LinkStatus is what a reconnecting link tells [RetryPolicy.Notify]: whether
// it is up, and if it is not, why, since when and for how much longer.
//
// It answers the two questions an operator has about a link that is not
// working — is it down, and how long has it been down — without their having to
// keep the state themselves, because the obvious mistake is to report only the
// failure and leave "still failing" indistinguishable from "failed once".
type LinkStatus struct {
	// Up is true for the one report that says the link is established and the
	// local replica has been caught up. The rest of the fields are zero.
	Up bool

	// Err is why the attempt ended, for a report that is not Up.
	Err error

	// Attempt counts the consecutive failures since the link was last up, so
	// the first report of an outage carries 1. It is what tells a second
	// failure from a hundredth.
	Attempt int

	// RetryIn is how long the link will wait before trying again — the jitter
	// already applied, so it is the real interval and not the policy's idea of
	// it.
	RetryIn time.Duration

	// DownFor is how long this outage has lasted: zero on the first report,
	// growing with each one. It is the number a health check thresholds on,
	// because "down for six seconds" and "down for six hours" want different
	// people woken up.
	DownFor time.Duration
}

// FollowWithRetry follows document on the peer dial reaches, exactly as
// [Server.Follow] does, and re-establishes the link when it drops — waiting
// longer after each failure, never longer than the policy's ceiling, and never
// the same interval as anybody else.
//
// It returns when ctx is cancelled, returning ctx's error, or when
// [RetryPolicy.Permanent] says an attempt's failure is not worth another,
// returning that failure. It returns immediately, without dialling, if the call
// itself is wrong: no dialler, no document name, a link claiming the server's
// own replica, or a policy that waits for a negative time or longer than its
// own ceiling.
//
// # Why this is here rather than in every caller
//
// [Server.Follow]'s reasoning stands: the policy belongs to the operator. What
// does not follow from it is leaving everyone to write the loop, because it is
// the same loop every time and it is usually written twice wrong. Without
// jitter, every link in a datacentre that lost the same peer waits the same
// interval and returns together, so the peer coming back up meets the whole
// fleet at once and goes down again — the retry itself becomes the outage.
// Without a ceiling, doubling either arrives somewhere absurd or, more often,
// is bounded by an attempt counter that gives up, and a federation link that
// gives up is a replica that stops converging while the process it lives in
// carries on looking healthy.
//
// So the loop is written here, once, and nothing acquires it by accident.
// [Server.Follow] is unchanged and remains what a caller with a different
// answer builds on.
//
// # What is retried, and what is not
//
// Everything the loop can see, and that is a deliberate line rather than a
// shrug. The two failures that are genuinely permanent — a document with no
// name, and a link claiming [serverSite] — are decided once, before the loop is
// entered, so they are returned to the caller rather than re-asked forever.
// After that, an attempt can only end because a dialler could not reach the
// peer, because a carrier broke, because the peer refused the link or spoke
// something unexpected, or because ctx ended. The first two are what a link
// between datacentres does on a normal day. The third is a deployment's
// business, not this package's, which is what [RetryPolicy.Permanent] is for.
// The last ends the loop rather than being retried.
//
// # Jitter
//
// The delay is drawn uniformly from the half-open band between half the current
// interval and the whole of it, rather than from zero to it: full jitter
// decorrelates best but can draw a delay near zero many times running, which is
// the hammering this exists to avoid, while a floor of half the interval bounds
// the attempt rate and still leaves no two links in step.
//
// It is not a knob. An operator has something to say about how long to wait and
// how stale they will tolerate, and nothing to say about a jitter fraction —
// but offered the field they would be able to set it to zero, which is the
// single mistake this whole function exists to prevent.
func (s *Server) FollowWithRetry(ctx context.Context, dial Dialer, document string, as crdt.SiteID, policy RetryPolicy) error {
	link, err := s.reconnecting(dial, document, as, policy)
	if err != nil {
		return err
	}
	return link.run(ctx)
}

// A reconnectingLink is one link and the loop that keeps it up.
//
// sleep, random and now are the timing, held here rather than called directly,
// because a test of a backoff that actually waits for the backoff is a test
// nobody runs twice: the interesting policy is minutes long, and asserting it
// truthfully would cost minutes. With the three in fields a test can drive the
// loop through a day of outage in no time at all and assert the sequence of
// delays exactly — which is the property, where "it eventually retried" is not.
// They are set by the constructor and never afterwards, and only a test in this
// package can reach them, so they are a seam and not an API.
type reconnectingLink struct {
	server   *Server
	dial     Dialer
	document string
	as       crdt.SiteID
	policy   RetryPolicy

	sleep  func(ctx context.Context, d time.Duration) error
	random func() float64
	now    func() time.Time
}

// reconnecting checks what cannot be retried and builds the link.
func (s *Server) reconnecting(dial Dialer, document string, as crdt.SiteID, policy RetryPolicy) (*reconnectingLink, error) {
	if dial == nil {
		return nil, errors.New("collab: FollowWithRetry needs a dialler")
	}
	if err := followable(document, as); err != nil {
		return nil, err
	}
	if policy.Wait < 0 || policy.Ceiling < 0 {
		return nil, fmt.Errorf("collab: a retry policy cannot wait for a negative time (wait %v, ceiling %v)", policy.Wait, policy.Ceiling)
	}
	if policy.Wait == 0 {
		policy.Wait = DefaultRetryWait
	}
	if policy.Ceiling == 0 {
		policy.Ceiling = DefaultRetryCeiling
	}
	// Refused rather than clamped: a first wait longer than the ceiling is
	// somebody who has misread one of the two fields, and silently doing what
	// the shorter one says would hide that from them for as long as the link
	// runs.
	if policy.Wait > policy.Ceiling {
		return nil, fmt.Errorf("collab: a retry policy cannot start at %v and be capped at %v", policy.Wait, policy.Ceiling)
	}
	return &reconnectingLink{
		server:   s,
		dial:     dial,
		document: document,
		as:       as,
		policy:   policy,
		sleep:    waitFor,
		random:   rand.Float64,
		now:      time.Now,
	}, nil
}

// run is the loop.
func (l *reconnectingLink) run(ctx context.Context) error {
	wait := l.policy.Wait
	attempt := 0
	// downSince is when the current outage began, and being zero is how the
	// loop knows there is not one: a link that has never been up is down from
	// its first failure, not from the moment it started.
	var downSince time.Time

	for {
		err := l.attempt(ctx, func() {
			// Only a session that was really established resets the backoff.
			wait, attempt, downSince = l.policy.Wait, 0, time.Time{}
			l.notify(LinkStatus{Up: true})
		})

		// Cancellation first, and before the policy is consulted: a link torn
		// down on purpose has not failed, and the error the attempt happened to
		// return on its way out says less than the reason it was told to stop.
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if l.policy.Permanent != nil && l.policy.Permanent(err) {
			return err
		}

		if downSince.IsZero() {
			downSince = l.now()
		}
		attempt++
		in := l.jittered(wait)
		l.notify(LinkStatus{Err: err, Attempt: attempt, RetryIn: in, DownFor: l.now().Sub(downSince)})

		// Waiting is where a link spends nearly all of an outage, so this is
		// where cancellation has to be answered rather than at the top of the
		// loop: a process shutting down must not have to sit out a ceiling.
		if err := l.sleep(ctx, in); err != nil {
			return err
		}
		wait = min(2*wait, l.policy.Ceiling)
	}
}

// attempt is one dial and one session.
func (l *reconnectingLink) attempt(ctx context.Context, established func()) error {
	peer, err := l.dial(ctx)
	if err != nil {
		return err
	}
	// A dialler that reports neither a transport nor a failure would otherwise
	// panic inside this package, on a goroutine of ours, and take the process
	// with it. It is the caller's mistake, so it is reported as one.
	if peer == nil {
		return errors.New("collab: the dialler returned no transport and no error")
	}
	return l.server.follow(ctx, peer, l.document, l.as, established)
}

// jittered draws the real delay for an interval: somewhere in the half-open
// band between half of it and all of it. See FollowWithRetry on why the band is
// that one and why it is not configurable.
func (l *reconnectingLink) jittered(d time.Duration) time.Duration {
	half := d / 2
	return half + time.Duration(l.random()*float64(half))
}

// notify tells the operator, if they asked to be told.
func (l *reconnectingLink) notify(status LinkStatus) {
	if l.policy.Notify != nil {
		l.policy.Notify(status)
	}
}

// waitFor is the real sleep: d, or ctx ending, whichever comes first. A bare
// time.Sleep here would be a link that cannot be shut down for as long as a
// ceiling.
func waitFor(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
