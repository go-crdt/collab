//go:build !js

package collab

import (
	"context"
	"testing"
	"time"
)

// within runs f and returns its results, or fails the test and reports ok=false
// if f does not return within bcastSettle. It is for a call that must END on its
// own -- an election whose window runs out, a dial whose context ends -- where a
// mutated loop that ignores the context spins for ever and would otherwise hold
// the test until go test's own timeout, with no assertion to say which call.
//
// The call that did not return is left behind, spinning: waiting for it is
// what the failure path must not do, and the process ends with the test run.
func within[A, B any](t *testing.T, what string, f func() (A, B)) (a A, b B, ok bool) {
	t.Helper()
	type pair struct {
		a A
		b B
	}
	done := make(chan pair, 1)
	go func() { x, y := f(); done <- pair{x, y} }()
	select {
	case p := <-done:
		return p.a, p.b, true
	case <-time.After(bcastSettle):
		t.Errorf("%s did not return within %v: it ignores its context", what, bcastSettle)
		return a, b, false
	}
}

// closeWithin closes a client, and fails the test if that does not return within
// bcastSettle. A bus read that ignores its context -- which is what a mutated
// Recv or inbox is -- spins for ever, so the Close that waits for the reader
// would hold the test until go test's own timeout kills the binary with a panic
// and no assertion to say what was held. This reports it instead.
//
// A hung Close is left behind, deliberately: waiting for it again is what the
// failure path must not do.
func closeWithin(t *testing.T, c *Client) {
	t.Helper()
	done := make(chan struct{})
	go func() { _ = c.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(bcastSettle):
		t.Errorf("Close of the client for site %d did not return within %v: its reader ignores its context", c.Site(), bcastSettle)
	}
}

// bcastCtx is the context a bus test hands to a call that waits on the bus. It
// ends after bcastSettle, so a frame that never arrives -- which is what a
// mutated decoder, inbox or election produces -- fails the test it is in with the
// context's error, instead of leaving the call to wait until go test's own
// timeout kills the binary with no assertion to say what was held.
func bcastCtx(t *testing.T) context.Context {
	t.Helper()
	// Four settles, not one: some of these calls wait out an election window of
	// one settle, and a context ending at the same instant would end it early.
	ctx, cancel := context.WithTimeout(t.Context(), 4*bcastSettle)
	t.Cleanup(cancel)
	return ctx
}
