//go:build !js

package collab

import (
	"context"
	"testing"
)

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
