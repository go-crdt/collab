package collab

import (
	"context"
	"testing"
	"time"
)

// promptWait is how long a test lets a call that should end at once go on for.
const promptWait = 5 * time.Second

// promptCtx is a context that ends after promptWait. A test hands it to a call
// whose guards keep it out of a wait or a retry loop: with a guard deleted, the
// call does not return an error, it waits for a context that never ends, and the
// suite is killed by go test's own timeout with no assertion to say why. Under
// this one the call returns promptWait later with the context's error, which a
// test can tell from the refusal it expected.
func promptCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), promptWait)
	t.Cleanup(cancel)
	return ctx
}
