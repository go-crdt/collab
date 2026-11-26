package collab

import "testing"

// These tests exist so that deleting a guard FAILS A TEST rather than killing the
// test binary. The guards held here sit in front of a nil dereference, so a
// mutant that removes one panics, and a panic ends the whole run: nothing after
// it is reported, and mutsweep cannot tell "an assertion caught this" from "the
// process died". Every scenario below runs under noPanic, so the panic becomes a
// t.Errorf, and the file sorts first so that its failure is printed before any
// later test can take the process down.

// noPanic runs f and turns a panic into a failure of t.
func noPanic(t *testing.T, what string, f func()) {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("%s panicked: %v", what, r)
		}
	}()
	f()
}
