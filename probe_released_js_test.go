//go:build js && wasm

package collab

import (
	"syscall/js"
	"testing"
)

// ⛔⛔ THIS PINS A FACT THE LOCK TESTS DEPEND ON, and it exists because I wrote
// those tests on an assumption and the assumption was wrong.
//
// I had them observe a released callback by catching a JavaScript exception. On
// this toolchain there is none: calling a RELEASED js.Func from JavaScript
// returns `undefined`, silently — it neither throws nor panics. Measured below,
// both directions, with the live call as the control.
//
//	live call: returned 42
//	dead call: returned undefined
//
// So the discriminator is WHAT THE CALL RETURNS, not what it throws, and the
// lock tests use that. Without this probe the give-up test passed vacuously:
// "no exception was caught" is satisfied just as well by "exceptions are never
// caught here".
//
// ⚠ And it narrows a claim in #276, which I filed: a reviewer did observe
// `call to released function` printed beside a PASS, so that panic is real in
// some circumstances — a reused slot, or a different call path — but it is NOT
// what a plain JS call to a released func produces here. The defect #276
// describes stands; the symptom I expected to see does not appear this way.
//
// ⛔⛔ AND THIS CHANGES A WITNESS THE WHOLE PACKAGE RELIED ON, so it is said
// here where somebody grepping will find it. "Any occurrence of `call to
// released function` in the wasm output is a finding" was a usable alarm
// because the baseline was ZERO. This test and the lock-release tests now
// produce it BY DESIGN — syscall/js prints it to console.error on exactly the
// call this measures — so the baseline is no longer zero. A bare grep no
// longer discriminates; the question is now whether the COUNT exceeds what
// these deliberate cases produce, and a reviewer who greps without knowing
// that will read a clean run as dirty, or a dirty one as expected.
//
// ⚠ Nothing in CI greps for it today (checked: no .github/workflows file
// mentions the string), so this degrades a manual practice rather than a gate.
// Said plainly because a witness that quietly stops discriminating is worse
// than one that was never there.
func TestCallingAReleasedFuncReturnsUndefinedRatherThanThrowing(t *testing.T) {
	g := js.Global()
	fn := js.FuncOf(func(js.Value, []js.Value) any { return 42 })
	g.Set("__probeFn", fn)
	// ⛔ __probeCall too: a review measured that it survived the cleanup
	// (`defined=true` afterwards). A test that leaves a global behind is a test
	// the next one inherits.
	t.Cleanup(func() {
		g.Call("eval", `delete globalThis.__probeFn; delete globalThis.__probe; delete globalThis.__probeCall`)
	})

	g.Call("eval", `globalThis.__probe = {live: null, dead: null};
globalThis.__probeCall = function (slot) {
  try { globalThis.__probe[slot] = "returned " + String(globalThis.__probeFn()); }
  catch (e) { globalThis.__probe[slot] = "threw " + String(e); }
};`)

	g.Call("__probeCall", "live")
	fn.Release()
	g.Call("__probeCall", "dead")

	p := g.Get("__probe")
	t.Logf("live call: %s", p.Get("live").String())
	t.Logf("dead call: %s", p.Get("dead").String())

	if !p.Get("live").Truthy() || !p.Get("dead").Truthy() {
		t.Fatal("the probe recorded nothing, so it measured nothing")
	}
	if got := p.Get("live").String(); got != "returned 42" {
		t.Errorf("the control: a LIVE call should return the body's value, got %q", got)
	}
	if got := p.Get("dead").String(); got != "returned undefined" {
		t.Errorf("a released call returned %q; the lock tests' discriminator assumes undefined", got)
	}
}
