//go:build js && wasm

package collab

import (
	"context"
	"errors"
	"strings"
	"syscall/js"
	"testing"
	"time"
)

// ⛔⛔ #276, AND IT IS #270's CLASS IN THE FUNCTION #274 NEVER REACHED.
// electByLock's give-up branch called cb.Release() while navigator.locks.request
// was still OUTSTANDING, so the browser could invoke a callback this tab had
// already freed.
//
// ⛔ THE WITNESS IS WHAT THE CALL RETURNS, and the first version of these tests
// got that wrong: it caught a JavaScript exception, and there is none.
// TestCallingAReleasedFuncReturnsUndefinedRatherThanThrowing measures it —
// a released js.Func called from JS returns `undefined`, silently. So the
// give-up test below passed VACUOUSLY until the discriminator changed: "no
// exception caught" is satisfied just as well by "exceptions are never thrown
// here".
//
// The live callback, handed a truthy lock, returns a PROMISE (it takes the
// RoleHost path and hands the holding promise back). Released, the same call
// returns undefined. That is the difference the tests read.
//
// ⚠ A control a reviewer of #275 paid a round for, repeated because it would
// hollow these tests out: node HAS its own navigator.locks, so a fake assigned
// with `navigator.locks = …` can be ignored entirely. The property lives on the
// prototype, so it is shadowed with Object.defineProperty, and every test here
// ASSERTS that the fake's own counter moved before trusting anything else.
const lockFakeHoldsTheCallback = `
globalThis.__lockFake = {requests: 0, lastReturn: "none", invoke: null, settle: null};
Object.defineProperty(navigator, "locks", {
  configurable: true,
  value: {
    request(name, opts, cb) {
      globalThis.__lockFake.requests++;
      // The browser keeps the callback and does NOT call it yet: that is the
      // state the give-up branch runs in. invoke() calls it later with a
      // truthy lock, which is exactly what a real lock manager may do after a
      // page has stopped waiting.
      globalThis.__lockFake.invoke = function () {
        const r = cb({name: name});
        globalThis.__lockFake.lastReturn = (r === undefined) ? "undefined" : typeof r;
      };
      return new Promise((resolve) => { globalThis.__lockFake.settle = resolve; });
    },
  },
});
`

func installLockFake(t *testing.T) js.Value {
	t.Helper()
	g := js.Global()
	g.Call("eval", lockFakeHoldsTheCallback)
	t.Cleanup(func() { g.Call("eval", `delete navigator.locks; delete globalThis.__lockFake`) })
	return g.Get("__lockFake")
}

// ⛔⛔ THE CONTEXT EXPIRES DURING THE WAIT, it is not already over at entry —
// and that distinction is a defect these tests found in each other.
// electByLock now refuses a finished context BEFORE asking the browser (a
// cancelled caller used to have a coin's chance of being handed a role, because
// both cases of its select were ready and Go picks at random). So a test that
// hands it a cancelled context never reaches the give-up branch at all: the
// fake is asked zero times, which is exactly what each test's request counter
// said when this landed.
//
// A short deadline reaches it deterministically instead: the request goes out,
// the fake never calls the callback, so `decided` is never ready and the
// select has exactly ONE ready case when the deadline passes.
func TestGivingUpOnTheLockDoesNotReleaseACallbackTheBrowserStillHolds(t *testing.T) {
	fake := installLockFake(t)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	role, ok := electByLock(ctx, "room:gaveup")
	if ok {
		t.Fatalf("a cancelled election must not claim to have decided, got %v", role)
	}

	// ⛔ THE CONTROL, asserted before anything else: the fake was actually
	// asked. Without it, every assertion below is satisfied by a request that
	// never happened — node's own navigator.locks answering instead of this one.
	if n := fake.Get("requests").Int(); n != 1 {
		t.Fatalf("the fake was asked %d time(s), want 1: this test would otherwise pass blind", n)
	}

	// Now the browser does what it is allowed to do: call the callback after
	// this tab stopped waiting. A LIVE callback returns the holding promise.
	fake.Call("invoke")
	if got := fake.Get("lastReturn").String(); got != "object" {
		t.Errorf("the browser's call returned %q, so the callback had already been released while the request was outstanding", got)
	}
}

// ⛔ And the release is not simply skipped: it happens once the browser is
// finished with the request. Without this, the fix above would trade a panic
// for a leak of one js.Func per abandoned election, and nothing would say which.
func TestTheCallbackIsReleasedOnceTheLockRequestSettles(t *testing.T) {
	fake := installLockFake(t)

	// A deadline that passes during the wait, for the reason the test above
	// states: a context already finished at entry is refused before the browser
	// is asked at all.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, ok := electByLock(ctx, "room:settles"); ok {
		t.Fatal("an abandoned election must not claim to have decided")
	}
	if n := fake.Get("requests").Int(); n != 1 {
		t.Fatalf("the fake was asked %d time(s), want 1", n)
	}

	// ⛔⛔ THE LIVE CONTROL, WHICH THIS TEST DID NOT HAVE. A review measured the
	// hole: "undefined" is the answer for a RELEASED callback and ALSO for a
	// live one that takes the RoleClient branch, which returns js.Undefined().
	// So without this invoke, finding "undefined" after settling proved nothing
	// about the release. The fake hands the callback a TRUTHY lock, so a live
	// one takes the RoleHost branch and returns the holding promise — an
	// "object" — and that is the difference the assertion below reads.
	//
	// ⚠ Invoking twice is safe only since the sends to `decided` became
	// non-blocking. Before that, the first version of this test polled in a
	// loop and the whole SUITE hit its timeout: the channel is buffered one and
	// nothing reads it after the give-up branch, so the second callback blocked
	// for ever inside a JavaScript callback and the event loop died. The fix to
	// that wedge is what makes this control possible.
	fake.Call("invoke")
	if got := fake.Get("lastReturn").String(); got != "object" {
		t.Fatalf("the control: before settling the callback must still be live, got %q", got)
	}

	fake.Call("settle", js.Undefined())
	// The release is handed to a goroutine woken by the settle handler, so this
	// yields several turns rather than assuming one is enough.
	for i := 0; i < 20; i++ {
		time.Sleep(5 * time.Millisecond)
	}
	fake.Call("invoke")
	if got := fake.Get("lastReturn").String(); got != "undefined" {
		t.Errorf("the callback returned %q after the request settled: it was never released, so one js.Func leaks per abandoned election", got)
	}
}

// ⛔⛔ A CONTEXT ALREADY OVER DOES NOT ASK THE BROWSER AT ALL, and the witness
// is the request COUNT rather than the answer. The answer alone cannot hold
// this guard: openWith refuses a finished context on its own, so the error
// comes back either way. What the guard buys is DETERMINISM — without it, a
// granted lock and a finished context make both cases of electByLock's select
// ready, and Go picks between ready cases at random, so a cancelled caller had
// a coin's chance of being handed a role and a held lock.
//
// ⚠ That coin flip is why this is a count and not a timing test: a random
// select cannot be caught by asserting an outcome. It was found by the full
// suite failing where the same test passed in isolation.
func TestAContextAlreadyOverDoesNotAskTheBrowserForTheLock(t *testing.T) {
	fake := installLockFake(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if role, ok := electByLock(ctx, "room:never-asked"); ok {
		t.Fatalf("a finished context must not decide, got %v", role)
	}
	if n := fake.Get("requests").Int(); n != 0 {
		t.Errorf("the browser was asked %d time(s) on a context that was already over, want 0", n)
	}
	// The control: with a live context the same fake IS asked, so the zero
	// above is about the context and not about a fixture nobody reached.
	live, lcancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer lcancel()
	_, _ = electByLock(live, "room:asked")
	if n := fake.Get("requests").Int(); n != 1 {
		t.Errorf("the control: a live context asked %d time(s), want 1", n)
	}
}

// ⛔⛔ A THENABLE THAT CALLS BOTH HANDLERS MUST NOT WEDGE THE PAGE. The guard in
// releaseWhenSettled admits any truthy `then`, and its own comment contemplates
// shims — so a `then` that calls onFulfilled AND onRejected used to panic
// `close of closed channel` inside handleEvent. A review reproduced it: the
// panic is recoverable and the page still dies, because the event loop is gone
// and even stdout blocks. Native promises never do this; a shim is not a native
// promise, which is the whole reason the guard is permissive.
func TestAThenableThatCallsBothHandlersDoesNotWedge(t *testing.T) {
	g := js.Global()
	g.Call("eval", `
globalThis.__lockFake3 = {requests: 0};
Object.defineProperty(navigator, "locks", {
  configurable: true,
  value: {
    request(name, opts, cb) {
      globalThis.__lockFake3.requests++;
      // A thenable, not a promise: it calls BOTH handlers, synchronously.
      return {then: function (onOK, onErr) { onOK(undefined); onErr(undefined); }};
    },
  },
});
`)
	t.Cleanup(func() { g.Call("eval", `delete navigator.locks; delete globalThis.__lockFake3`) })

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, ok := electByLock(ctx, "room:thenable"); ok {
		t.Fatal("the callback was never granted the lock, so nothing was decided")
	}
	if n := g.Get("__lockFake3").Get("requests").Int(); n != 1 {
		t.Fatalf("the thenable was asked %d time(s), want 1: this test would otherwise pass blind", n)
	}
	// Reaching here at all is the assertion: a second close panics, and a
	// panic in handleEvent takes the event loop with it, so the test would
	// never return rather than fail. The sleep gives the handlers their turn.
	time.Sleep(20 * time.Millisecond)
}

// ⛔⛔ AND THE OTHER HALF OF #276: giving up is not "this browser has no Web
// Locks". electByLock answers (0, false) for both, by design — its bool says
// only "the lock answered" — so openWith reads the CONTEXT to tell them apart.
// Without this it fell through and started the WINDOW election: a second
// election nobody asked for, on a context that was already finished.
func TestACancelledLockElectionDoesNotStartTheWindowElection(t *testing.T) {
	installLockFake(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// A nil busConn is safe precisely because nothing may be wired: if the
	// window election were reached it would use it, so a panic here would be
	// the defect announcing itself rather than a flaw in the fixture.
	_, _, _, err := openWith(ctx, nil, "room:gaveup", time.Second)
	if err == nil {
		t.Fatal("a finished context must produce an error, not an election")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("the context's own error must come back, got %v", err)
	}
	if strings.Contains(err.Error(), "window") {
		t.Errorf("the window election was reached; it said %q", err)
	}
}
