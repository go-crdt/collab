//go:build js && wasm

package collab

import (
	"context"
	"sync"
	"syscall/js"
)

// electByLock decides this tab's role with a Web Lock rather than with a window,
// and reports whether it could decide at all.
//
// # Why a lock rather than a window
//
// [electRole] listens for a duration and concludes from silence. Its own comment
// says what that costs: a machine loaded enough to leave the frames in a queue
// closes both windows having heard nothing, and both tabs elect themselves. That
// is not a corner -- it is what a browser running wasm on a loaded runner does --
// and what keeps the room to one document then is [ErrHostSuperseded] healing it
// afterwards.
//
// A lock has no duration to lose. The browser grants an exclusive lock to exactly
// one holder per name and releases it when that holder's tab goes away, including
// abruptly: measured on Chrome for Testing 149 at 52 ms or less from a renderer
// crash to another tab holding it, with the control that no other tab could take
// it while the first one lived. So two hosts become impossible where this path is
// taken, rather than recoverable.
//
// [ErrHostSuperseded] is untouched and still load-bearing: a browser without the
// API falls back to the window, and this returns false there.
//
// # ifAvailable, and why not a pending request
//
// The request asks for an IMMEDIATE answer. Without ifAvailable a losing tab's
// request stays pending until the host's tab dies, which is not a client -- it is
// a tab that never returns from electing. With it the callback is handed null at
// once, which is the answer "somebody else holds this room".
//
// # What holds the lock
//
// The callback returns a promise that settles when ctx does, because a lock is held
// for exactly as long as its callback runs. ctx here is the session's lifetime,
// which is why this is reached from [OpenBroadcastSession] and not from
// [HostOrJoin]: the latter's ctx bounds only the election, so a lock taken there
// would be released before the caller had served anything -- reopening the very gap
// that shape is already documented to have.
func electByLock(ctx context.Context, room string) (Role, bool) {
	// ⛔⛔ A FINISHED CONTEXT DOES NOT START AN ELECTION, and the select below is
	// why this has to be here. With the context already over AND the lock
	// granted at once — which is what happens to a tab alone in a room — BOTH
	// cases of that select are ready, and Go picks between ready cases AT
	// RANDOM. So a cancelled caller had a coin's chance of being handed a role
	// and a held lock.
	//
	// Measured rather than reasoned: the test for this passed in isolation and
	// FAILED in the full suite, which is the shape of a chosen-at-random select
	// rather than of a broken fixture. Asking first makes the answer the same
	// every time, and it is the same precedence the peer's waits use — a
	// context that is over outranks whatever else became ready.
	if ctx.Err() != nil {
		return 0, false
	}
	locks := js.Global().Get("navigator").Get("locks")
	if !locks.Truthy() || !locks.Get("request").Truthy() {
		return 0, false
	}

	opts := js.Global().Get("Object").New()
	opts.Set("mode", "exclusive")
	opts.Set("ifAvailable", true)

	decided := make(chan Role, 1)
	// held is closed when the holding promise has been handed back, so the
	// callback's own js.Func is only released once nothing will call it again.
	var release func()
	// ⛔⛔ THE SEND IS NON-BLOCKING, and that is a wedge the give-up path made
	// reachable. `decided` is buffered ONE and nothing reads it once the
	// ctx.Done branch has been taken, so a SECOND callback blocked for ever on
	// the send — inside a JavaScript callback, which stops the event loop for
	// the whole page. `ifAvailable: true` means a real lock manager calls back
	// once, so this was only ever a shim's or a retry's doing; before #276 a
	// second call merely printed a released-function line, and after it the call
	// is live and would have hung. A role nobody is waiting for is dropped.
	tell := func(role Role) {
		select {
		case decided <- role:
		default:
		}
	}
	cb := js.FuncOf(func(_ js.Value, args []js.Value) any {
		if len(args) == 0 || !args[0].Truthy() {
			// Not ours: a host holds the room. Settling at once releases nothing,
			// because nothing was taken.
			tell(RoleClient)
			return js.Undefined()
		}
		tell(RoleHost)
		promise, releasePromise := heldUntil(ctx)
		// ⛔ Recorded under the mutex-free assumption this file already makes
		// (one callback, cooperative scheduling), and read by BOTH exits now:
		// the give-up path used to leave this write unread, so a grant arriving
		// after the give-up leaked heldUntil's own js.Func — the leak one level
		// below the one the release test rules out.
		release = releasePromise
		return promise
	})

	req := locks.Call("request", "collab:"+room, opts, cb)

	select {
	case role := <-decided:
		if role == RoleClient {
			cb.Release()
		} else {
			// The callback has returned, so it will not be called again; the
			// promise's own functions outlive it and are released with ctx.
			go func() {
				<-ctx.Done()
				cb.Release()
				if release != nil {
					release()
				}
			}()
		}
		return role, true
	case <-ctx.Done():
		// ⛔⛔ NOT cb.Release() HERE (#276). The lock request is still
		// outstanding when this tab stops waiting, so the browser may invoke cb
		// afterwards — and calling a released js.Func panics in whatever
		// goroutine the callback lands on, where it cannot fail the test that
		// caused it. This is #270's class, in the one function #274 never
		// reached, and the comment above already stated the invariant it broke:
		// the js.Func is released "once nothing will call it again".
		releaseWhenSettled(req, cb, &release)
		// ⚠ (0, false) is the same answer as "this page has no Web Locks", and
		// that ambiguity is REAL but it is not resolved here: the bool means
		// only "the lock answered", and the CALLER has the context that says
		// why it did not. [openWith] reads it, so a cancelled election stops
		// instead of starting a second one.
		return 0, false
	}
}

// releaseWhenSettled releases cb once the BROWSER is finished with the lock
// request, which is not the moment this tab stops waiting for it.
//
// ⛔ The releasing happens in a goroutine woken by the handler rather than
// inside the handler: a js.Func that releases itself while it is running is a
// race nothing in this package could observe.
func releaseWhenSettled(req js.Value, cb js.Func, release *func()) {
	// ⚠ A shim whose request() returns no promise cannot be hooked. Leaking one
	// js.Func is the lesser fault: releasing it would reintroduce exactly the
	// panic above, and this says so rather than pretending the hook took.
	if !req.Truthy() || !req.Get("then").Truthy() {
		return
	}
	settled := make(chan struct{})
	// ⛔⛔ ONCE, AND IT IS NOT DEFENSIVE DECORATION. The guard above admits any
	// truthy `then`, and a thenable that calls BOTH handlers panics
	// `close of closed channel` inside handleEvent — which a review reproduced:
	// the panic is recoverable, and the page still wedges, because the event
	// loop is dead and even stdout blocks. Native promises never do this; this
	// function's own comment contemplates shims, so the shim's shape has to be
	// survivable rather than assumed away.
	var once sync.Once
	var onSettle js.Func
	onSettle = js.FuncOf(func(js.Value, []js.Value) any {
		once.Do(func() { close(settled) })
		return nil
	})
	// then(onFulfilled, onRejected): one of the two runs for a real promise.
	req.Call("then", onSettle, onSettle)
	go func() {
		<-settled
		cb.Release()
		onSettle.Release()
		// ⛔ heldUntil's own js.Func, if a grant arrived after this tab gave up.
		// The host path releases it from its own goroutine; the give-up path had
		// no reader for that write at all, so the executor leaked for the page's
		// life. Read here, after the browser is finished, which is the only
		// moment it is safe to.
		//
		// ⚠⚠ NOT HELD BY A TEST, and said rather than left to be assumed: a
		// mutation inverting this condition SURVIVES the suite. heldUntil hands
		// back `executor.Release`, and the executor is called exactly once, by
		// Promise.New, at creation — so releasing it has no effect any
		// JavaScript can observe, and syscall/js exposes no count of live
		// js.Funcs. The fix is right by construction (it frees a func that
		// otherwise never would be) and there is no witness available in this
		// package. A reviewer found it by reading, which is the only way it
		// could have been found.
		if release != nil && *release != nil {
			(*release)()
		}
	}()
}

// heldUntil is a JavaScript promise that settles when ctx does, and the function
// that frees what it holds.
//
// It is what keeps a Web Lock: the browser holds a lock for as long as the promise
// its callback returned is unsettled, so this is the lock's lifetime expressed in
// the only terms the browser accepts.
func heldUntil(ctx context.Context) (js.Value, func()) {
	var resolve js.Value
	executor := js.FuncOf(func(_ js.Value, args []js.Value) any {
		resolve = args[0]
		return js.Undefined()
	})
	promise := js.Global().Get("Promise").New(executor)
	go func() {
		<-ctx.Done()
		if resolve.Truthy() {
			resolve.Invoke()
		}
	}()
	return promise, executor.Release
}
