//go:build js && wasm

package collab

import (
	"context"
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
	cb := js.FuncOf(func(_ js.Value, args []js.Value) any {
		if len(args) == 0 || !args[0].Truthy() {
			// Not ours: a host holds the room. Settling at once releases nothing,
			// because nothing was taken.
			decided <- RoleClient
			return js.Undefined()
		}
		decided <- RoleHost
		promise, releasePromise := heldUntil(ctx)
		release = releasePromise
		return promise
	})

	locks.Call("request", "collab:"+room, opts, cb)

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
		cb.Release()
		return 0, false
	}
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
