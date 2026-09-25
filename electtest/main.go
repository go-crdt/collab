//go:build js && wasm

// Command electtest is the real-browser proof that a host election decided by a
// Web Lock leaves exactly one host, where the window it replaces would leave two.
//
// It is driven by election_browser_test.go, which opens TWO pages on it — the
// whole point, since a lock is only exclusive across tabs and a one-page harness
// cannot see that.
//
// The window passed to OpenBroadcastSession is ZERO, and that is what makes this
// discriminating rather than decorative. With a zero window electRole concludes at
// once that it heard no host and hosts; both tabs would therefore host, which is
// the starvation failure electRole's own comment describes, reached deliberately
// instead of by loading the machine. If a lock is taken, exactly one tab hosts
// whatever the window says.
//
// Each page leaves its role on globalThis.__role and, on failure, its reason on
// globalThis.__err. main does not return: a host holds its lock for as long as its
// promise is unsettled, which is for as long as this program's context lives.
package main

import (
	"context"
	"syscall/js"

	"github.com/go-crdt/collab"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	room := js.Global().Get("location").Get("hash").String()
	if room == "" {
		room = "#proof"
	}

	// Zero window: see the command's own comment. The fallback would host here.
	session, err := collab.OpenBroadcastSession(ctx, room[1:], 0)
	if err != nil {
		js.Global().Set("__err", js.ValueOf(err.Error()))
		println("DONE election failed: " + err.Error())
		<-make(chan struct{})
	}
	role := "client"
	if session.Role() == collab.RoleHost {
		role = "host"
	}
	js.Global().Set("__role", js.ValueOf(role))
	println("DONE role " + role)

	// Never return: the lock is held for as long as this context lives, and a host
	// that let go would let the other tab host too — which is the thing being
	// disproved.
	<-make(chan struct{})
}
