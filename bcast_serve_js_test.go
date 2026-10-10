//go:build js && wasm

package collab

import (
	"errors"
	"testing"
)

// Serve refuses a session that is not a host, rather than dereferencing the host
// it has not got.
//
// The gate survived the first mutation sweep of this package's js-only files, on
// 2026-10-10. Deleted, Serve goes straight to bs.host.attachServer on a nil
// host: a panic inside a browser tab, where the stack ends up in a console
// nobody is reading and the tab stops syncing.
//
// Both halves are here because they are different mistakes. A client session is
// a page calling Serve on the tab that was told to join -- the common one. A
// session with the host role and no host is the same page after a host failed to
// come up, which the role alone does not rule out.
//
// The sessions are built here rather than opened, because reaching RoleClient
// through OpenBroadcastSession needs another tab already serving the room, and
// that fixture would make this test about the election rather than about the
// gate. The positive path is not fabricated to match: it is held by
// TestTwoTabsShareADocumentOverABroadcastChannel, which hosts a room and serves
// it for real.
func TestServeRefusesASessionThatIsNotAHost(t *testing.T) {
	srv := NewServer(Config{Store: NewMemoryStore()})
	t.Cleanup(func() { _ = srv.Close(t.Context()) })

	for _, c := range []struct {
		name    string
		session *BroadcastSession
	}{
		{"a client session", &BroadcastSession{role: RoleClient}},
		{"the host role with no host", &BroadcastSession{role: RoleHost}},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := c.session.Role(); got != c.session.role {
				t.Fatalf("the fixture reads back as %v, not what it was built with", got)
			}
			if err := c.session.Serve(srv); !errors.Is(err, ErrProtocol) {
				t.Errorf("Serve gave %v, want ErrProtocol — without it this dereferences a nil host", err)
			}
		})
	}
}

// One of the twenty survivors is named here because the pull request that
// opened this file guessed wrong about it, and the correction is worth keeping
// where the next reader will find it.
//
// peer_js.go:255 and :275 are the SAME check -- the data channel's readyState
// read before the open/error/close listeners are wired, and again after. 255 is
// a fast path: deleting it wires three listeners that are then thrown away, and
// 275 still answers for a channel that was already open. The one that carries
// the behaviour is 275, which closes the window where the channel opens between
// the first read and the wiring -- and that window cannot be opened on purpose
// from a test without controlling the browser's own timing, which the fake does
// not offer.
//
// So: 255 is a cost guard, not the guard against waiting for an event that will
// never fire. The pull request said the opposite before this comment was
// written.
