//go:build js && wasm

// Command browsertest is the real-browser half of the WebRTC proof. Everything
// the Node end-to-end test stubs — the RTCPeerConnection, ICE, the data
// channel — is here the browser's own, driven by a headless Chrome from
// peer_browser_test.go.
//
// It runs the whole thing in one page and pastes for itself: two participants
// each open a real WebRTC connection to a server also running in the page, the
// offer and answer handed across in process rather than by a person. Then both
// edit one document, and the page asserts that each participant's character
// reached the other — over WebRTC, with no server on either connection. The
// result is left on globalThis.__result for the driver to read, and each step is
// logged so a run can be followed.
package main

import (
	"context"
	"fmt"
	"syscall/js"
	"time"

	"github.com/go-crdt/collab"
	"github.com/go-crdt/crdt"
)

func main() {
	ok, detail := run()
	js.Global().Get("console").Call("log", "DONE "+detail)
	js.Global().Set("__result", map[string]any{"ok": ok, "detail": detail})
	// The verdict is recorded, and the driver reads it off the page. The program
	// stays running rather than returning: a real RTCPeerConnection goes on
	// firing events, and firing one into a Go instance that has exited throws.
	select {}
}

func logf(format string, args ...any) {
	js.Global().Get("console").Call("log", fmt.Sprintf(format, args...))
}

// run connects two participants to one in-page server over real WebRTC and has
// them edit one document, returning whether each one's edit reached the other.
func run() (bool, string) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	srv := collab.NewServer(collab.Config{Store: collab.NewMemoryStore()})

	// Two participants, so that "an edit on one reaches the other" is a claim
	// about two clients and not about the server's own copy: each opens its own
	// real connection to the server, and the server serves both.
	//
	// Nothing is closed on the way out. This is a page that runs once and reports;
	// a real RTCDataChannel goes on firing after a Close would have released the
	// callbacks under it, and a released callback fired is a page throwing where
	// it should be still. Leaving it all open is what a one-shot proof wants.
	var bodies [2]*collab.Text
	var clients [2]*collab.Client
	for i := range clients {
		guestCh, hostCh, err := connect(ctx, i)
		if err != nil {
			return false, err.Error()
		}
		logf("participant %d: WebRTC connected, both channels open", i)
		go func() { _ = srv.ServeDataChannel(ctx, hostCh) }()

		client, err := collab.Join(ctx, collab.DataChannel(guestCh),
			collab.ClientConfig{Document: "these.tex", Site: collabSite(i)})
		if err != nil {
			return false, fmt.Sprintf("participant %d join: %v", i, err)
		}
		clients[i] = client
		if bodies[i], err = client.Text("file:main.tex"); err != nil {
			return false, fmt.Sprintf("participant %d text: %v", i, err)
		}
	}

	// Both type at once, which is the case that needs the merge to agree.
	letters := [2]string{"A", "B"}
	for i, body := range bodies {
		if err := body.Insert(0, letters[i]); err != nil {
			return false, fmt.Sprintf("participant %d insert: %v", i, err)
		}
	}
	logf("both participants edited")

	// Each has to end up holding both characters: the one it typed and the one
	// that crossed a WebRTC connection to reach it.
	for i, client := range clients {
		if err := awaitBoth(ctx, client, bodies[i]); err != nil {
			return false, fmt.Sprintf("participant %d did not converge: %v", i, err)
		}
	}
	first, second := bodies[0].String(), bodies[1].String()
	if first != second {
		return false, fmt.Sprintf("replicas disagree: %q vs %q", first, second)
	}
	logf("edit propagated between participants over WebRTC: both hold %q", first)
	return true, "both participants converged on " + first
}

// connect opens one real WebRTC connection in the page: a host Peer offers, a
// guest Peer answers, and the offer and answer are handed across in process. It
// returns the guest's channel (the one that joins) and the host's channel (the
// one the server serves), both open.
func connect(ctx context.Context, i int) (guestCh, hostCh js.Value, err error) {
	host, err := collab.NewPeer(collab.PeerConfig{})
	if err != nil {
		return js.Value{}, js.Value{}, fmt.Errorf("participant %d host peer: %w", i, err)
	}
	guest, err := collab.NewPeer(collab.PeerConfig{})
	if err != nil {
		return js.Value{}, js.Value{}, fmt.Errorf("participant %d guest peer: %w", i, err)
	}

	offer, err := host.Offer(ctx, "collab")
	if err != nil {
		return js.Value{}, js.Value{}, fmt.Errorf("participant %d offer: %w", i, err)
	}
	answer, err := guest.Answer(ctx, offer)
	if err != nil {
		return js.Value{}, js.Value{}, fmt.Errorf("participant %d answer: %w", i, err)
	}
	if err := host.AcceptAnswer(answer); err != nil {
		return js.Value{}, js.Value{}, fmt.Errorf("participant %d accept: %w", i, err)
	}
	if hostCh, err = host.DataChannel(ctx); err != nil {
		return js.Value{}, js.Value{}, fmt.Errorf("participant %d host channel: %w", i, err)
	}
	if guestCh, err = guest.DataChannel(ctx); err != nil {
		return js.Value{}, js.Value{}, fmt.Errorf("participant %d guest channel: %w", i, err)
	}
	return guestCh, hostCh, nil
}

// awaitBoth blocks until a participant holds both characters, or the context
// ends.
func awaitBoth(ctx context.Context, client *collab.Client, body *collab.Text) error {
	for body.Len() < 2 {
		select {
		case <-client.Changes():
		case <-client.Done():
			if body.Len() < 2 {
				return client.Err()
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// collabSite gives each participant a distinct replica identity; the server is
// nobody's site, so 2 and 3 are free.
func collabSite(i int) crdt.SiteID { return crdt.SiteID(i + 2) }
