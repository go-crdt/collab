//go:build !js

package collab

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/go-crdt/crdt"
	"github.com/go-crdt/crdt/awareness"
)

// A session speaks for itself and for nobody else. The site in a presence
// update is chosen by whoever sent it: without this a participant could move
// another user's cursor, announce their departure, or -- by inventing a fresh
// site per message -- grow the registry here and on every peer it is fanned
// out to, without bound.
func TestPresenceIsRefusedForAnotherSite(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	srv := NewServer(Config{Store: NewMemoryStore()})
	t.Cleanup(func() { _ = srv.Close(context.Background()) })

	published := func(site crdt.SiteID) []byte {
		t.Helper()
		reg := awareness.New()
		raw, err := reg.Publish(site, awareness.Cursor{Head: 1, Anchor: 1}, nil).MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}

	// Its own: accepted, and the session goes on.
	own := &scriptedCarrier{ctx: ctx, hangsUp: true, in: []scripted{
		{kindJoin, joinMsg{Document: "d", Site: 7}},
		{kindPresence, presenceMsg{Update: published(7)}},
	}}
	if err := srv.session(own); err != nil && !strings.Contains(err.Error(), "context canceled") {
		t.Fatalf("a session publishing its own presence was refused: %v", err)
	}

	// Another site's: refused, by name.
	forged := &scriptedCarrier{ctx: ctx, hangsUp: true, in: []scripted{
		{kindJoin, joinMsg{Document: "d", Site: 7}},
		{kindPresence, presenceMsg{Update: published(9)}},
	}}
	err := srv.session(forged)
	var se *sessionError
	if !asSessionError(err, &se) || se.kind != errInvalid {
		t.Fatalf("the session ended with %v, want a refusal", err)
	}
	if !strings.Contains(err.Error(), "presence for site 9") {
		t.Fatalf("the refusal does not name the site: %v", err)
	}
}

// asSessionError is errors.As without importing errors for one call.
func asSessionError(err error, out **sessionError) bool {
	se, ok := err.(*sessionError)
	if ok {
		*out = se
	}
	return ok
}
