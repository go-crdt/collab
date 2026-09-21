// Copyright (c) the go-crdt authors.
// SPDX-License-Identifier: BSD-3-Clause

//go:build !js

package collab

import (
	"context"
	"testing"

	"github.com/go-crdt/crdt"
)

// An acknowledgement from behind a link reaches the peer, in a document where
// nothing else happens afterwards.
//
// A link computes what it can promise at two edges: when it relays an operation
// and when it applies one. A participant saying "I have it now" is neither. So
// before [document.wakeLinks] a promise that became available after the last
// operation was never sent, and the peer's floor stayed where that edge had left
// it — for ever, in a document that had gone quiet.
//
// The shape that shows it needs exactly ONE operation. With two, the second
// operation's edge carries the promise the first one's acknowledgement made
// available, and everything looks fine; that is why this went unnoticed, and why
// TestAParticipantBehindALinkIsNotCollectedPast met it only as a rare failure on
// the slowest lanes — there, the deletion's edge usually arrived after the
// reader's acknowledgement and occasionally before it.
//
// What it cost was collection rather than correctness: a floor that does not
// move keeps work rather than losing it, which is the direction that stays
// invisible. It is the same failure Config.CollectEvery had in a federation
// before a link acknowledged at all.
func TestAnAcknowledgementBehindALinkReachesThePeer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	paris := NewServer(Config{Store: NewMemoryStore()})
	defer func() { _ = paris.Close(context.Background()) }()
	lyon := NewServer(Config{Store: NewMemoryStore()})
	defer func() { _ = lyon.Close(context.Background()) }()

	tr, sc := Pipe()
	go func() { _ = paris.ServePipe(ctx, sc) }()
	author, err := Join(ctx, tr, ClientConfig{Document: "paper", Site: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = author.Close() }()
	cells, err := author.Map("cells")
	if err != nil {
		t.Fatal(err)
	}

	link := &directDial{srv: paris, ctx: ctx}
	go func() { _ = lyon.Follow(ctx, link, "paper", crdt.SiteID(9001)) }()

	// A participant on Lyon that only ever reads, so it never produces an
	// operation of its own for the link to relay. Its acknowledgement is the
	// only thing it contributes, which is the point.
	rtr, rsc := Pipe()
	go func() { _ = lyon.ServePipe(ctx, rsc) }()
	reader, err := Join(ctx, rtr, ClientConfig{Document: "paper", Site: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Close() }()
	theirs, err := reader.Map("cells")
	if err != nil {
		t.Fatal(err)
	}

	// Exactly one operation, and then silence.
	if err := cells.Set("k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	until(t, "the reader on Lyon to see it", func() bool {
		v, held := theirs.Get("k")
		return held && string(v) == "v"
	})

	seen := func(s *Server, site crdt.SiteID) crdt.CompositeVersion {
		s.mu.Lock()
		d := s.docs["paper"]
		s.mu.Unlock()
		if d == nil {
			return nil
		}
		d.mu.Lock()
		defer d.mu.Unlock()
		return d.seen[site]
	}

	until(t, "Lyon to hold the reader's acknowledgement", func() bool {
		return seen(lyon, 2) != nil
	})
	// Which is what makes Lyon's promise available, and nothing else will
	// happen: no operation follows, so nothing but the wake can carry it.
	until(t, "Paris to learn what the link promises", func() bool {
		return seen(paris, crdt.SiteID(9001)) != nil
	})
}

// wakeLinks skips the participant that spoke and everyone who is not a link, and
// a second wake for a link that has not drained the first is dropped.
//
// Asserted directly because the arms are a select: two acknowledgements racing
// one drain is not something to arrange through servers, and a coalescing channel
// whose coalescing is never exercised is a comment.
func TestWakeLinksSkipsWhoItShouldAndCoalesces(t *testing.T) {
	speaker := &subscriber{site: 1}
	ordinary := &subscriber{site: 2}
	linkOne := &subscriber{site: 9001, promiseMoved: make(chan struct{}, 1)}
	linkTwo := &subscriber{site: 9002, promiseMoved: make(chan struct{}, 1)}
	// Already awake, and not drained.
	linkTwo.promiseMoved <- struct{}{}

	d := &document{subs: map[*subscriber]struct{}{
		speaker: {}, ordinary: {}, linkOne: {}, linkTwo: {},
	}}
	d.mu.Lock()
	d.wakeLinks(speaker)
	d.mu.Unlock()

	if len(linkOne.promiseMoved) != 1 {
		t.Fatal("a link was not woken")
	}
	if len(linkTwo.promiseMoved) != 1 {
		t.Fatalf("a link already awake holds %d wakes, want the one it had", len(linkTwo.promiseMoved))
	}
	// And nothing was written for the two that have no channel, which would have
	// panicked on a nil channel send rather than being skipped.
	if speaker.promiseMoved != nil || ordinary.promiseMoved != nil {
		t.Fatal("this test no longer distinguishes a link from a participant")
	}
}

// newParticipant joins srv over its own pipe.
func newParticipant(t *testing.T, ctx context.Context, srv *Server, site crdt.SiteID) *Client {
	t.Helper()
	tr, sc := Pipe()
	go func() { _ = srv.ServePipe(ctx, sc) }()
	client, err := Join(ctx, tr, ClientConfig{Document: "paper", Site: site})
	if err != nil {
		t.Fatal(err)
	}
	return client
}
