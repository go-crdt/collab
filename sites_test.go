//go:build !js

package collab

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"reflect"
	"strings"
	"testing"

	"github.com/go-crdt/crdt"
)

func TestWhatADocumentRemembersAboutWhoHasBeenInIt(t *testing.T) {
	cells := crdt.Part{Kind: crdt.PartMap, Name: "cells"}
	seen := map[crdt.SiteID]crdt.CompositeVersion{
		1: {cells: crdt.VersionVector{1: 4}},
		7: nil, // joined, and has said nothing
		3: {cells: crdt.VersionVector{1: 2, 3: 9}},
	}
	reached := map[crdt.SiteID]crdt.CompositeClocks{
		1: {cells: 11},
		3: {cells: 9},
	}

	raw, err := encodeSites(seen, reached)
	if err != nil {
		t.Fatal(err)
	}
	gotSeen, gotReached, err := decodeSites(raw)
	if err != nil {
		t.Fatalf("what was just written did not read back: %v", err)
	}
	if len(gotSeen) != 3 {
		t.Fatalf("read back %d sites, wrote 3", len(gotSeen))
	}
	if v, named := gotSeen[7]; !named || v != nil {
		t.Fatalf("the site that has said nothing came back as %v, named=%v", v, named)
	}
	if gotSeen[1][cells][1] != 4 || gotSeen[3][cells][3] != 9 {
		t.Fatalf("versions came back as %v", gotSeen)
	}
	if gotReached[1][cells] != 11 || gotReached[3][cells] != 9 {
		t.Fatalf("clocks came back as %v", gotReached)
	}
	if _, said := gotReached[7]; said {
		t.Fatal("a site that has said nothing came back having said something")
	}

	// Sorted, so two servers holding the same thing write the same bytes.
	again, err := encodeSites(map[crdt.SiteID]crdt.CompositeVersion{
		3: {cells: crdt.VersionVector{1: 2, 3: 9}},
		1: {cells: crdt.VersionVector{1: 4}},
		7: nil,
	}, reached)
	if err != nil {
		t.Fatal(err)
	}
	if string(again) != string(raw) {
		t.Fatal("the same participants encoded to different bytes")
	}

	// Nothing that comes back from a store is trusted.
	for _, bad := range [][]byte{
		{},
		{2, 1, 0, 0},          // says two, gives one
		{2, 3, 0, 0, 1, 0, 0}, // sites out of order
		{2, 1, 0, 0, 1, 0, 0}, // one site named twice
		{1, 1, 9, 0, 0},       // a version longer than the message
		{1, 1, 1, 0xff, 0},    // a version that is not one
		{1, 1, 0, 1, 0xff},    // clocks that are not any
		{1, 1, 0, 0, 0, 7},    // bytes left over
	} {
		if _, _, err := decodeSites(bad); !errors.Is(err, crdt.ErrMalformed) {
			t.Fatalf("decodeSites(%v) = %v, want ErrMalformed", bad, err)
		}
	}
}

// A store that cannot be encoded for is reported rather than half-written.
func TestParticipantsThatCannotBeEncoded(t *testing.T) {
	bad := crdt.Part{Kind: crdt.PartKind(9), Name: "not a part"}
	if _, err := encodeSites(
		map[crdt.SiteID]crdt.CompositeVersion{1: {bad: crdt.VersionVector{1: 1}}}, nil,
	); err == nil {
		t.Fatal("a version naming a part that is not one was encoded")
	}
	if _, err := encodeSites(
		map[crdt.SiteID]crdt.CompositeVersion{1: nil},
		map[crdt.SiteID]crdt.CompositeClocks{1: {bad: 1}},
	); err == nil {
		t.Fatal("clocks naming a part that is not one were encoded")
	}
}

// refusingSites keeps documents and refuses to keep anybody, in whichever
// direction a test asks for.
type refusingSites struct {
	*MemoryStore
	load func() ([]byte, error)
	save error
}

func (r refusingSites) LoadSites(ctx context.Context, document string) ([]byte, error) {
	if r.load != nil {
		return r.load()
	}
	return r.MemoryStore.LoadSites(ctx, document)
}

func (r refusingSites) SaveSites(ctx context.Context, document string, sites []byte) error {
	if r.save != nil {
		return r.save
	}
	return r.MemoryStore.SaveSites(ctx, document, sites)
}

// A store that will not give the participants back, or gives back something
// that is not them, stops the document being opened. Neither is a document that
// does not exist, and opening one anyway would collect against a set nobody
// vouched for.
func TestADocumentWhoseParticipantsCannotBeReadDoesNotOpen(t *testing.T) {
	for _, tt := range []struct {
		name  string
		store Store
		want  string
	}{
		{
			"the store will not say",
			refusingSites{MemoryStore: NewMemoryStore(), load: func() ([]byte, error) {
				return nil, errors.New("the disk is gone")
			}},
			"reading the participants",
		},
		{
			"what it says is not participants",
			refusingSites{MemoryStore: NewMemoryStore(), load: func() ([]byte, error) {
				return []byte{9, 9, 9}, nil
			}},
			"unreadable",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			srv := NewServer(Config{Store: tt.store})
			defer func() { _ = srv.Close(context.Background()) }()
			// Asked of the server rather than through a session: an internal
			// error ends the session, and what reaches a client is that the
			// carrier closed. What is being checked is the reason it closed.
			_, err := srv.open(context.Background(), "paper")
			if err == nil {
				t.Fatal("the document opened")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("the error is %q, want it to say %q", err, tt.want)
			}
		})
	}
}

// And a store that will not keep them says so, rather than letting the server
// believe they are kept.
func TestAStoreThatWillNotKeepTheParticipantsSaysSo(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := refusingSites{MemoryStore: NewMemoryStore(), save: errors.New("no room for anybody")}
	srv := NewServer(Config{Store: store})
	defer func() { _ = srv.Close(context.Background()) }()

	tr, sc := Pipe()
	go func() { _ = srv.ServePipe(ctx, sc) }()
	author, err := Join(ctx, tr, ClientConfig{Document: "paper", Site: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = author.Close() }()
	cells, err := author.Map("cells")
	if err != nil {
		t.Fatal(err)
	}
	if err := cells.Set("k", []byte("v")); err != nil {
		t.Fatal(err)
	}

	srv.mu.Lock()
	d := srv.docs["paper"]
	srv.mu.Unlock()
	until(t, "the write to reach the server", func() bool {
		d.mu.Lock()
		defer d.mu.Unlock()
		return d.dirty
	})
	if err := d.persist(ctx); err == nil {
		t.Fatal("a store that would not keep the participants reported success")
	}
	// The snapshot went, and the participants are owed again rather than
	// forgotten.
	d.mu.Lock()
	owed := d.sitesDirty
	d.mu.Unlock()
	if !owed {
		t.Fatal("the participants were given up on rather than kept for the next save")
	}
}

// countRaised counts the counters in got that stand ABOVE the matching ones in
// want. That is the unsafe direction: collectable() is a meet over what the
// participants acknowledged, so raising one lifts the collect floor past a
// participant that is away, and its rejoin is then answered with a superseded
// run — advancing its version vector without telling it what the operation did.
func countRaised(want, got crdt.CompositeVersion) int {
	raised := 0
	for part, vector := range got {
		for site, counter := range vector {
			if counter > want[part][site] {
				raised++
			}
		}
	}
	return raised
}

// Participants that have rotted are refused, rather than read back as a
// different set of people.
//
// Every single-bit flip of what encodeSites writes must be refused. How big
// the hole was is measured, not remembered, by
// TestEverySingleBitFlipOfTheParticipantsIsCaught below, which runs the same
// census over the body alone and over the whole file: dense varints have almost
// no redundancy to trip over, so the structural checks let a quarter of the
// flips through as a DIFFERENT set of people. A CRC32C detects every single-bit
// error there is, so the answer here is none.
func TestAParticipantsFileThatHasRottedIsRefused(t *testing.T) {
	cells := crdt.Part{Kind: crdt.PartMap, Name: "cells"}
	seen := map[crdt.SiteID]crdt.CompositeVersion{
		1: {cells: crdt.VersionVector{1: 4, 2: 1}},
		2: {cells: crdt.VersionVector{1: 1, 2: 5}},
		7: nil,
	}
	reached := map[crdt.SiteID]crdt.CompositeClocks{1: {cells: 11}, 2: {cells: 9}}
	raw, err := encodeSites(seen, reached)
	if err != nil {
		t.Fatal(err)
	}

	accepted, raised := 0, 0
	first := ""
	for i := range raw {
		for bit := 0; bit < 8; bit++ {
			bad := append([]byte(nil), raw...)
			bad[i] ^= 1 << bit
			got, _, err := decodeSites(bad)
			if err != nil {
				// Refused is the answer; it must stay the documented one.
				if !errors.Is(err, crdt.ErrMalformed) {
					t.Fatalf("byte %d bit %d was refused with %v, want ErrMalformed", i, bit, err)
				}
				continue
			}
			accepted++
			up := 0
			for site, version := range got {
				up += countRaised(seen[site], version)
			}
			if up > 0 {
				raised += up
				if first == "" {
					first = fmt.Sprintf("byte %d bit %d reads back %v, which raises %d counter(s)", i, bit, got, up)
				}
			}
		}
	}
	if accepted > 0 {
		t.Fatalf("%d of %d single-bit flips were read back as participants rather than refused, %d of them raising a counter: %s",
			accepted, len(raw)*8, raised, first)
	}

	// And it is the checksum that refuses, not a structural check that happened
	// to trip: asking which error is the point of the fix.
	rotted := append([]byte(nil), raw...)
	rotted[len(rotted)-1] ^= 1
	_, _, err = decodeSites(rotted)
	if err == nil {
		t.Fatal("a flip in the last byte was accepted")
	}
	if !errors.Is(err, crdt.ErrMalformed) {
		t.Fatalf("the refusal is %v, want it to be ErrMalformed", err)
	}
	if !strings.Contains(err.Error(), "checksum") {
		t.Fatalf("the refusal is %q, want it to name the checksum", err)
	}
}

// What is written now carries its own magic and its own checksum, so that a
// store which hands back bytes that rotted is caught rather than believed.
func TestAParticipantsFileSavedNowCarriesItsChecksum(t *testing.T) {
	cells := crdt.Part{Kind: crdt.PartMap, Name: "cells"}
	raw, err := encodeSites(
		map[crdt.SiteID]crdt.CompositeVersion{1: {cells: crdt.VersionVector{1: 4}}},
		map[crdt.SiteID]crdt.CompositeClocks{1: {cells: 11}},
	)
	if err != nil {
		t.Fatal(err)
	}
	// Spelled out rather than taken from the constant: what is being pinned is
	// the bytes on disk, which a later reader has to recognise.
	if len(raw) < 9 || string(raw[:5]) != "crdts" {
		t.Fatalf("the participants begin %q, want them to begin %q", raw[:min(5, len(raw))], "crdts")
	}
	if got, want := crc32.Checksum(raw[9:], checksumTable), binary.BigEndian.Uint32(raw[5:9]); got != want {
		t.Fatalf("the checksum written is %08x, and the body's is %08x", want, got)
	}

	// A header with nothing behind it is not a participants file either.
	short := append([]byte("crdts"), 0, 0)
	if _, _, err := decodeSites(short); !errors.Is(err, crdt.ErrMalformed) {
		t.Fatalf("decodeSites(%q) = %v, want ErrMalformed", short, err)
	}
}

// Participants written before there was a checksum are still read, and gain one
// at the next save — the same story compression and the document's own checksum
// each told, with nothing to migrate.
//
// This one passes with the fix and without it. It is a compatibility guard, not
// the proof; the proof is TestAParticipantsFileThatHasRottedIsRefused.
func TestParticipantsWrittenBeforeThereWasAChecksumAreStillRead(t *testing.T) {
	// One site, id 1, that has joined and said nothing.
	seen, reached, err := decodeSites([]byte{1, 1, 0, 0})
	if err != nil {
		t.Fatalf("a file written before the checksum existed was refused: %v", err)
	}
	if v, named := seen[1]; len(seen) != 1 || !named || v != nil {
		t.Fatalf("read back %v, want one site that has said nothing", seen)
	}
	if len(reached) != 0 {
		t.Fatalf("read back clocks %v, want none", reached)
	}

	// And a full one, assembled the way the encoder used to write it.
	cells := crdt.Part{Kind: crdt.PartMap, Name: "cells"}
	version, err := crdt.CompositeVersion{cells: crdt.VersionVector{1: 4}}.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	clocks, err := crdt.CompositeClocks{cells: 11}.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	legacy := binary.AppendUvarint(nil, 1)
	legacy = binary.AppendUvarint(legacy, 1)
	legacy = binary.AppendUvarint(legacy, uint64(len(version)))
	legacy = append(legacy, version...)
	legacy = binary.AppendUvarint(legacy, uint64(len(clocks)))
	legacy = append(legacy, clocks...)

	seen, reached, err = decodeSites(legacy)
	if err != nil {
		t.Fatalf("a full file written before the checksum existed was refused: %v", err)
	}
	if seen[1][cells][1] != 4 || reached[1][cells] != 11 {
		t.Fatalf("read back seen=%v reached=%v", seen, reached)
	}
}

// The census the comment on [sitesMagic] cites, kept in the tree so its figures
// are produced rather than remembered. It flips every single bit of a real
// participants file twice over: once through the body alone, which is what
// decodeSites saw before there was a checksum, and once through the whole file
// as it is written now.
//
// The first count is the size of the hole; the second is zero, because a CRC32C
// detects every single-bit error there is.
func TestEverySingleBitFlipOfTheParticipantsIsCaught(t *testing.T) {
	cells := crdt.Part{Kind: crdt.PartMap, Name: "cells"}
	notes := crdt.Part{Kind: crdt.PartText, Name: "notes"}
	seen := map[crdt.SiteID]crdt.CompositeVersion{
		1: {cells: crdt.VersionVector{1: 4, 2: 1}, notes: crdt.VersionVector{1: 3}},
		2: {cells: crdt.VersionVector{1: 1, 2: 5}},
		7: {notes: crdt.VersionVector{7: 2}},
		9: nil,
	}
	reached := map[crdt.SiteID]crdt.CompositeClocks{1: {cells: 11}, 2: {cells: 9}, 7: {notes: 4}}
	whole, err := encodeSites(seen, reached)
	if err != nil {
		t.Fatal(err)
	}
	body := whole[len(sitesMagic)+4:] // what decodeSites read before the checksum

	same := func(a, b map[crdt.SiteID]crdt.CompositeVersion) bool {
		if len(a) != len(b) {
			return false
		}
		for site, av := range a {
			bv, ok := b[site]
			if !ok || !reflect.DeepEqual(av, bv) {
				return false
			}
		}
		return true
	}

	// Without the checksum: the body decoded on its own, which is the legacy
	// path and exactly what a rotted file used to reach.
	var refusedBody, differentBody int
	for i := range body {
		for bit := range 8 {
			flipped := append([]byte(nil), body...)
			flipped[i] ^= 1 << bit
			got, _, err := decodeSites(flipped)
			switch {
			case err != nil:
				refusedBody++
			case !same(got, seen):
				differentBody++
			}
		}
	}
	// With it: the same flips through the file as it is written today.
	var refusedWhole, differentWhole int
	for i := range whole {
		for bit := range 8 {
			flipped := append([]byte(nil), whole...)
			flipped[i] ^= 1 << bit
			got, _, err := decodeSites(flipped)
			switch {
			case err != nil:
				refusedWhole++
			case !same(got, seen):
				differentWhole++
			}
		}
	}
	t.Logf("body alone (%d bytes, %d flips): %d refused, %d decoded a DIFFERENT set",
		len(body), 8*len(body), refusedBody, differentBody)
	t.Logf("whole file (%d bytes, %d flips): %d refused, %d decoded a DIFFERENT set",
		len(whole), 8*len(whole), refusedWhole, differentWhole)

	if differentBody == 0 {
		t.Fatal("no flip of the body decoded into a different set, so this measures nothing")
	}
	if differentWhole != 0 {
		t.Errorf("%d single-bit flips still decode into a different participant set", differentWhole)
	}
}
