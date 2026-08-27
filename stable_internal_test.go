//go:build !js

package collab

import (
	"context"
	"errors"
	"testing"

	"github.com/go-crdt/crdt"
)

// The wire carries an acknowledgement both ways round, and refuses what is not
// one — the same as every other message here.
func TestAnAcknowledgementGoesOverTheWire(t *testing.T) {
	raw, err := encodeClient(kindAcknowledge, ackMsg{Version: []byte("v")})
	if err != nil {
		t.Fatalf("encodeClient: %v", err)
	}
	kind, msg, err := decodeClient(raw)
	if err != nil || kind != kindAcknowledge {
		t.Fatalf("decodeClient = %d, %v", kind, err)
	}
	if got, ok := msg.(ackMsg); !ok || string(got.Version) != "v" {
		t.Fatalf("decoded %#v", msg)
	}
	if _, err := encodeClient(kindAcknowledge, joinMsg{}); !errors.Is(err, ErrProtocol) {
		t.Fatalf("encoding a join as an acknowledgement = %v, want ErrProtocol", err)
	}
	// A truncated one is refused rather than read as an empty version.
	if _, _, err := decodeClient([]byte{kindAcknowledge}); !errors.Is(err, ErrProtocol) {
		t.Fatalf("decoding a truncated acknowledgement = %v, want ErrProtocol", err)
	}
	if _, _, err := decodeClient(append(raw, 'x')); !errors.Is(err, ErrProtocol) {
		t.Fatalf("decoding an acknowledgement with bytes left over = %v, want ErrProtocol", err)
	}
}

// A version that does not decode is a participant describing itself
// unreadably, and is refused rather than guessed at. An empty one is a
// participant saying it holds nothing, which is a thing it may honestly say.
func TestAnUnreadableAcknowledgementIsRefused(t *testing.T) {
	d := &document{}
	sub := &subscriber{site: 1}
	if err := d.acknowledge(sub, []byte{0xff, 0xff, 0xff}); err == nil {
		t.Fatal("an unreadable version was accepted")
	}
	if err := d.acknowledge(sub, nil); err != nil {
		t.Fatalf("an empty version was refused: %v", err)
	}
	if sub.have == nil {
		t.Fatal("a participant saying it holds nothing was recorded as having said nothing")
	}
	if len(sub.have) != 0 {
		t.Fatalf("an empty version decoded to %d parts", len(sub.have))
	}
}

// Stable answers for a document that is open and has been heard from, and
// declines otherwise rather than inventing a version.
func TestStableDeclinesWhenItCannotKnow(t *testing.T) {
	srv := NewServer(Config{Store: NewMemoryStore()})
	t.Cleanup(func() { _ = srv.Close(context.Background()) })

	if _, ok := srv.Stable("never opened"); ok {
		t.Fatal("a document nobody has opened has a stable version")
	}
	// Opened but with nobody in it: there is no set of participants to take a
	// meet over, and the meet of nothing is not zero, it is unknown.
	if _, err := srv.open(context.Background(), "empty"); err != nil {
		t.Fatal(err)
	}
	if _, ok := srv.Stable("empty"); ok {
		t.Fatal("a document with no participants has a stable version")
	}
}

// The meet of two versions is what both of them have, and a part one of them
// has never heard of is a part neither can be said to hold.
func TestTheMeetKeepsOnlyWhatBothHave(t *testing.T) {
	text := crdt.Part{Name: "body", Kind: crdt.PartText}
	other := crdt.Part{Name: "notes", Kind: crdt.PartText}
	a := crdt.CompositeVersion{
		text:  crdt.VersionVector{1: 7, 2: 3},
		other: crdt.VersionVector{1: 4},
	}
	b := crdt.CompositeVersion{
		text: crdt.VersionVector{1: 5, 2: 9, 3: 1},
	}
	got := meet(a, b)
	if len(got) != 1 {
		t.Fatalf("the meet holds %d parts, want the one they share", len(got))
	}
	want := crdt.VersionVector{1: 5, 2: 3}
	for site, seq := range want {
		if got[text][site] != seq {
			t.Fatalf("site %d met at %d, want %d", site, got[text][site], seq)
		}
	}
	// A site only one of them has heard of is not in the meet at all, rather
	// than being carried at that one's value.
	if _, there := got[text][3]; there {
		t.Fatal("a site only one replica had reached is in the meet")
	}
}
