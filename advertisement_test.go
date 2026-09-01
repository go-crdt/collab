package collab

import (
	"bytes"
	"errors"
	"testing"
)

// A join and a welcome now have room for a peer to say what it speaks, and a
// peer that says nothing is unchanged.
//
// This is the half that can ship. A peer built before it reads these messages to
// the end and refuses anything after them, so there is nowhere to put an
// advertisement until enough peers have somewhere to put it: the room ships
// first, the sending a release later.
func TestAJoinAndAWelcomeHaveRoomForAnAdvertisement(t *testing.T) {
	// What every peer sends today, and must go on meaning the same thing.
	plain, err := encodeClient(kindJoin, joinMsg{Document: "d", Site: 7, Have: []byte("v")})
	if err != nil {
		t.Fatal(err)
	}
	kind, msg, err := decodeClient(plain)
	if err != nil || kind != kindJoin {
		t.Fatalf("a join without an advertisement did not decode: %v", err)
	}
	if j := msg.(joinMsg); j.Document != "d" || j.Site != 7 || len(j.Speaks) != 0 {
		t.Fatalf("a join without one came back as %+v", j)
	}

	// And one with an advertisement appended, which is what a later release
	// will send. The bytes are opaque here on purpose.
	said := []byte("whatever a peer will say about itself")
	kind, msg, err = decodeClient(appendBytes(plain, said))
	if err != nil || kind != kindJoin {
		t.Fatalf("a join carrying an advertisement was refused: %v", err)
	}
	if j := msg.(joinMsg); !bytes.Equal(j.Speaks, said) {
		t.Fatalf("the advertisement did not survive: %q", j.Speaks)
	}

	// The same for a welcome, in the other direction.
	w, err := encodeServer(kindWelcome, welcomeMsg{Snapshot: []byte("s")})
	if err != nil {
		t.Fatal(err)
	}
	if _, m, err := decodeServer(w); err != nil {
		t.Fatalf("a welcome without an advertisement did not decode: %v", err)
	} else if len(m.(welcomeMsg).Speaks) != 0 {
		t.Fatal("a welcome without one reported having one")
	}
	if _, m, err := decodeServer(appendBytes(w, said)); err != nil {
		t.Fatalf("a welcome carrying an advertisement was refused: %v", err)
	} else if !bytes.Equal(m.(welcomeMsg).Speaks, said) {
		t.Fatal("the welcome's advertisement did not survive")
	}

	// Room for one block is not room for anything at all: a second block, or a
	// truncated one, is still a protocol error.
	if _, _, err := decodeClient(appendBytes(appendBytes(plain, said), said)); !errors.Is(err, ErrProtocol) {
		t.Fatalf("two trailing blocks were accepted: %v", err)
	}
	if _, _, err := decodeClient(append(plain, 0xFF)); !errors.Is(err, ErrProtocol) {
		t.Fatalf("a truncated advertisement was accepted: %v", err)
	}
	// And an empty one is not an advertisement: a peer with nothing to say says
	// nothing, so a zero-length block would be a second way to say it.
	if _, _, err := decodeClient(append(plain, 0x00)); !errors.Is(err, ErrProtocol) {
		t.Fatalf("an empty advertisement was accepted: %v", err)
	}
}

// Nothing writes one yet, which is the whole of what makes this safe to ship.
func TestNothingSendsAnAdvertisementYet(t *testing.T) {
	j, err := encodeClient(kindJoin, joinMsg{Document: "d", Site: 1, Speaks: []byte("x")})
	if err != nil {
		t.Fatal(err)
	}
	if _, m, err := decodeClient(j); err != nil {
		t.Fatal(err)
	} else if len(m.(joinMsg).Speaks) != 0 {
		t.Fatal("the encoder sent an advertisement; a peer built before this would refuse the message")
	}
}
