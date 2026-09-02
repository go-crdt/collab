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

// The encoders send it now, on the wires that had only the room for it.
//
// The room went out first (#107) because a peer built before it refuses trailing
// bytes. What settled the wait was not time passing but a fact: there are no
// production servers on this project, so the peer that would have been broken
// does not exist.
func TestEveryWireSendsTheAdvertisement(t *testing.T) {
	said, err := Mine().MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}

	j, err := encodeClient(kindJoin, joinMsg{Document: "d", Site: 1, Speaks: said})
	if err != nil {
		t.Fatal(err)
	}
	kind, msg, err := decodeClient(j)
	if err != nil || kind != kindJoin {
		t.Fatalf("a join carrying an advertisement did not decode: %v", err)
	}
	if !bytes.Equal(msg.(joinMsg).Speaks, said) {
		t.Fatal("the join's advertisement did not survive the hand-rolled wire")
	}

	w, err := encodeServer(kindWelcome, welcomeMsg{Snapshot: []byte("s"), Speaks: said})
	if err != nil {
		t.Fatal(err)
	}
	if _, m, err := decodeServer(w); err != nil {
		t.Fatalf("a welcome carrying an advertisement did not decode: %v", err)
	} else if !bytes.Equal(m.(welcomeMsg).Speaks, said) {
		t.Fatal("the welcome's advertisement did not survive")
	}

	// And a peer with nothing to say still writes nothing, so the two ways of
	// saying nothing do not both exist on the wire.
	quiet, err := encodeClient(kindJoin, joinMsg{Document: "d", Site: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(quiet) >= len(j) {
		t.Fatal("a join with nothing to say is no smaller than one with something")
	}
	if _, m, err := decodeClient(quiet); err != nil {
		t.Fatal(err)
	} else if len(m.(joinMsg).Speaks) != 0 {
		t.Fatal("a peer that said nothing came back having said something")
	}
}
