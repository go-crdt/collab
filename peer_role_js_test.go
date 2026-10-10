//go:build js && wasm

package collab

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// Answering twice is refused, and refused for the right reason.
//
// TestPeerRoleIsTakenOnce already covers three of the four ways a page can take
// two roles, and it was not enough. A mutation sweep of the js-only files -- the
// first this package has had, since a native `go test` does not compile them --
// deleted the role gate at the top of Answer on 2026-10-10 and that test stayed
// green.
//
// It stayed green because of what it asserts. Its answering case passes the
// literal "anything", which is not a connection description, so with the gate
// deleted Answer carries on and decodeSignal refuses it instead -- and both
// refusals wrap ErrTransport, so neither the test nor errors.Is can tell them
// apart. That is the campaign's most common shape and it is here too: a test
// asserting that AN error happened, where the code after the deleted guard also
// fails and says something else.
//
// So this one hands Answer a VALID offer. Then nothing below the gate can refuse
// it, and the only thing that can is the gate.
func TestAnsweringTwiceIsRefusedByTheRoleAndNotByTheDecoder(t *testing.T) {
	installFake(t)
	ctx := context.Background()

	// Two real offers, so neither call below can fail on its argument.
	first, err := offerer2Offer(t)
	if err != nil {
		t.Fatal(err)
	}
	second, err := offerer2Offer(t)
	if err != nil {
		t.Fatal(err)
	}

	p, err := NewPeer(PeerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Close() }()

	// The control: the first answer succeeds, so the refusal below is about the
	// second one and not about the fake, the offer or the peer.
	if _, err := p.Answer(ctx, first); err != nil {
		t.Fatalf("the first answer failed, so this test is not about answering twice: %v", err)
	}

	_, err = p.Answer(ctx, second)
	if err == nil {
		t.Fatal("a second answer was accepted: this peer now has two roles and the connection it describes is nobody's")
	}
	if !errors.Is(err, ErrTransport) {
		t.Errorf("the second answer gave %v, want an ErrTransport", err)
	}
	// And the message, because ErrTransport alone is what let the older test
	// pass with the gate deleted.
	if !strings.Contains(err.Error(), "already has a role") {
		t.Errorf("the second answer gave %q, which does not say the role was already taken — "+
			"an ErrTransport from the decoder reads the same to errors.Is", err)
	}

	// The same question of the other order, with a valid offer this time, since
	// that is the case the older test spends on an invalid one.
	q, err := NewPeer(PeerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = q.Close() }()
	if _, err := q.Offer(ctx, "collab"); err != nil {
		t.Fatal(err)
	}
	_, err = q.Answer(ctx, first)
	if err == nil {
		t.Fatal("answering after offering was accepted")
	}
	if !strings.Contains(err.Error(), "already has a role") {
		t.Errorf("answering after offering gave %q, which does not say the role was already taken", err)
	}
}
