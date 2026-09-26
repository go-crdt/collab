//go:build !js

package collab

import (
	"bytes"
	"context"
	"errors"
	"testing"
)

// The digest travels in a second trailing block, which is a place the wire did
// not have before. These are the corners of that place.
//
// The block is sent only to a peer that announced [CapDigest], so a peer built
// before this never meets one -- but the encoder and decoder still have to be
// exact about it, because "nobody will send that" is how a wire acquires a
// second way to say the same thing.
func TestTheWelcomesSecondTrailingBlock(t *testing.T) {
	said, err := Mine().MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	digest := bytes.Repeat([]byte{0xAB}, 32)

	t.Run("it survives a round trip", func(t *testing.T) {
		raw, err := encodeServer(kindWelcome, welcomeMsg{
			Snapshot: []byte("s"), Speaks: said, Digest: digest,
		})
		if err != nil {
			t.Fatal(err)
		}
		_, msg, err := decodeServer(raw)
		if err != nil {
			t.Fatalf("a welcome carrying a digest was refused: %v", err)
		}
		if got := msg.(welcomeMsg).Digest; !bytes.Equal(got, digest) {
			t.Errorf("the digest did not survive: %x", got)
		}
	})

	t.Run("a welcome without one still decodes, and reports none", func(t *testing.T) {
		raw, err := encodeServer(kindWelcome, welcomeMsg{Snapshot: []byte("s"), Speaks: said})
		if err != nil {
			t.Fatal(err)
		}
		_, msg, err := decodeServer(raw)
		if err != nil {
			t.Fatalf("a welcome without a digest was refused: %v", err)
		}
		if got := msg.(welcomeMsg).Digest; len(got) != 0 {
			t.Errorf("a welcome without a digest reported %x", got)
		}
	})

	t.Run("a digest with no advertisement will not encode", func(t *testing.T) {
		// It would be written as the FIRST trailing block and read back as the
		// advertisement: a message decoding to something other than what was
		// encoded, which is worse than one that fails to encode.
		if _, err := encodeServer(kindWelcome, welcomeMsg{Snapshot: []byte("s"), Digest: digest}); !errors.Is(err, ErrProtocol) {
			t.Fatalf("a digest without an advertisement encoded: %v", err)
		}
	})

	t.Run("an empty second block, and a third, are protocol errors", func(t *testing.T) {
		raw, err := encodeServer(kindWelcome, welcomeMsg{Snapshot: []byte("s"), Speaks: said})
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := decodeServer(append(raw, 0x00)); !errors.Is(err, ErrProtocol) {
			t.Errorf("an empty digest block was accepted: %v", err)
		}
		two := appendBytes(appendBytes(raw, digest), digest)
		if _, _, err := decodeServer(two); !errors.Is(err, ErrProtocol) {
			t.Errorf("a third trailing block was accepted: %v", err)
		}
	})
}

// TestWhoIsSaidToReadADigest covers the answers readsDigest gives, and the one
// that matters is the last: silence is not acceptance.
func TestWhoIsSaidToReadADigest(t *testing.T) {
	said, err := Mine().MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if !readsDigest(said) {
		t.Error("this build does not think it reads its own digest")
	}
	if readsDigest(nil) {
		t.Error("a peer that said nothing was taken to read a digest")
	}
	if readsDigest([]byte{0xFF, 0xFF, 0xFF}) {
		t.Error("an advertisement that will not decode was taken to read a digest")
	}
	// A peer announcing a DIFFERENT comparison is a no, not a yes: two digests
	// computed under different rules have nothing to say to each other.
	other, err := Capabilities{CapDigest: []byte{digestComparison + 1}}.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if readsDigest(other) {
		t.Error("a peer announcing another comparison version was taken to read this one")
	}
}

// TestAPeerThatDidNotAskIsNotSentADigest is the claim the whole compatibility
// argument rests on, and it was not proven by anything until here.
//
// The digest goes in a second trailing block. A peer built before this reads ONE
// and refuses whatever follows, so sending it one would end its session — and
// the reason nothing has to wait a release for that is that such a peer never
// announces [CapDigest] and is therefore never sent one.
//
// That is an argument about compose, not about the wire, so it is checked
// against compose.
func TestAPeerThatDidNotAskIsNotSentADigest(t *testing.T) {
	ctx := context.Background()
	srv := NewServer(Config{Store: NewMemoryStore()})
	t.Cleanup(func() { _ = srv.Close(context.Background()) })
	doc, err := srv.open(ctx, "paper")
	if err != nil {
		t.Fatal(err)
	}

	said, err := Mine().MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	// A peer built before this: it says what snapshots it reads, because that
	// has been on the wire for a while, and says nothing about a digest.
	older := Mine()
	delete(older, CapDigest)
	quiet, err := older.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name   string
		speaks []byte
		want   bool
	}{
		{"a peer that announced it", said, true},
		{"a peer built before it", quiet, false},
		{"a peer that said nothing at all", nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var w welcomeMsg
			if err := doc.compose(&w, joinMsg{Document: "paper", Site: 5, Speaks: tc.speaks}); err != nil {
				t.Fatal(err)
			}
			if got := len(w.Digest) > 0; got != tc.want {
				t.Errorf("digest sent = %v, want %v", got, tc.want)
			}
			// And whatever was composed has to go over the wire, since an
			// unsendable welcome is a session that ends on a handshake.
			if _, err := encodeServer(kindWelcome, w); err != nil {
				t.Errorf("the composed welcome would not encode: %v", err)
			}
		})
	}
}
