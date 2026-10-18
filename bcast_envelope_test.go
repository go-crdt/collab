//go:build !js

package collab

import (
	"bytes"
	"encoding/binary"
	"testing"
)

// ⛔ AN OVERFLOWING VARINT MAKES Uvarint RETURN A NEGATIVE COUNT, and the line
// after this guard is `rest = rest[n:]`.
//
// bcast.go:186, `if n <= 0`, survived a mutation sweep of client.go,
// capability.go, bcast.go, authorize.go and backoff.go on 2026-10-17 -- 58
// refusals, six survivors. It reads like a check for a truncated header, which
// is what its own comment says, and it is also the only thing standing between
// a malformed frame and a panic. Measured with it deleted:
//
//	panic: runtime error: slice bounds out of range [-11:]
//	github.com/go-crdt/collab.decodeEnvelope(...)
//		bcast.go:186
//
// binary.Uvarint returns (0, 0) when the buffer is too short and (0, -n) when
// the value needs more than 64 bits -- n is NEGATIVE, not zero, and a negative
// slice index is a panic rather than an empty slice. The guard spells `<= 0`
// and covers both; nothing held either half.
//
// ⛔ Where the bytes come from is what makes it worth a test. This is the
// BroadcastChannel bus: the frame is written by another tab of the same
// origin, so a page that can post one message can stop the tab holding the
// document. The second varint's guard, `m <= 0` one branch down, was already
// held by a test. The first one was not.
func TestAMalformedEnvelopeIsRefusedRatherThanPanicking(t *testing.T) {
	// Ten continuation bytes carry seventy bits, so this overflows.
	overflow := []byte{0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x02}
	if _, n := binary.Uvarint(overflow); n >= 0 {
		t.Fatalf("Uvarint returned %d for an overflowing varint, so this fixture no longer "+
			"exercises the negative-count path the guard exists for", n)
	}

	for _, c := range []struct {
		name string
		raw  []byte
	}{
		{"nothing at all", nil},
		{"empty", []byte{}},
		{"a control byte and no more", []byte{1}},
		{"a truncated first identifier", append([]byte{1}, 0x80)},
		{"an overflowing first identifier", append([]byte{1}, overflow...)},
		{"a truncated second identifier", append([]byte{1, 7}, 0x80)},
		{"an overflowing second identifier", append([]byte{1, 7}, overflow...)},
	} {
		t.Run(c.name, func(t *testing.T) {
			// A panic here fails the test by itself; the assertion is that the
			// answer is a refusal rather than an envelope.
			if env, ok := decodeEnvelope(c.raw); ok {
				t.Errorf("accepted, as %+v", env)
			}
		})
	}

	// The control, and it is the half that keeps the above from passing over a
	// decoder that refuses everything: a well-formed frame still decodes, and
	// to what it was.
	want := envelope{ctl: 3, from: 1 << 40, to: 42, payload: []byte("hello")}
	got, ok := decodeEnvelope(want.encode())
	if !ok {
		t.Fatal("a well-formed envelope was refused, so the refusals above say nothing")
	}
	if got.ctl != want.ctl || got.from != want.from || got.to != want.to || !bytes.Equal(got.payload, want.payload) {
		t.Errorf("round trip gave %+v, want %+v", got, want)
	}
}

// And the same question asked by a fuzzer, since this repository already runs
// five fuzz lanes and a sixth costs nothing to add.
//
// The property is the one the panic broke: whatever the bytes, this RETURNS.
// Then, for a frame it accepted, re-encoding and decoding again gives the same
// envelope -- the meaning survives a round trip, which is what the bus needs,
// since it reads the fields and never the bytes (attach pushes the decoded
// envelope into an inbox, and nothing re-encodes a received frame).
//
// ⛔ THE BYTES DO NOT ROUND-TRIP, and that is the first thing this target was
// asked. In under a second:
//
//	accepted 30800030 and re-encoded it as 300030
//
// `80 00` is a non-canonical varint zero — a continuation byte and then
// nothing more — which binary.Uvarint accepts while AppendUvarint writes a
// single 00. So one envelope has many spellings on this wire.
//
// That is NOT a defect here, and what says so is the measurement rather than
// the reading: nothing compares frame bytes or re-emits a received frame. The
// contrast is worth keeping. In crdt/awareness the same slackness WAS a
// defect — a `gone > 1` clause keeps that encoding canonical precisely because
// its snapshots are compared by their bytes, and without it one state had 255
// spellings. Same looseness, opposite verdict, and what decides is what the
// format is used FOR.
func FuzzDecodeEnvelope(f *testing.F) {
	f.Add(envelope{ctl: 1, from: 2, to: 3, payload: []byte("p")}.encode())
	f.Add(envelope{ctl: 0, from: 0, to: 0}.encode())
	// The shapes the table above names, so the corpus starts where the panic
	// was rather than where a round trip is.
	f.Add([]byte{1, 0x80})
	f.Add([]byte{1, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x02})
	// And the non-canonical zero the fuzzer found, kept as a SENTENCE here
	// rather than as an opaque file under testdata, which is this package's
	// convention for a seed worth remembering.
	f.Add([]byte{0x30, 0x80, 0x00, 0x30})

	f.Fuzz(func(t *testing.T, data []byte) {
		env, ok := decodeEnvelope(data)
		if !ok {
			return
		}
		again, ok := decodeEnvelope(env.encode())
		if !ok {
			t.Fatalf("accepted %x, and what it encodes back to was refused", data)
		}
		if again.ctl != env.ctl || again.from != env.from || again.to != env.to ||
			!bytes.Equal(again.payload, env.payload) {
			t.Fatalf("accepted %x as %+v, which came back as %+v", data, env, again)
		}
	})
}
