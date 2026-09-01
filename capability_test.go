//go:build (js && wasm) || !js

package collab

import (
	"bytes"
	"errors"
	"testing"

	"github.com/go-crdt/crdt"
)

// What this build says about itself is what crdt says about itself, rather than
// a list here kept in step with it by hand.
func TestMineIsWhatCrdtReads(t *testing.T) {
	mine := Mine()
	for _, f := range crdt.Formats() {
		want := crdt.Reads(f)
		got := mine[capabilityOf(f)]
		if !bytes.Equal(got, want) {
			t.Errorf("%v: says %v, crdt reads %v", f, got, want)
		}
	}
	// The gap is the point of a set: crdt reserves version 7 of a text.
	if mine.Accepts(CapText, 7) {
		t.Error("this build claims to read a text version crdt refuses")
	}
	if !mine.Accepts(CapText, crdt.Writes(crdt.FormatText)) {
		t.Error("this build does not claim to read what it writes")
	}
}

// Saying nothing about something is not accepting it: the peer that says nothing
// at all is exactly the old build this exists to protect.
func TestSilenceIsNotAcceptance(t *testing.T) {
	var nothing Capabilities
	if nothing.Accepts(CapText, 1) {
		t.Error("a peer that said nothing was taken to accept")
	}
	said := Capabilities{CapText: {1, 2}}
	if said.Accepts(CapMap, 1) {
		t.Error("a capability never named was taken as accepted")
	}
	if said.Accepts(CapText, 3) {
		t.Error("a version never named was taken as accepted")
	}
	if !said.Accepts(CapText, 2) {
		t.Error("a version that was named is not accepted")
	}
}

// The encoding is canonical, so two peers understanding the same things say so
// in the same bytes.
func TestCapabilitiesRoundTripCanonically(t *testing.T) {
	mine := Mine()
	encoded, err := mine.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	var back Capabilities
	if err := back.UnmarshalBinary(encoded); err != nil {
		t.Fatalf("what this build wrote it cannot read: %v", err)
	}
	again, err := back.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(encoded, again) {
		t.Fatal("a round trip did not reproduce the bytes")
	}
	for name, versions := range mine {
		if !bytes.Equal(back[name], versions) {
			t.Errorf("%s did not survive: %v against %v", name, back[name], versions)
		}
	}

	// Order in the map cannot change the bytes.
	shuffled := Capabilities{CapMap: {1, 2}, CapText: {8, 1}, CapList: {2, 1}}
	first, err := shuffled.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	same := Capabilities{CapText: {1, 8}, CapList: {1, 2}, CapMap: {2, 1}}
	second, err := same.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, second) {
		t.Fatal("the same claim written twice gave different bytes")
	}
}

// A capability this build has never heard of is kept and reported as not
// accepted, which is what makes naming them extensible without a registry.
func TestAnUnknownCapabilityIsKeptNotRefused(t *testing.T) {
	future := Capabilities{"something.invented.later": {1, 4}, CapText: {8}}
	encoded, err := future.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	var back Capabilities
	if err := back.UnmarshalBinary(encoded); err != nil {
		t.Fatalf("a capability this build does not know was refused: %v", err)
	}
	if !back.Accepts("something.invented.later", 4) {
		t.Error("it was not kept")
	}
	if back.Accepts("something.invented.later", 2) {
		t.Error("a version it did not name was taken as accepted")
	}
}

// Everything that is not what MarshalBinary would have written is refused.
func TestDecodingCapabilitiesRefusesEveryWayItCanBeWrong(t *testing.T) {
	good, err := Capabilities{CapList: {1, 2}, CapText: {1, 8}}.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	var probe Capabilities
	if err := probe.UnmarshalBinary(good); err != nil {
		t.Fatalf("the fixture does not decode: %v", err)
	}

	for _, c := range []struct {
		name string
		data []byte
	}{
		{"empty", nil},
		{"a count with nothing after it", []byte{2}},
		{"a count past the bytes", []byte{0xFF, 0x01}},
		{"trailing bytes", append(append([]byte(nil), good...), 0)},
		{"a truncated name", []byte{1, 9, 'x'}},
		{"an empty name", []byte{1, 0, 1, 1}},
		{"no versions", []byte{1, 4, 't', 'e', 'x', 't', 0}},
		{"versions past the bytes", []byte{1, 4, 't', 'e', 'x', 't', 0xFF, 1}},
		{"fewer versions than promised", []byte{1, 1, 'a', 2, 1}},
	} {
		t.Run(c.name, func(t *testing.T) {
			var got Capabilities
			if err := got.UnmarshalBinary(c.data); !errors.Is(err, ErrProtocol) {
				t.Fatalf("UnmarshalBinary = %v, want ErrProtocol", err)
			}
		})
	}

	// And the orderings, which are what canonical means here.
	t.Run("names out of order", func(t *testing.T) {
		data := []byte{2, 1, 'b', 1, 1, 1, 'a', 1, 1}
		var got Capabilities
		if err := got.UnmarshalBinary(data); !errors.Is(err, ErrProtocol) {
			t.Fatalf("names descending decoded: %v", err)
		}
	})
	t.Run("a name repeated", func(t *testing.T) {
		data := []byte{2, 1, 'a', 1, 1, 1, 'a', 1, 2}
		var got Capabilities
		if err := got.UnmarshalBinary(data); !errors.Is(err, ErrProtocol) {
			t.Fatalf("a repeated name decoded: %v", err)
		}
	})
	t.Run("versions out of order", func(t *testing.T) {
		data := []byte{1, 1, 'a', 2, 2, 1}
		var got Capabilities
		if err := got.UnmarshalBinary(data); !errors.Is(err, ErrProtocol) {
			t.Fatalf("versions descending decoded: %v", err)
		}
	})
	t.Run("a version repeated", func(t *testing.T) {
		data := []byte{1, 1, 'a', 2, 1, 1}
		var got Capabilities
		if err := got.UnmarshalBinary(data); !errors.Is(err, ErrProtocol) {
			t.Fatalf("a repeated version decoded: %v", err)
		}
	})
}

// Writing refuses what it could not write canonically, rather than writing it.
func TestEncodingRefusesWhatItCouldNotWriteTwice(t *testing.T) {
	if _, err := (Capabilities{"": {1}}).MarshalBinary(); !errors.Is(err, ErrProtocol) {
		t.Error("an unnamed capability was written")
	}
	if _, err := (Capabilities{CapText: {1, 1}}).MarshalBinary(); !errors.Is(err, ErrProtocol) {
		t.Error("a version said twice was written")
	}
	// A capability with nothing to say is left out, not written empty.
	out, err := (Capabilities{CapText: {1}, CapMap: nil}).MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	var back Capabilities
	if err := back.UnmarshalBinary(out); err != nil {
		t.Fatal(err)
	}
	if _, named := back[CapMap]; named {
		t.Error("a capability with no versions was written")
	}
}

// A crdt format this build has no name for is left out rather than announced
// under a made-up one. Not reachable through Mine, which only walks the formats
// crdt lists, so it is asked directly.
func TestAFormatWithNoNameIsLeftOut(t *testing.T) {
	if got := capabilityOf(crdt.Format(200)); got != "" {
		t.Errorf("capabilityOf(unknown) = %q, want empty", got)
	}
}

// capabilityOf is total over the formats crdt lists, which is what lets
// readsOurSnapshots ask about each without a special case for a nameless one.
func TestEveryFormatCrdtListsHasAName(t *testing.T) {
	for _, f := range crdt.Formats() {
		if capabilityOf(f) == "" {
			t.Errorf("crdt lists %v and this build has no name for it", f)
		}
	}
}
