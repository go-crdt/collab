//go:build (js && wasm) || !js

package collab

import (
	"encoding/binary"
	"sort"

	"github.com/go-crdt/crdt"
)

// A Capability names something a peer may understand. Named rather than
// numbered so that a peer meeting one it has never heard of can say nothing
// about it and carry on, which is what makes this extensible without a registry
// everyone has to agree on first.
type Capability string

// The capabilities this build knows how to talk about. Each is a snapshot
// encoding, and each moves at its own pace: a change to how a text is written
// does not disturb a map.
//
// This list will grow. The next entry is already known -- a text operation kind
// a peer refuses if it does not understand it (crdt#80) -- and it is the reason
// this says capabilities with versions rather than one number for the snapshot
// format. A mechanism built for the one and stretched to the other afterwards is
// how a protocol ends up with two of everything.
const (
	CapText      Capability = "snapshot.text"
	CapList      Capability = "snapshot.list"
	CapMap       Capability = "snapshot.map"
	CapComposite Capability = "snapshot.composite"
)

// Capabilities is what a peer says it understands: for each capability, every
// version of it the peer accepts.
//
// A version set rather than a highest version, because they are not always
// contiguous: crdt reserves version 7 of a text for work on another branch and
// refuses it, so a build reading 1 to 6 and 8 told a peer "up to 8" would be
// sent a 7 and refuse it.
type Capabilities map[Capability][]byte

// Mine is what this build understands, taken from crdt rather than from a list
// here that would have to be kept in step with it.
func Mine() Capabilities {
	out := Capabilities{}
	for _, f := range crdt.Formats() {
		if versions := crdt.Reads(f); len(versions) > 0 {
			out[capabilityOf(f)] = versions
		}
	}
	return out
}

// capabilityOf names a crdt snapshot format. A format this build does not know
// gets no name and is left out rather than announced under a made-up one.
func capabilityOf(f crdt.Format) Capability {
	switch f {
	case crdt.FormatText:
		return CapText
	case crdt.FormatList:
		return CapList
	case crdt.FormatMap:
		return CapMap
	case crdt.FormatComposite:
		return CapComposite
	}
	return ""
}

// Accepts reports whether the peer said it understands this version of this
// capability.
//
// A capability the peer never named is not accepted. Saying nothing about
// something is not the same as accepting it, and treating it as acceptance is
// how a negotiation becomes a formality -- the peer that says nothing at all is
// exactly the old build this exists to protect.
func (c Capabilities) Accepts(name Capability, version byte) bool {
	for _, v := range c[name] {
		if v == version {
			return true
		}
	}
	return false
}

// MarshalBinary writes what a peer says it understands.
//
// Canonical: capabilities by name ascending, versions ascending within each,
// neither repeated and neither empty. Two peers that understand the same things
// therefore say so in the same bytes, which is what lets a test compare an
// advertisement with itself and a reader refuse anything that could have been
// written two ways.
//
// A capability with no versions is left out rather than written empty. Saying
// "I understand this, in no version" is saying nothing, and a wire with two ways
// to say nothing has one too many.
func (c Capabilities) MarshalBinary() ([]byte, error) {
	names := make([]Capability, 0, len(c))
	for name, versions := range c {
		if name == "" {
			return nil, ErrProtocol
		}
		if len(versions) > 0 {
			names = append(names, name)
		}
	}
	sort.Slice(names, func(i, j int) bool { return names[i] < names[j] })

	out := binary.AppendUvarint(nil, uint64(len(names)))
	for _, name := range names {
		out = binary.AppendUvarint(out, uint64(len(name)))
		out = append(out, name...)

		versions := append([]byte(nil), c[name]...)
		sort.Slice(versions, func(i, j int) bool { return versions[i] < versions[j] })
		out = binary.AppendUvarint(out, uint64(len(versions)))
		var last byte
		for _, v := range versions {
			if v == last {
				// A version said twice is the same claim twice, and there is no
				// reading of the second that differs from the first.
				return nil, ErrProtocol
			}
			out = append(out, v)
			last = v
		}
	}
	return out, nil
}

// UnmarshalBinary reads what a peer said, and refuses anything that is not what
// this would have written.
//
// A peer naming a capability this build has never heard of is not an error: it
// is kept and answered about with "not accepted", which is the whole reason
// these are named rather than numbered. Only a malformed encoding is refused.
func (c *Capabilities) UnmarshalBinary(data []byte) error {
	f := &frame{buf: data}
	count, ok := f.uvarint()
	if !ok || count > uint64(len(f.buf)) {
		return ErrProtocol
	}
	out := make(Capabilities, count)
	var lastName Capability
	for range count {
		name, ok := f.bytes()
		if !ok || len(name) == 0 {
			return ErrProtocol
		}
		this := Capability(name)
		if lastName != "" && this <= lastName {
			return ErrProtocol
		}
		lastName = this

		n, ok := f.uvarint()
		if !ok || n == 0 || n > uint64(len(f.buf)) {
			return ErrProtocol
		}
		// n was held to what is left, so these bytes are there: reading them
		// one at a time through a helper that checks again would be a line no
		// test could reach.
		versions := append([]byte(nil), f.buf[:n]...)
		f.buf = f.buf[n:]
		for i, v := range versions {
			if i > 0 && v <= versions[i-1] {
				return ErrProtocol
			}
		}
		out[this] = versions
	}
	if !f.done() {
		return ErrProtocol
	}
	*c = out
	return nil
}

// readsOurSnapshots reports whether a peer that said this can read a composite
// snapshot written by this build.
//
// Every format in one: a composite embeds a text, a list and a map, so a peer
// that reads composites and not texts reads nothing this would send it. A peer
// that said nothing reads nothing as far as this can tell, which is the answer
// that keeps it safe.
func readsOurSnapshots(said []byte) bool {
	if len(said) == 0 {
		return false
	}
	var speaks Capabilities
	if err := speaks.UnmarshalBinary(said); err != nil {
		// A peer whose advertisement cannot be read has not said anything this
		// can act on. Refusing the session over it would turn a garbled
		// courtesy into an outage.
		return false
	}
	for _, f := range crdt.Formats() {
		// Every format crdt lists has a name here -- capabilityOf is total over
		// crdt.Formats and a test says so -- so an unnamed one would be a format
		// added there and not here, and the honest answer for it is that this
		// build cannot say whether the peer reads it.
		if !speaks.Accepts(capabilityOf(f), crdt.Writes(f)) {
			return false
		}
	}
	return true
}
