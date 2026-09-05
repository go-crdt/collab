//go:build (js && wasm) || !js

package collab

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"sort"

	"github.com/go-crdt/crdt"
)

// sitesMagic marks a participants file that carries a checksum of what follows
// it. A file written before this existed is recognised by not starting with it
// and read as it is; it gains the checksum the next time it is saved, one
// document at a time, with nothing to migrate. It is in the same family as
// crdt's own "crdtc" and dircompress's "crdtz" and "crdth", and deliberately
// none of them.
//
// # Why these bytes carry a checksum at all
//
// Rot, not forgery: anything that can write the file can recompute any checksum
// in it, so this is not authentication and does not pretend to be. That is
// dircompress.go's reasoning, and CRC32C is its choice for its reasons — one
// hardware instruction on every architecture this builds for.
//
// What is different here is which way a corruption points. A document that rots
// is a document nobody wrote; a participants file that rots can be a collect
// floor nobody wrote, and a floor does harm by moving UP. decodeSites' checks
// below are structure only, and dense varints have almost no redundancy to trip
// over: measured on a four-site file, 358 of 784 single-bit flips — 45.7% —
// decoded cleanly into a DIFFERENT participant set, where the same measurement
// on a document's file gave 10.7%. The one file here with nothing underneath it
// was the one four times more exposed. A CRC32C detects every single-bit error
// and every burst up to 32 bits, which is every one of those 358.
//
// # What it is not, and what is left
//
// The checksum is over the encoded body, where pack() checksums what it
// decompresses to. There is no compressor in between here, so the stored bytes
// are the encoding and the corruption that "survives the decompressor" has no
// equivalent.
//
// And it does not bind the document: a file restored from the wrong backup, or
// swapped with another document's, is misdelivery rather than rot, and nothing
// here would notice. Naming it rather than implying it is covered.
//
// The residual is one file: a participants file written before this existed
// whose first five bytes happened to be these. It would have to say it holds 99
// sites whose lowest id is 114, followed by a 100-byte version blob beginning
// 't', 's'. If one existed its checksum would fail and it would be refused,
// never misread — the safe direction.
var sitesMagic = [...]byte{'c', 'r', 'd', 't', 's'}

// What a document remembers about the people who have been in it, written down
// so that letting go of the document does not forget them. See [SiteStore].
//
// One entry per site: what it last acknowledged holding, and how far its clocks
// had counted. Either may be absent, which is what a site that has joined and
// said nothing looks like — and a site like that holds the floor at nothing,
// which is the answer that keeps a document collectable only when it should be.
//
// Sorted by site, so that two servers holding the same thing write the same
// bytes and a store can be compared with itself — which a checksum of those
// bytes leaves exactly as it was, since the same body gives the same four
// bytes.
func encodeSites(seen map[crdt.SiteID]crdt.CompositeVersion, reached map[crdt.SiteID]crdt.CompositeClocks) ([]byte, error) {
	sites := make([]crdt.SiteID, 0, len(seen))
	for site := range seen {
		sites = append(sites, site)
	}
	sort.Slice(sites, func(i, j int) bool { return sites[i] < sites[j] })

	body := binary.AppendUvarint(nil, uint64(len(sites)))
	for _, site := range sites {
		body = binary.AppendUvarint(body, uint64(site))
		var version, clocks []byte
		if v := seen[site]; v != nil {
			raw, err := v.MarshalBinary()
			if err != nil {
				return nil, err
			}
			version = raw
		}
		if c := reached[site]; c != nil {
			raw, err := c.MarshalBinary()
			if err != nil {
				return nil, err
			}
			clocks = raw
		}
		body = binary.AppendUvarint(body, uint64(len(version)))
		body = append(body, version...)
		body = binary.AppendUvarint(body, uint64(len(clocks)))
		body = append(body, clocks...)
	}
	out := make([]byte, 0, len(sitesMagic)+4+len(body))
	out = append(out, sitesMagic[:]...)
	out = binary.BigEndian.AppendUint32(out, crc32.Checksum(body, checksumTable))
	return append(out, body...), nil
}

// decodeSites reads what encodeSites wrote, and refuses anything else: these
// bytes come back from a store, which is somewhere else and may have been
// anywhere. Every refusal is [crdt.ErrMalformed], and the message says which
// one it was — a file that has rotted and a file that is not one at all are the
// same answer to the server and different news to whoever has to act on it.
//
// A file written before there was a checksum has no magic, and is read as it
// is. See [sitesMagic].
func decodeSites(in []byte) (map[crdt.SiteID]crdt.CompositeVersion, map[crdt.SiteID]crdt.CompositeClocks, error) {
	if hasMagic(in, sitesMagic) {
		body := in[len(sitesMagic):]
		if len(body) < 4 {
			return nil, nil, fmt.Errorf("collab: checksummed participants of %d bytes, which is not enough for one: %w", len(in), crdt.ErrMalformed)
		}
		want := binary.BigEndian.Uint32(body[:4])
		if got := crc32.Checksum(body[4:], checksumTable); got != want {
			// Refusing is the whole point. Accepted instead, this is a collect
			// floor nobody wrote, and a floor that has moved up is written into
			// the next snapshot as CollectedBelow and never comes back.
			return nil, nil, fmt.Errorf("collab: the participants do not match their checksum (%08x, want %08x); they have been corrupted: %w", got, want, crdt.ErrMalformed)
		}
		in = body[4:]
	}
	r := &siteReader{buf: in}
	n, ok := r.uvarint()
	if !ok || n > uint64(len(r.buf)) {
		return nil, nil, crdt.ErrMalformed
	}
	seen := map[crdt.SiteID]crdt.CompositeVersion{}
	reached := map[crdt.SiteID]crdt.CompositeClocks{}
	var last uint64
	for i := uint64(0); i < n; i++ {
		site, ok := r.uvarint()
		if !ok {
			return nil, nil, crdt.ErrMalformed
		}
		if i > 0 && site <= last {
			// Out of order, or one site named twice: not something encodeSites
			// writes, and accepting it would make two encodings of one set.
			return nil, nil, crdt.ErrMalformed
		}
		last = site
		version, okVersion := r.sized()
		clocks, okClocks := r.sized()
		if !okVersion || !okClocks {
			return nil, nil, crdt.ErrMalformed
		}
		if len(version) > 0 {
			var v crdt.CompositeVersion
			if err := v.UnmarshalBinary(version); err != nil {
				return nil, nil, crdt.ErrMalformed
			}
			seen[crdt.SiteID(site)] = v
		} else {
			seen[crdt.SiteID(site)] = nil
		}
		if len(clocks) > 0 {
			var c crdt.CompositeClocks
			if err := c.UnmarshalBinary(clocks); err != nil {
				return nil, nil, crdt.ErrMalformed
			}
			reached[crdt.SiteID(site)] = c
		}
	}
	if len(r.buf) != 0 {
		return nil, nil, crdt.ErrMalformed
	}
	return seen, reached, nil
}

// siteReader is the little reader these two need; the wire has its own and it
// is for frames rather than for bytes from a store.
type siteReader struct{ buf []byte }

func (r *siteReader) uvarint() (uint64, bool) {
	v, used := binary.Uvarint(r.buf)
	if used <= 0 {
		return 0, false
	}
	r.buf = r.buf[used:]
	return v, true
}

func (r *siteReader) sized() ([]byte, bool) {
	n, ok := r.uvarint()
	if !ok || n > uint64(len(r.buf)) {
		return nil, false
	}
	out := r.buf[:n]
	r.buf = r.buf[n:]
	return out, true
}
