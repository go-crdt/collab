//go:build (js && wasm) || !js

package collab

import (
	"encoding/binary"
	"sort"

	"github.com/go-crdt/crdt"
)

// What a document remembers about the people who have been in it, written down
// so that letting go of the document does not forget them. See [SiteStore].
//
// One entry per site: what it last acknowledged holding, and how far its clocks
// had counted. Either may be absent, which is what a site that has joined and
// said nothing looks like — and a site like that holds the floor at nothing,
// which is the answer that keeps a document collectable only when it should be.
//
// Sorted by site, so that two servers holding the same thing write the same
// bytes and a store can be compared with itself.
func encodeSites(seen map[crdt.SiteID]crdt.CompositeVersion, reached map[crdt.SiteID]crdt.CompositeClocks) ([]byte, error) {
	sites := make([]crdt.SiteID, 0, len(seen))
	for site := range seen {
		sites = append(sites, site)
	}
	sort.Slice(sites, func(i, j int) bool { return sites[i] < sites[j] })

	out := binary.AppendUvarint(nil, uint64(len(sites)))
	for _, site := range sites {
		out = binary.AppendUvarint(out, uint64(site))
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
		out = binary.AppendUvarint(out, uint64(len(version)))
		out = append(out, version...)
		out = binary.AppendUvarint(out, uint64(len(clocks)))
		out = append(out, clocks...)
	}
	return out, nil
}

// decodeSites reads what encodeSites wrote, and refuses anything else: these
// bytes come back from a store, which is somewhere else and may have been
// anywhere.
func decodeSites(in []byte) (map[crdt.SiteID]crdt.CompositeVersion, map[crdt.SiteID]crdt.CompositeClocks, error) {
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
