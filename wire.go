package collab

import (
	"encoding/binary"
	"unicode/utf8"
)

// A session is four kinds of message, and every field in them is already a
// stretch of bytes some encoder in [github.com/go-crdt/crdt] produced and will
// check on arrival. The only thing a carrier has to do is keep them apart.
//
// That is worth saying because the obvious carrier costs more than everything
// it carries. The browser test client, compiled with -ldflags="-s -w" and
// gzipped, over each carrier:
//
//	over this framing      919 KB
//	over gRPC            4 461 KB
//
// The cost is protobuf rather than gRPC — its reflection and registry machinery
// cannot be linked away, and a build carrying protobuf without gRPC measures the
// same. For scale, the CRDT alone is 633 KB and the CRDT with this framing and a
// WebSocket is 641 KB: the carrier that was costing six times the document was
// describing fields nobody reads through it.
//
// So the browser gets this instead: a kind byte, then each field
// length-prefixed. The gRPC service is unchanged and is what a native peer still
// uses, because outside a browser none of this matters.

// The kinds a session exchanges. A client sends join, operations and presence;
// a server sends welcome, operations and presence.
const (
	kindJoin      byte = 1
	kindWelcome   byte = 2
	kindOperation byte = 3
	kindPresence  byte = 4
	// kindAcknowledge carries a participant's version, so the server can say
	// what every participant has certainly seen. Nothing depends on it
	// arriving: it is an observation, and a participant that never sends one
	// simply holds the answer back.
	kindAcknowledge byte = 5
)

// A joinMsg opens a session.
type joinMsg struct {
	// Document names the document to edit. It is opened if it does not exist.
	Document string
	// Site is the participant's replica identity; see [crdt.DeriveSiteID].
	Site uint64
	// Have is the participant's encoded version, per part, sent when rejoining a
	// document it already holds. Empty means "send everything".
	Have []byte
}

// A welcomeMsg answers a join with the state of the document at that moment.
type welcomeMsg struct {
	// Snapshot is the whole document, sent to a participant that is new to it.
	// Empty when Operations were sent instead.
	Snapshot []byte
	// Operations catches up a participant that said what it already had.
	Operations []byte
	// Presence is the state of everyone already in the document.
	Presence [][]byte
	// Version is the server's encoded version, per part. A participant answers
	// with whatever the server is missing, which is what lets work done while
	// disconnected reach everyone else rather than being stranded here.
	Version []byte
}

// An opsMsg carries operations addressed to the parts of a document, encoded by
// [crdt.AppendPartOps]. Applying them is idempotent and order-independent, so
// this needs no sequencing of its own.
type opsMsg struct{ Operations []byte }

// A presenceMsg carries one encoded awareness update. It is never persisted.
type presenceMsg struct{ Update []byte }

// An ackMsg carries a participant's encoded version, per part: what it has
// applied, as of sending. It is how the server learns what every participant
// has certainly seen, which is the one thing a stable version needs and the one
// thing a fan-out server does not otherwise know.
type ackMsg struct{ Version []byte }

// appendBytes writes a length-prefixed field.
func appendBytes(dst, field []byte) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(field)))
	return append(dst, field...)
}

// encode writes a client message.
func encodeJoin(m joinMsg) []byte {
	out := []byte{kindJoin}
	out = appendBytes(out, []byte(m.Document))
	out = binary.AppendUvarint(out, m.Site)
	return appendBytes(out, m.Have)
}

func encodeWelcome(m welcomeMsg) []byte {
	out := []byte{kindWelcome}
	out = appendBytes(out, m.Snapshot)
	out = appendBytes(out, m.Operations)
	out = appendBytes(out, m.Version)
	out = binary.AppendUvarint(out, uint64(len(m.Presence)))
	for _, p := range m.Presence {
		out = appendBytes(out, p)
	}
	return out
}

func encodeOps(m opsMsg) []byte {
	return appendBytes([]byte{kindOperation}, m.Operations)
}

func encodePresence(m presenceMsg) []byte {
	return appendBytes([]byte{kindPresence}, m.Update)
}

// A frame is a decoder over one message, refusing rather than trusting a length
// it was given.
type frame struct{ buf []byte }

func (f *frame) uvarint() (uint64, bool) {
	v, used := binary.Uvarint(f.buf)
	// An encoding longer than its value needs is refused, for the reason the
	// document formats refuse one: the same message would otherwise have more
	// than one encoding.
	if used <= 0 || (used > 1 && f.buf[used-1] == 0) {
		return 0, false
	}
	f.buf = f.buf[used:]
	return v, true
}

// bytes reads a length-prefixed field. What it returns is a window on the
// caller's buffer, so anything kept from it must be copied — which is what the
// decoders below do, since a WebSocket message is read into a buffer the
// transport may reuse.
func (f *frame) bytes() ([]byte, bool) {
	n, ok := f.uvarint()
	if !ok || n > uint64(len(f.buf)) {
		return nil, false
	}
	out := f.buf[:n]
	f.buf = f.buf[n:]
	return out, true
}

func (f *frame) copied() ([]byte, bool) {
	got, ok := f.bytes()
	if !ok {
		return nil, false
	}
	if len(got) == 0 {
		return nil, true
	}
	return append([]byte(nil), got...), true
}

// done reports whether the message ended exactly where it said it would.
// Trailing bytes are refused: a message is decoded from its own encoding and
// nothing more.
func (f *frame) done() bool { return len(f.buf) == 0 }

// decodeClient reads a message a participant sent. The kind is returned so the
// caller can tell a join from what may follow it.
func decodeClient(data []byte) (byte, any, error) {
	if len(data) == 0 {
		return 0, nil, ErrProtocol
	}
	f := &frame{buf: data[1:]}
	switch data[0] {
	case kindJoin:
		name, ok1 := f.bytes()
		site, ok2 := f.uvarint()
		have, ok3 := f.copied()
		// A document name reaches a store and a log, and crosses into
		// JavaScript, so it is held to being text like everything else here.
		if !ok1 || !ok2 || !ok3 || !f.done() || !utf8.Valid(name) {
			return 0, nil, ErrProtocol
		}
		return kindJoin, joinMsg{Document: string(name), Site: site, Have: have}, nil
	case kindOperation:
		ops, ok := f.copied()
		if !ok || !f.done() {
			return 0, nil, ErrProtocol
		}
		return kindOperation, opsMsg{Operations: ops}, nil
	case kindPresence:
		update, ok := f.copied()
		if !ok || !f.done() {
			return 0, nil, ErrProtocol
		}
		return kindPresence, presenceMsg{Update: update}, nil
	case kindAcknowledge:
		version, ok := f.copied()
		if !ok || !f.done() {
			return 0, nil, ErrProtocol
		}
		return kindAcknowledge, ackMsg{Version: version}, nil
	default:
		return 0, nil, ErrProtocol
	}
}

// decodeServer reads a message the server sent.
func decodeServer(data []byte) (byte, any, error) {
	if len(data) == 0 {
		return 0, nil, ErrProtocol
	}
	f := &frame{buf: data[1:]}
	switch data[0] {
	case kindWelcome:
		snapshot, ok1 := f.copied()
		ops, ok2 := f.copied()
		version, ok3 := f.copied()
		count, ok4 := f.uvarint()
		if !ok1 || !ok2 || !ok3 || !ok4 || count > uint64(len(f.buf)) {
			return 0, nil, ErrProtocol
		}
		presence := make([][]byte, 0, count)
		for range count {
			one, ok := f.copied()
			if !ok {
				return 0, nil, ErrProtocol
			}
			presence = append(presence, one)
		}
		if !f.done() {
			return 0, nil, ErrProtocol
		}
		return kindWelcome, welcomeMsg{
			Snapshot: snapshot, Operations: ops, Version: version, Presence: presence,
		}, nil
	case kindOperation:
		ops, ok := f.copied()
		if !ok || !f.done() {
			return 0, nil, ErrProtocol
		}
		return kindOperation, opsMsg{Operations: ops}, nil
	case kindPresence:
		update, ok := f.copied()
		if !ok || !f.done() {
			return 0, nil, ErrProtocol
		}
		return kindPresence, presenceMsg{Update: update}, nil
	default:
		return 0, nil, ErrProtocol
	}
}

// encodeClient writes one message a participant sends.
func encodeClient(kind byte, msg any) ([]byte, error) {
	switch kind {
	case kindJoin:
		m, ok := msg.(joinMsg)
		if !ok {
			return nil, ErrProtocol
		}
		return encodeJoin(m), nil
	case kindOperation:
		m, ok := msg.(opsMsg)
		if !ok {
			return nil, ErrProtocol
		}
		return encodeOps(m), nil
	case kindPresence:
		m, ok := msg.(presenceMsg)
		if !ok {
			return nil, ErrProtocol
		}
		return encodePresence(m), nil
	case kindAcknowledge:
		m, ok := msg.(ackMsg)
		if !ok {
			return nil, ErrProtocol
		}
		return encodeAck(m), nil
	default:
		return nil, ErrProtocol
	}
}

// encodeAck writes a participant's version.
func encodeAck(m ackMsg) []byte {
	return appendBytes([]byte{kindAcknowledge}, m.Version)
}

// encodeServer writes one message a server sends.
func encodeServer(kind byte, msg any) ([]byte, error) {
	switch kind {
	case kindWelcome:
		m, ok := msg.(welcomeMsg)
		if !ok {
			return nil, ErrProtocol
		}
		return encodeWelcome(m), nil
	case kindOperation:
		m, ok := msg.(opsMsg)
		if !ok {
			return nil, ErrProtocol
		}
		return encodeOps(m), nil
	case kindPresence:
		m, ok := msg.(presenceMsg)
		if !ok {
			return nil, ErrProtocol
		}
		return encodePresence(m), nil
	default:
		return nil, ErrProtocol
	}
}
