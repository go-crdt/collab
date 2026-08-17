//go:build !js

package collab

import (
	"bytes"
	"encoding/binary"
	"errors"
	"net/http"
	"reflect"
	"testing"
)

// The framing is a decoder, and a decoder reads what a peer sent. So it is held
// to what every decoder in this stack is held to: what it accepts re-encodes to
// the bytes it was given, and everything else is refused rather than read
// half-way.

func TestAMessageComesBackAsItWentOut(t *testing.T) {
	join := joinMsg{Document: "file:main.tex", Site: 1 << 40, Have: []byte{1, 2, 3}}
	welcome := welcomeMsg{
		Snapshot:   []byte("snap"),
		Operations: []byte("ops"),
		Version:    []byte("ver"),
		Presence:   [][]byte{[]byte("a"), []byte("bb")},
	}
	ops := opsMsg{Operations: []byte("batch")}
	presence := presenceMsg{Update: []byte("cursor")}

	t.Run("client", func(t *testing.T) {
		for _, tt := range []struct {
			kind byte
			msg  any
		}{{kindJoin, join}, {kindOperation, ops}, {kindPresence, presence}} {
			raw, err := encodeClient(tt.kind, tt.msg)
			if err != nil {
				t.Fatalf("encodeClient(%d): %v", tt.kind, err)
			}
			kind, got, err := decodeClient(raw)
			if err != nil || kind != tt.kind {
				t.Fatalf("decodeClient = %d, %v, %v", kind, got, err)
			}
			if !reflect.DeepEqual(got, tt.msg) {
				t.Fatalf("round trip gave %+v, want %+v", got, tt.msg)
			}
			again, err := encodeClient(kind, got)
			if err != nil || !bytes.Equal(again, raw) {
				t.Fatalf("re-encoding gave %x, want %x (%v)", again, raw, err)
			}
		}
	})

	t.Run("server", func(t *testing.T) {
		for _, tt := range []struct {
			kind byte
			msg  any
		}{{kindWelcome, welcome}, {kindOperation, ops}, {kindPresence, presence}} {
			raw, err := encodeServer(tt.kind, tt.msg)
			if err != nil {
				t.Fatalf("encodeServer(%d): %v", tt.kind, err)
			}
			kind, got, err := decodeServer(raw)
			if err != nil || kind != tt.kind {
				t.Fatalf("decodeServer = %d, %v, %v", kind, got, err)
			}
			if !reflect.DeepEqual(got, tt.msg) {
				t.Fatalf("round trip gave %+v, want %+v", got, tt.msg)
			}
			again, err := encodeServer(kind, got)
			if err != nil || !bytes.Equal(again, raw) {
				t.Fatalf("re-encoding gave %x, want %x (%v)", again, raw, err)
			}
		}
	})

	// A welcome with nobody in the document yet, which is the common one.
	raw, err := encodeServer(kindWelcome, welcomeMsg{Snapshot: []byte("s"), Version: []byte("v")})
	if err != nil {
		t.Fatal(err)
	}
	_, got, err := decodeServer(raw)
	if err != nil {
		t.Fatal(err)
	}
	if w := got.(welcomeMsg); len(w.Presence) != 0 || w.Operations != nil {
		t.Fatalf("an empty welcome decoded to %+v", w)
	}
}

// An encoder that is handed the wrong shape for its kind refuses rather than
// writing bytes the far end would reject.
func TestEncodingRefusesWhatItCannotBe(t *testing.T) {
	for _, kind := range []byte{kindJoin, kindOperation, kindPresence, 0, 99} {
		if _, err := encodeClient(kind, "not a message"); !errors.Is(err, ErrProtocol) {
			t.Errorf("encodeClient(%d, string) = %v, want ErrProtocol", kind, err)
		}
	}
	for _, kind := range []byte{kindWelcome, kindOperation, kindPresence, 0, 99} {
		if _, err := encodeServer(kind, "not a message"); !errors.Is(err, ErrProtocol) {
			t.Errorf("encodeServer(%d, string) = %v, want ErrProtocol", kind, err)
		}
	}
	// A server never sends a join, and a client never sends a welcome.
	if _, err := encodeClient(kindWelcome, welcomeMsg{}); !errors.Is(err, ErrProtocol) {
		t.Errorf("a client encoding a welcome = %v", err)
	}
	if _, err := encodeServer(kindJoin, joinMsg{}); !errors.Is(err, ErrProtocol) {
		t.Errorf("a server encoding a join = %v", err)
	}
}

func TestDecodingRefusesEveryWayItCanBeWrong(t *testing.T) {
	good, err := encodeClient(kindJoin, joinMsg{Document: "d", Site: 2, Have: []byte("h")})
	if err != nil {
		t.Fatal(err)
	}
	welcome, err := encodeServer(kindWelcome, welcomeMsg{
		Snapshot: []byte("s"), Version: []byte("v"), Presence: [][]byte{[]byte("p")},
	})
	if err != nil {
		t.Fatal(err)
	}

	client := map[string][]byte{
		"nothing at all":             {},
		"a kind nobody sends":        {99},
		"a kind only a server sends": {kindWelcome},
		"truncated mid-field":        good[:len(good)-1],
		"trailing bytes":             append(append([]byte{}, good...), 0),
		"a length past the end":      {kindOperation, 0xff, 0x01},
		"a padded length":            {kindPresence, 0x81, 0x00},
		"a name that is not text": func() []byte {
			out := []byte{kindJoin}
			out = appendBytes(out, []byte{0xff, 0xfe})
			out = binary.AppendUvarint(out, 1)
			return appendBytes(out, nil)
		}(),
		"an operation batch cut short": {kindOperation, 4, 1, 2},
	}
	for name, raw := range client {
		t.Run("client: "+name, func(t *testing.T) {
			if _, _, err := decodeClient(raw); !errors.Is(err, ErrProtocol) {
				t.Fatalf("decodeClient(%x) = %v, want ErrProtocol", raw, err)
			}
		})
	}

	server := map[string][]byte{
		"nothing at all":             {},
		"a kind nobody sends":        {99},
		"a kind only a client sends": {kindJoin},
		"truncated mid-field":        welcome[:len(welcome)-1],
		"trailing bytes":             append(append([]byte{}, welcome...), 0),
		"more presence than bytes":   {kindWelcome, 0, 0, 0, 0xff, 0x01},
		"a presence entry cut short": {kindWelcome, 0, 0, 0, 1, 9, 1},
		"a padded presence count":    {kindWelcome, 0, 0, 0, 0x81, 0x00},
		"an operation cut short":     {kindOperation, 4, 1},
		"presence cut short":         {kindPresence, 4, 1},
	}
	for name, raw := range server {
		t.Run("server: "+name, func(t *testing.T) {
			if _, _, err := decodeServer(raw); !errors.Is(err, ErrProtocol) {
				t.Fatalf("decodeServer(%x) = %v, want ErrProtocol", raw, err)
			}
		})
	}

	// The control: the two messages the rejections were cut from are accepted.
	if _, _, err := decodeClient(good); err != nil {
		t.Fatalf("the join the truncations came from was refused: %v", err)
	}
	if _, _, err := decodeServer(welcome); err != nil {
		t.Fatalf("the welcome the truncations came from was refused: %v", err)
	}
}

// Nothing decoded may point into the buffer it came from: a WebSocket message is
// read into a buffer the transport is free to reuse for the next one.
func TestDecodingKeepsNothingOfTheCallersBytes(t *testing.T) {
	raw, err := encodeClient(kindOperation, opsMsg{Operations: []byte("batch")})
	if err != nil {
		t.Fatal(err)
	}
	_, got, err := decodeClient(raw)
	if err != nil {
		t.Fatal(err)
	}
	for i := range raw {
		raw[i] = 0xff
	}
	if op := got.(opsMsg); !bytes.Equal(op.Operations, []byte("batch")) {
		t.Fatalf("the decoded message holds %q: it pointed into the caller's bytes", op.Operations)
	}
}

// The option a browser has no use for still has to work where it does.
func TestWithHTTPHeaderIsCarriedToTheHandshake(t *testing.T) {
	header := http.Header{}
	header.Set("Cookie", "session=ada")
	tr, ok := WebSocket("ws://example.invalid", WithHTTPHeader(header)).(*wsTransport)
	if !ok {
		t.Fatalf("WebSocket returned %T", tr)
	}
	if got := tr.header["Cookie"]; len(got) != 1 || got[0] != "session=ada" {
		t.Fatalf("the transport carries %v", tr.header)
	}
}

func FuzzDecodeClient(f *testing.F) {
	seed, _ := encodeClient(kindJoin, joinMsg{Document: "d", Site: 1, Have: []byte("h")})
	f.Add(seed)
	seed, _ = encodeClient(kindOperation, opsMsg{Operations: []byte("o")})
	f.Add(seed)
	seed, _ = encodeClient(kindPresence, presenceMsg{Update: []byte("u")})
	f.Add(seed)
	f.Fuzz(func(t *testing.T, data []byte) {
		kind, msg, err := decodeClient(data)
		if err != nil {
			return
		}
		// Every freedom the format has is spent, so what was accepted encodes
		// back to itself — the bytes, not merely an equivalent message.
		again, err := encodeClient(kind, msg)
		if err != nil {
			t.Fatalf("a decoded message would not encode: %v", err)
		}
		if !bytes.Equal(again, data) {
			t.Fatalf("re-encoding %x gave %x", data, again)
		}
	})
}

func FuzzDecodeServer(f *testing.F) {
	seed, _ := encodeServer(kindWelcome, welcomeMsg{
		Snapshot: []byte("s"), Operations: []byte("o"), Version: []byte("v"),
		Presence: [][]byte{[]byte("p")},
	})
	f.Add(seed)
	seed, _ = encodeServer(kindOperation, opsMsg{Operations: []byte("o")})
	f.Add(seed)
	seed, _ = encodeServer(kindPresence, presenceMsg{Update: []byte("u")})
	f.Add(seed)
	f.Fuzz(func(t *testing.T, data []byte) {
		kind, msg, err := decodeServer(data)
		if err != nil {
			return
		}
		again, err := encodeServer(kind, msg)
		if err != nil {
			t.Fatalf("a decoded message would not encode: %v", err)
		}
		if !bytes.Equal(again, data) {
			t.Fatalf("re-encoding %x gave %x", data, again)
		}
	})
}
