//go:build !js

package collab

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/coder/websocket"
)

// A carrier fails in ways a session has to survive telling apart: a server that
// is not there, a peer speaking something else, and bytes that are not a
// message. These are those paths, on the carrier itself, because a session test
// would only ever see them as "the session ended".

// serveRaw runs fn against one accepted WebSocket, so a test can be the server.
func serveRaw(t *testing.T, fn func(ctx context.Context, conn *websocket.Conn)) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{OriginPatterns: []string{"*"}})
		if err != nil {
			return
		}
		defer conn.CloseNow()
		fn(r.Context(), conn)
	}))
	t.Cleanup(srv.Close)
	return "ws" + strings.TrimPrefix(srv.URL, "http")
}

func TestDiallingSomethingThatIsNotThere(t *testing.T) {
	_, err := WebSocket("ws://127.0.0.1:1").open(t.Context())
	if !errors.Is(err, ErrTransport) {
		t.Fatalf("dialling a closed port = %v, want ErrTransport", err)
	}
}

func TestACarrierRefusesWhatIsNotAMessage(t *testing.T) {
	t.Run("a text frame", func(t *testing.T) {
		url := serveRaw(t, func(ctx context.Context, conn *websocket.Conn) {
			_ = conn.Write(ctx, websocket.MessageText, []byte("hello"))
		})
		conn, err := WebSocket(url).open(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		if _, _, err := conn.Recv(); !errors.Is(err, ErrProtocol) {
			t.Fatalf("a text frame = %v, want ErrProtocol", err)
		}
	})

	t.Run("bytes that are not a message", func(t *testing.T) {
		url := serveRaw(t, func(ctx context.Context, conn *websocket.Conn) {
			_ = conn.Write(ctx, websocket.MessageBinary, []byte{99})
		})
		conn, err := WebSocket(url).open(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		if _, _, err := conn.Recv(); !errors.Is(err, ErrProtocol) {
			t.Fatalf("an unknown kind = %v, want ErrProtocol", err)
		}
	})

	t.Run("a message the carrier cannot encode", func(t *testing.T) {
		url := serveRaw(t, func(ctx context.Context, conn *websocket.Conn) {
			<-ctx.Done()
		})
		conn, err := WebSocket(url).open(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		if err := conn.Send(kindWelcome, welcomeMsg{}); !errors.Is(err, ErrProtocol) {
			t.Fatalf("sending a welcome from a client = %v, want ErrProtocol", err)
		}
	})
}

// The server side of the same: a participant that speaks something else is told
// why, in the only place a WebSocket has to say it.
func TestTheServerRefusesACarrierThatSpeaksSomethingElse(t *testing.T) {
	srv := NewServer(Config{})
	front := httptest.NewServer(srv.ServeWebSocket("*"))
	t.Cleanup(front.Close)
	url := "ws" + strings.TrimPrefix(front.URL, "http")

	for _, tt := range []struct {
		name  string
		write func(context.Context, *websocket.Conn) error
	}{
		{"a text frame", func(ctx context.Context, c *websocket.Conn) error {
			return c.Write(ctx, websocket.MessageText, []byte("hello"))
		}},
		{"bytes that are not a message", func(ctx context.Context, c *websocket.Conn) error {
			return c.Write(ctx, websocket.MessageBinary, []byte{99})
		}},
		{"something other than a join", func(ctx context.Context, c *websocket.Conn) error {
			raw, _ := encodeClient(kindPresence, presenceMsg{Update: []byte("x")})
			return c.Write(ctx, websocket.MessageBinary, raw)
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			conn, _, err := websocket.Dial(t.Context(), url, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.CloseNow()
			if err := tt.write(t.Context(), conn); err != nil {
				t.Fatal(err)
			}
			if _, _, err := conn.Read(t.Context()); err == nil {
				t.Fatal("the server kept the session open")
			}
		})
	}
}

// A close frame carries 123 bytes of reason and is dropped entirely if given
// more, so a long refusal has to be cut rather than lost.
//
// The case where a session ends with no reason at all is here rather than in a
// session test because this carrier cannot produce one: a session ends when its
// carrier fails or its participant is displaced, and both carry a reason. The
// branch is kept, and tested where it can be, so that it is right if that ever
// stops being true.
func TestHowASessionsEndReachesTheParticipant(t *testing.T) {
	code, reason := closing(nil)
	if code != websocket.StatusNormalClosure || reason != "" {
		t.Fatalf("closing(nil) = %v, %q", code, reason)
	}
	code, reason = closing(errors.New("no"))
	if code != websocket.StatusPolicyViolation || reason != "no" {
		t.Fatalf("closing(err) = %v, %q", code, reason)
	}
	if _, long := closing(errors.New(strings.Repeat("x", 400))); len(long) != 120 {
		t.Fatalf("a long reason came back %d bytes long", len(long))
	}
}

// A session ends rather than guesses when the server sends something that
// cannot arrive at that moment.
func TestAClientRefusesAMessageThatCannotArrive(t *testing.T) {
	c := &Client{}
	for _, tt := range []struct {
		name string
		kind byte
		msg  any
	}{
		{"a second welcome", kindWelcome, welcomeMsg{}},
		{"a join", kindJoin, joinMsg{}},
		{"operations that are not operations", kindOperation, joinMsg{}},
		{"presence that is not presence", kindPresence, joinMsg{}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if err := c.absorb(tt.kind, tt.msg); !errors.Is(err, ErrProtocol) {
				t.Fatalf("absorb(%d) = %v, want ErrProtocol", tt.kind, err)
			}
		})
	}
}

// The gRPC carrier refuses the same shapes, so neither carrier is the lenient
// one.
func TestTheGRPCCarrierRefusesTheSameShapes(t *testing.T) {
	c := &grpcConn{}
	for _, tt := range []struct {
		kind byte
		msg  any
	}{
		{kindJoin, opsMsg{}},
		{kindOperation, joinMsg{}},
		{kindPresence, joinMsg{}},
		{kindAcknowledge, joinMsg{}},
		{kindWelcome, welcomeMsg{}},
		{99, joinMsg{}},
	} {
		if err := c.Send(tt.kind, tt.msg); !errors.Is(err, ErrProtocol) {
			t.Errorf("Send(%d, %T) = %v, want ErrProtocol", tt.kind, tt.msg, err)
		}
	}
}

// An origin the server does not allow is refused at the handshake, which is the
// check that stops another site's page from opening a session with the
// visitor's cookies.
func TestAnOriginTheServerDoesNotAllowIsRefused(t *testing.T) {
	srv := NewServer(Config{})
	front := httptest.NewServer(srv.ServeWebSocket("collab.example"))
	t.Cleanup(front.Close)
	url := "ws" + strings.TrimPrefix(front.URL, "http")

	header := http.Header{}
	header.Set("Origin", "https://somewhere.else.example")
	if _, _, err := websocket.Dial(t.Context(), url, &websocket.DialOptions{HTTPHeader: header}); err == nil {
		t.Fatal("a session was opened from an origin that is not allowed")
	}
	// The control: the origin it does allow gets in.
	header.Set("Origin", "https://collab.example")
	conn, _, err := websocket.Dial(t.Context(), url, &websocket.DialOptions{HTTPHeader: header})
	if err != nil {
		t.Fatalf("the allowed origin was refused: %v", err)
	}
	_ = conn.CloseNow()
}

// A message of a kind that does not exist is refused rather than written as
// something else. Nothing produces one; the branches exist so that nothing ever
// can, and there are two of them now: the WebSocket carrier refuses through the
// wire encoder it shares with the client, and the gRPC one refuses in its own
// conversion.
func TestACarrierRefusesAMessageOfNoKind(t *testing.T) {
	ws := &wsCarrier{ctx: t.Context()}
	if err := ws.Send(0xfe, opsMsg{}); !errors.Is(err, ErrProtocol) {
		t.Fatalf("the WebSocket carrier sent a message of no kind: %v", err)
	}
	g := &grpcCarrier{}
	if err := g.Send(0xfe, opsMsg{}); !errors.Is(err, ErrProtocol) {
		t.Fatalf("the gRPC carrier sent a message of no kind: %v", err)
	}
}
