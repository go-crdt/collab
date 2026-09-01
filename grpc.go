//go:build !js

package collab

import (
	"context"
	"errors"

	"github.com/go-crdt/collab/collabpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GRPC returns a transport that opens sessions on a gRPC connection.
//
// It is deliberately not the default. Everything it carries is bytes some
// encoder in [github.com/go-crdt/crdt] produced, so protobuf is describing
// fields nobody reads through it — and compiled for a browser it costs six
// times the CRDT itself. Outside a browser that does not matter, and gRPC
// brings deadlines, interceptors and the tooling built around them, which is
// reason enough to keep it. See [Transport] and [WebSocket].
func GRPC(conn grpc.ClientConnInterface) Transport { return &grpcTransport{conn: conn} }

type grpcTransport struct{ conn grpc.ClientConnInterface }

func (t *grpcTransport) open(ctx context.Context) (carrierConn, error) {
	stream, err := collabpb.NewCollabClient(t.conn).Session(ctx)
	if err != nil {
		return nil, err
	}
	return &grpcConn{stream: stream}, nil
}

// A grpcConn translates between the messages a session is made of and the
// protobuf carrying them. Every field is bytes on both sides, so the
// translation is a rename.
type grpcConn struct{ stream collabpb.Collab_SessionClient }

func (c *grpcConn) Send(kind byte, msg any) error {
	out := &collabpb.ClientMessage{}
	switch kind {
	case kindJoin:
		m, ok := msg.(joinMsg)
		if !ok {
			return ErrProtocol
		}
		out.Body = &collabpb.ClientMessage_Join{Join: &collabpb.Join{
			Document: m.Document, Site: m.Site, Have: m.Have, Speaks: m.Speaks,
		}}
	case kindOperation:
		m, ok := msg.(opsMsg)
		if !ok {
			return ErrProtocol
		}
		out.Body = &collabpb.ClientMessage_Operations{
			Operations: &collabpb.Operations{Operations: m.Operations},
		}
	case kindPresence:
		m, ok := msg.(presenceMsg)
		if !ok {
			return ErrProtocol
		}
		out.Body = &collabpb.ClientMessage_Presence{
			Presence: &collabpb.Presence{Update: m.Update},
		}
	case kindAcknowledge:
		m, ok := msg.(ackMsg)
		if !ok {
			return ErrProtocol
		}
		out.Body = &collabpb.ClientMessage_Acknowledge{
			Acknowledge: &collabpb.Acknowledge{Version: m.Version, Clocks: m.Clocks},
		}
	default:
		return ErrProtocol
	}
	return c.stream.Send(out)
}

func (c *grpcConn) Recv() (byte, any, error) {
	in, err := c.stream.Recv()
	if err != nil {
		return 0, nil, err
	}
	switch body := in.GetBody().(type) {
	case *collabpb.ServerMessage_Welcome:
		w := body.Welcome
		return kindWelcome, welcomeMsg{
			Snapshot:   w.GetSnapshot(),
			Operations: w.GetOperations(),
			Version:    w.GetVersion(),
			Presence:   w.GetPresence(),
			Speaks:     w.GetSpeaks(),
		}, nil
	case *collabpb.ServerMessage_Operations:
		return kindOperation, opsMsg{Operations: body.Operations.GetOperations()}, nil
	case *collabpb.ServerMessage_Presence:
		return kindPresence, presenceMsg{Update: body.Presence.GetUpdate()}, nil
	default:
		return 0, nil, ErrProtocol
	}
}

func (c *grpcConn) Close() error { return c.stream.CloseSend() }

// GRPCServer presents a Server as the generated gRPC service.
//
// It exists because the Server itself no longer does. The session logic speaks
// the wire format in wire.go — four small types, hand-written — and that is
// what let it stop depending on the generated protobuf code. The reason is a
// measurement: compiling the server for the browser with protobuf attached
// takes the WebAssembly binding from 5.3 MB to 19.3, because gRPC and protobuf
// come with it. A browser holding a document for a colleague on another
// continent cannot pay that, and it is the same reason wire.go exists at all.
//
// So gRPC is a binding rather than a foundation: this type converts, and the
// document logic never sees a protobuf message.
type GRPCServer struct {
	collabpb.UnimplementedCollabServer
	inner *Server
}

// GRPC presents a Server over gRPC. Register the result with
// [collabpb.RegisterCollabServer] on any grpc.Server.
func GRPCService(s *Server) *GRPCServer { return &GRPCServer{inner: s} }

// Session is the service method: one bidirectional stream, one participant, one
// document.
func (g *GRPCServer) Session(stream collabpb.Collab_SessionServer) error {
	return asStatus(g.inner.session(&grpcCarrier{stream: stream}))
}

// asStatus says in gRPC's vocabulary what the session said in its own.
//
// The session does not know what gRPC is, which is the point: naming an error
// with grpc/status pulls in the protobuf runtime, and protobuf registers its
// descriptors in init, so a linker cannot drop it. That cost three and a half
// megabytes on the WebAssembly build — for the privilege of naming an error.
//
// A refusal keeps whatever Authorize returned, so a caller that answered with a
// status error gets the code it chose. That was documented before this and is
// documented still; it is recovered by unwrapping rather than by the session
// carrying a status it has no use for.
func asStatus(err error) error {
	var se *sessionError
	if !errors.As(err, &se) {
		return err
	}
	if se.kind == errRefused {
		if s, ok := status.FromError(se.cause); ok && se.cause != nil {
			return s.Err()
		}
		return status.Error(codes.PermissionDenied, se.msg)
	}
	code := codes.Internal
	switch se.kind {
	case errInvalid:
		code = codes.InvalidArgument
	case errExhausted:
		code = codes.ResourceExhausted
	case errAborted:
		code = codes.Aborted
	}
	return status.Error(code, se.msg)
}

// A grpcCarrier presents a gRPC stream to the session logic, converting between
// the generated messages and the wire types on the way through.
type grpcCarrier struct {
	stream collabpb.Collab_SessionServer
}

func (c *grpcCarrier) Context() context.Context { return c.stream.Context() }

func (c *grpcCarrier) Recv() (byte, any, error) {
	in, err := c.stream.Recv()
	if err != nil {
		return 0, nil, err
	}
	switch body := in.GetBody().(type) {
	case *collabpb.ClientMessage_Join:
		j := body.Join
		return kindJoin, joinMsg{
			Document: j.GetDocument(), Site: j.GetSite(),
			Have: j.GetHave(), Speaks: j.GetSpeaks(),
		}, nil
	case *collabpb.ClientMessage_Operations:
		return kindOperation, opsMsg{Operations: body.Operations.GetOperations()}, nil
	case *collabpb.ClientMessage_Presence:
		return kindPresence, presenceMsg{Update: body.Presence.GetUpdate()}, nil
	case *collabpb.ClientMessage_Acknowledge:
		return kindAcknowledge, ackMsg{Version: body.Acknowledge.GetVersion(), Clocks: body.Acknowledge.GetClocks()}, nil
	default:
		// A message the generated code allows and this does not understand,
		// which the session logic refuses as "not a join" or "not operations".
		return 0, nil, nil
	}
}

func (c *grpcCarrier) Send(kind byte, msg any) error {
	switch kind {
	case kindWelcome:
		w := msg.(welcomeMsg)
		return c.stream.Send(&collabpb.ServerMessage{
			Body: &collabpb.ServerMessage_Welcome{Welcome: &collabpb.Welcome{
				Snapshot:   w.Snapshot,
				Operations: w.Operations,
				Version:    w.Version,
				Presence:   w.Presence,
				Speaks:     w.Speaks,
			}},
		})
	case kindOperation:
		return c.stream.Send(&collabpb.ServerMessage{
			Body: &collabpb.ServerMessage_Operations{
				Operations: &collabpb.Operations{Operations: msg.(opsMsg).Operations},
			},
		})
	case kindPresence:
		return c.stream.Send(&collabpb.ServerMessage{
			Body: &collabpb.ServerMessage_Presence{
				Presence: &collabpb.Presence{Update: msg.(presenceMsg).Update},
			},
		})
	default:
		return ErrProtocol
	}
}
