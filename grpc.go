//go:build !js

package collab

import (
	"context"

	"github.com/go-crdt/collab/collabpb"
	"google.golang.org/grpc"
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
			Document: m.Document, Site: m.Site, Have: m.Have,
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
