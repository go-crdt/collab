//go:build !js

package collab

import (
	"errors"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// The gRPC binding says in gRPC's vocabulary what the session said in its own.
//
// The session does not know what gRPC is — naming an error with grpc/status
// carries the protobuf runtime, and protobuf registers its descriptors in init,
// so a linker cannot drop it. That was three and a half megabytes on the
// WebAssembly build. The translation therefore happens here, and here is where
// it has to be checked: a kind that fell through to Internal by accident would
// tell a participant nothing about what it did wrong.
func TestTheGRPCBindingNamesEveryRefusal(t *testing.T) {
	for _, tt := range []struct {
		kind errKind
		want codes.Code
	}{
		{errInvalid, codes.InvalidArgument},
		{errExhausted, codes.ResourceExhausted},
		{errAborted, codes.Aborted},
		{errInternal, codes.Internal},
		{errRefused, codes.PermissionDenied},
	} {
		err := asStatus(&sessionError{kind: tt.kind, msg: "because"})
		if got := status.Code(err); got != tt.want {
			t.Errorf("kind %d became %v, want %v", tt.kind, got, tt.want)
		}
		if s, _ := status.FromError(err); s.Message() != "because" {
			t.Errorf("kind %d lost its reason: %q", tt.kind, s.Message())
		}
	}

	// An error that is not the session's passes through untouched, which is
	// what a context cancellation and a transport failure both are.
	plain := errors.New("something else")
	if got := asStatus(plain); got != plain {
		t.Fatalf("a plain error became %v", got)
	}
	if asStatus(nil) != nil {
		t.Fatal("no error became an error")
	}
}

// Authorize may answer with a gRPC status, and that status reaches the
// participant with the code its author chose. It was documented before the
// session stopped speaking gRPC and it is documented still — so it is recovered
// by unwrapping rather than by the session carrying a status it has no use for.
func TestAuthorizeKeepsTheCodeItChose(t *testing.T) {
	chosen := status.Error(codes.Unauthenticated, "no token")
	got := asStatus(refusal(chosen))
	if code := status.Code(got); code != codes.Unauthenticated {
		t.Fatalf("the code Authorize chose became %v", code)
	}
	if s, _ := status.FromError(got); s.Message() != "no token" {
		t.Fatalf("the reason Authorize gave became %q", s.Message())
	}

	// And an ordinary error is a refusal and nothing more.
	got = asStatus(refusal(errors.New("not for you")))
	if code := status.Code(got); code != codes.PermissionDenied {
		t.Fatalf("an ordinary refusal became %v", code)
	}
	if s, _ := status.FromError(got); s.Message() != "not for you" {
		t.Fatalf("the reason became %q", s.Message())
	}
}
