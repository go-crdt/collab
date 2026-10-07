//go:build !js

package collab

import (
	"strings"
	"testing"
	"time"
)

// A retry policy that cannot be honoured is refused before anything waits.
//
// Both conditions in [RetryPolicy.checked] were held only through
// [JoinWithRetry], which refuses when checked does -- and deleting either one
// did not make the suite fail. It made the suite HANG: with the policy
// accepted, the caller went on to retry, and the first wait was the hour
// somebody had typed into the wrong field. Three hundred seconds without an
// answer, in a suite that takes sixty-five.
//
// A hang is the worst way for a suite to notice something. It arrives as a
// timeout with no line naming the cause, it costs a CI runner's whole budget,
// and on a loaded machine it is indistinguishable from slowness. So the
// conditions are asserted here, directly, where nothing sleeps.
func TestARetryPolicyThatCannotBeHonouredIsRefused(t *testing.T) {
	for _, tt := range []struct {
		name   string
		policy RetryPolicy
		says   string
	}{
		{"a negative wait", RetryPolicy{Wait: -time.Second}, "negative"},
		{"a negative ceiling", RetryPolicy{Ceiling: -time.Second}, "negative"},
		{"both negative", RetryPolicy{Wait: -time.Second, Ceiling: -time.Minute}, "negative"},
		{"a wait past the ceiling", RetryPolicy{Wait: time.Hour, Ceiling: time.Second}, "capped"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := tt.policy.checked()
			if err == nil {
				t.Fatal("accepted: the caller goes on to wait out whichever field was mistyped")
			}
			if !strings.Contains(err.Error(), tt.says) {
				t.Errorf("the refusal does not say what was wrong (%q is not in %q)", tt.says, err)
			}
		})
	}

	// The controls. A policy that says nothing is filled in rather than
	// refused, and an ordinary one is left alone -- without these, a version
	// that refused everything would pass the cases above.
	filled, err := RetryPolicy{}.checked()
	if err != nil {
		t.Fatalf("an empty policy was refused: %v", err)
	}
	if filled.Wait != DefaultRetryWait || filled.Ceiling != DefaultRetryCeiling {
		t.Fatalf("an empty policy filled in as wait %v ceiling %v, want the defaults %v and %v",
			filled.Wait, filled.Ceiling, DefaultRetryWait, DefaultRetryCeiling)
	}
	ordinary := RetryPolicy{Wait: time.Second, Ceiling: time.Minute}
	if got, err := ordinary.checked(); err != nil || got.Wait != ordinary.Wait || got.Ceiling != ordinary.Ceiling {
		t.Fatalf("an ordinary policy came back as %+v, %v", got, err)
	}
}
