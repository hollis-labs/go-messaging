package mailbox

import (
	"context"
	"testing"
)

func TestCallerIdentity_IsZero(t *testing.T) {
	tests := []struct {
		name string
		id   CallerIdentity
		want bool
	}{
		{"both empty", CallerIdentity{}, true},
		{"session only", CallerIdentity{SessionID: "s"}, false},
		{"agent only", CallerIdentity{AgentID: "a"}, false},
		{"both set", CallerIdentity{SessionID: "s", AgentID: "a"}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.id.IsZero(); got != tc.want {
				t.Fatalf("IsZero=%v, want %v", got, tc.want)
			}
		})
	}
}

func TestWithCaller_RoundTrip(t *testing.T) {
	ctx := context.Background()
	id := CallerIdentity{SessionID: "sess-1", AgentID: "file-backend"}

	ctx = WithCaller(ctx, id)
	got, ok := CallerFromCtx(ctx)
	if !ok {
		t.Fatal("CallerFromCtx missed round-trip identity")
	}
	if got != id {
		t.Fatalf("got %+v, want %+v", got, id)
	}
}

func TestWithCaller_ZeroIsNoOp(t *testing.T) {
	ctx := context.Background()
	ctx = WithCaller(ctx, CallerIdentity{})
	if _, ok := CallerFromCtx(ctx); ok {
		t.Fatal("zero CallerIdentity must NOT be stamped on ctx")
	}
}

// TestCallerIdentity_IsComplete pins down the all-or-nothing predicate
// used by WithCaller: both fields must be populated for the identity to
// be considered trustworthy enough to stamp on ctx.
func TestCallerIdentity_IsComplete(t *testing.T) {
	tests := []struct {
		name string
		id   CallerIdentity
		want bool
	}{
		{"both empty", CallerIdentity{}, false},
		{"session only", CallerIdentity{SessionID: "s"}, false},
		{"agent only", CallerIdentity{AgentID: "a"}, false},
		{"both set", CallerIdentity{SessionID: "s", AgentID: "a"}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.id.IsComplete(); got != tc.want {
				t.Fatalf("IsComplete=%v, want %v", got, tc.want)
			}
		})
	}
}

// TestWithCaller_PartialIsNoOp guards the all-or-nothing contract: a
// CallerIdentity with only one of (SessionID, AgentID) populated must
// NOT be stamped on ctx. Stamping a partial identity would flip
// service-layer authz checks from fall-open to enforce (or trigger 400s
// in resolveCaller) without an actual authenticated caller behind it —
// see WithCaller doc comment.
func TestWithCaller_PartialIsNoOp(t *testing.T) {
	tests := []struct {
		name string
		id   CallerIdentity
	}{
		{"session only", CallerIdentity{SessionID: "s"}},
		{"agent only", CallerIdentity{AgentID: "a"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := WithCaller(context.Background(), tc.id)
			if _, ok := CallerFromCtx(ctx); ok {
				t.Fatalf("partial CallerIdentity %+v must NOT be stamped on ctx", tc.id)
			}
		})
	}
}

func TestCallerFromCtx_NilCtx(t *testing.T) {
	//nolint:staticcheck // Explicitly verifies the documented defensive nil case.
	if _, ok := CallerFromCtx(nil); ok {
		t.Fatal("nil ctx must report caller-absent")
	}
}

func TestCallerFromCtx_AbsentReturnsFalse(t *testing.T) {
	ctx := context.Background()
	if _, ok := CallerFromCtx(ctx); ok {
		t.Fatal("bare ctx must report caller-absent")
	}
}

func TestCallerFromCtx_OverridesParent(t *testing.T) {
	ctx := WithCaller(context.Background(), CallerIdentity{SessionID: "outer", AgentID: "a"})
	ctx = WithCaller(ctx, CallerIdentity{SessionID: "inner", AgentID: "b"})

	got, ok := CallerFromCtx(ctx)
	if !ok {
		t.Fatal("expected override identity")
	}
	if got.SessionID != "inner" || got.AgentID != "b" {
		t.Fatalf("expected inner override, got %+v", got)
	}
}
