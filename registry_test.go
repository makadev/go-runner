package runner

import (
	"context"
	"testing"
)

func TestRegistry_RegisterAndHandle(t *testing.T) {
	reg := NewRegistry()
	called := false
	reg.Register("demo", func(ctx context.Context, j Job) error {
		called = true
		if j.Type != "demo" {
			t.Fatalf("unexpected job type: %q", j.Type)
		}
		return nil
	})

	if err := reg.Handle(context.Background(), Job{Type: "demo"}); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if !called {
		t.Fatalf("handler not called")
	}
}

func TestRegistry_UnknownType(t *testing.T) {
	reg := NewRegistry()
	if err := reg.Handle(context.Background(), Job{Type: "missing"}); err == nil {
		t.Fatalf("expected error for unknown type")
	}
}

func TestRegistry_RegisterPanics(t *testing.T) {
	cases := []struct {
		name string
		typ  string
		h    Handler
		dup  bool
	}{
		{name: "empty type", typ: "", h: func(context.Context, Job) error { return nil }},
		{name: "nil handler", typ: "x", h: nil},
		{name: "duplicate", typ: "dup", h: func(context.Context, Job) error { return nil }, dup: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Fatalf("expected panic")
				}
			}()
			reg := NewRegistry()
			if tc.dup {
				reg.Register(tc.typ, func(context.Context, Job) error { return nil })
			}
			reg.Register(tc.typ, tc.h)
		})
	}
}
