package runner

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestScheduler_RunOnBootAndTicks(t *testing.T) {
	var calls atomic.Int64
	calledCh := make(chan struct{}, 10)

	s := &Scheduler{}
	s.Register(Schedule{
		Name:      "demo",
		Every:     20 * time.Millisecond,
		RunOnBoot: true,
		Task: func(ctx context.Context) error {
			calls.Add(1)
			select {
			case calledCh <- struct{}{}:
			default:
			}
			return nil
		},
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		s.Run(ctx)
		close(done)
	}()

	// RunOnBoot should happen quickly.
	select {
	case <-calledCh:
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("expected RunOnBoot call")
	}

	// And at least one tick shortly after.
	select {
	case <-calledCh:
	case <-time.After(800 * time.Millisecond):
		t.Fatalf("expected tick call")
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("scheduler did not stop after cancel")
	}

	if calls.Load() < 2 {
		t.Fatalf("expected at least 2 calls, got %d", calls.Load())
	}
}

func TestScheduler_DuplicateNamePanics(t *testing.T) {
	s := &Scheduler{}
	s.Register(Schedule{Name: "x", Every: time.Second, Task: func(context.Context) error { return nil }})
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic")
		}
	}()
	s.Register(Schedule{Name: "x", Every: time.Second, Task: func(context.Context) error { return nil }})
}

func TestScheduler_RunNilNoop(t *testing.T) {
	var s *Scheduler
	s.Run(context.Background())
}

func TestScheduler_RegisterPanics(t *testing.T) {
	cases := []struct {
		name string
		sc   Schedule
	}{
		{name: "zero interval", sc: Schedule{Name: "x", Every: 0, Task: func(context.Context) error { return nil }}},
		{name: "negative interval", sc: Schedule{Name: "x", Every: -time.Second, Task: func(context.Context) error { return nil }}},
		{name: "nil task", sc: Schedule{Name: "x", Every: time.Second, Task: nil}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Fatalf("expected panic")
				}
			}()
			s := &Scheduler{}
			s.Register(tc.sc)
		})
	}
}
