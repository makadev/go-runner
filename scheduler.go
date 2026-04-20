package runner

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

type ScheduledTask func(ctx context.Context) error

type Schedule struct {
	Name      string
	Every     time.Duration
	Task      ScheduledTask
	RunOnBoot bool
}

// Scheduler runs in-process periodic tasks. Tasks may enqueue durable jobs via Queue,
// but schedules themselves are intentionally not persisted.
type Scheduler struct {
	Log *slog.Logger

	mu        sync.Mutex
	schedules []Schedule
}

func (s *Scheduler) Register(sc Schedule) {
	if sc.Name == "" {
		panic("runner: schedule name required")
	}
	if sc.Every <= 0 {
		panic("runner: schedule interval must be > 0")
	}
	if sc.Task == nil {
		panic("runner: schedule task is nil")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, existing := range s.schedules {
		if existing.Name == sc.Name {
			panic(fmt.Sprintf("runner: duplicate schedule %q", sc.Name))
		}
	}
	s.schedules = append(s.schedules, sc)
}

func (s *Scheduler) Run(ctx context.Context) {
	if s == nil {
		return
	}
	log := s.Log
	if log == nil {
		log = slog.Default()
	}

	s.mu.Lock()
	schedules := append([]Schedule(nil), s.schedules...)
	s.mu.Unlock()

	var wg sync.WaitGroup
	for _, sc := range schedules {
		sc := sc
		wg.Add(1)
		go func() {
			defer wg.Done()

			run := func() {
				if err := sc.Task(ctx); err != nil && !errors.Is(err, context.Canceled) {
					log.Error("scheduled task failed", "name", sc.Name, "err", err)
				}
			}

			if sc.RunOnBoot {
				run()
			}

			tick := time.NewTicker(sc.Every)
			defer tick.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-tick.C:
					run()
				}
			}
		}()
	}

	<-ctx.Done()
	wg.Wait()
}

