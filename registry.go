package runner

import (
	"context"
	"fmt"
	"sync"
)

type Handler func(ctx context.Context, job Job) error

// Registry dispatches DB-backed jobs by type.
type Registry struct {
	mu       sync.RWMutex
	handlers map[string]Handler
}

func NewRegistry() *Registry {
	return &Registry{handlers: map[string]Handler{}}
}

func (r *Registry) Register(typ string, h Handler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if typ == "" {
		panic("runner: empty type")
	}
	if h == nil {
		panic("runner: nil handler")
	}
	if _, exists := r.handlers[typ]; exists {
		panic(fmt.Sprintf("runner: duplicate handler for %q", typ))
	}
	r.handlers[typ] = h
}

func (r *Registry) Handle(ctx context.Context, job Job) error {
	r.mu.RLock()
	h := r.handlers[job.Type]
	r.mu.RUnlock()
	if h == nil {
		return fmt.Errorf("runner: no handler registered for type %q", job.Type)
	}
	return h(ctx, job)
}
