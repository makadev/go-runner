package runner

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"
)

type Queue struct {
	Store    Storage
	Registry *Registry
	Log      *slog.Logger

	// Inline, when true, claims and executes jobs immediately after enqueue.
	Inline bool

	// WorkerID is used when claiming jobs in Inline mode.
	WorkerID string

	// LockTTL is used to reclaim stuck running jobs in Inline mode.
	LockTTL time.Duration
}

func (q *Queue) Enqueue(ctx context.Context, typ string, payload any, opt EnqueueOptions) (Job, error) {
	if q == nil || q.Store == nil {
		return Job{}, fmt.Errorf("runner: queue not initialized")
	}
	typ = strings.TrimSpace(typ)
	if typ == "" {
		return Job{}, fmt.Errorf("runner: type is required")
	}
	if opt.MaxAttempts <= 0 {
		opt.MaxAttempts = DefaultMaxAttempts
	}

	var b []byte
	switch v := payload.(type) {
	case json.RawMessage:
		b = v
	case []byte:
		b = v
	default:
		enc, err := json.Marshal(payload)
		if err != nil {
			return Job{}, err
		}
		b = enc
	}
	if len(b) == 0 {
		b = []byte("{}")
	}

	created, err := q.Store.CreateJob(ctx, typ, json.RawMessage(b), opt)
	if err != nil {
		return Job{}, err
	}

	if q.Inline {
		if err := q.runInline(ctx, created.ID); err != nil {
			return created, err
		}
	}
	return created, nil
}

func (q *Queue) runInline(ctx context.Context, id int64) error {
	if q.Registry == nil {
		return fmt.Errorf("runner: inline mode requires registry")
	}
	log := q.Log
	if log == nil {
		log = slog.Default()
	}

	workerID := strings.TrimSpace(q.WorkerID)
	if workerID == "" {
		workerID = "inline"
	}
	lockTTL := q.LockTTL
	if lockTTL <= 0 {
		lockTTL = 15 * time.Minute
	}

	reclaimBefore := time.Now().UTC().Add(-lockTTL)
	job, err := q.Store.ClaimJob(ctx, id, workerID, reclaimBefore)
	if err != nil {
		if err == ErrNotClaimed {
			return nil
		}
		return err
	}

	if err := q.Registry.Handle(ctx, job); err != nil {
		log.Warn("job failed (inline)", "id", job.ID, "type", job.Type, "attempts", job.Attempts, "err", err)
		if err2 := q.Store.MarkFailed(ctx, job.ID, err.Error()); err2 != nil {
			return err2
		}
		return err
	}

	if err := q.Store.MarkSucceeded(ctx, job.ID); err != nil {
		return err
	}
	log.Info("job succeeded (inline)", "id", job.ID, "type", job.Type)
	return nil
}
