package runner

import (
	"context"
	"encoding/json"
	"errors"
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

	// FinalizeTimeout bounds the storage write that records an inline job's outcome.
	// That write is not cancelled by ctx. Defaults to DefaultFinalizeTimeout.
	FinalizeTimeout time.Duration
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

	owner := newLockOwner(workerID)
	reclaimBefore := time.Now().UTC().Add(-lockTTL)
	job, err := q.Store.ClaimJob(ctx, id, owner, reclaimBefore)
	if err != nil {
		if errors.Is(err, ErrNotClaimed) {
			return nil
		}
		return err
	}

	runErr := q.Registry.Handle(ctx, job)

	fctx, cancel := finalizeContext(ctx, q.FinalizeTimeout)
	defer cancel()

	if runErr != nil {
		logHandlerError(log, "job failed (inline)", job, runErr)
		if err := q.Store.MarkFailed(fctx, job.ID, owner, runErr.Error()); err != nil && !errors.Is(err, ErrLockLost) {
			return err
		}
		return runErr
	}

	if err := q.Store.MarkSucceeded(fctx, job.ID, owner); err != nil {
		if errors.Is(err, ErrLockLost) {
			log.Warn("job lock lost, outcome discarded (inline)", "id", job.ID, "type", job.Type)
			return nil
		}
		return err
	}
	log.Info("job succeeded (inline)", "id", job.ID, "type", job.Type)
	return nil
}
