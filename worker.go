package runner

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"time"
)

type Worker struct {
	Store    Storage
	Log      *slog.Logger
	Registry *Registry

	// ProcessInterval defaults to 10s.
	ProcessInterval time.Duration
	// BatchSize defaults to 10.
	BatchSize int
	// LockTTL defaults to 15m; running jobs older than this may be reclaimed.
	LockTTL time.Duration

	// WorkerID identifies this worker in DB locks (optional but recommended).
	WorkerID string

	// Backoff controls retry scheduling; defaults to DefaultBackoff when nil.
	Backoff BackoffFunc
}

// RunCoordinated runs the worker with separate soft/hard shutdown signals:
// - softCtx: stop claiming/starting new jobs (finish at most the in-flight job)
// - hardCtx: cancel in-flight job work if a grace deadline expires
func (w *Worker) RunCoordinated(softCtx, hardCtx context.Context) {
	if w == nil || w.Store == nil || w.Registry == nil {
		return
	}
	log := w.Log
	if log == nil {
		log = slog.Default()
	}
	interval := w.ProcessInterval
	if interval <= 0 {
		interval = 10 * time.Second
	}
	batch := w.BatchSize
	if batch <= 0 {
		batch = 10
	}
	lockTTL := w.LockTTL
	if lockTTL <= 0 {
		lockTTL = 15 * time.Minute
	}
	workerID := strings.TrimSpace(w.WorkerID)
	if workerID == "" {
		workerID = "worker"
	}
	backoff := w.Backoff
	if backoff == nil {
		backoff = DefaultBackoff
	}

	tick := time.NewTicker(interval)
	defer tick.Stop()

	runOnce := func() {
		select {
		case <-softCtx.Done():
			return
		default:
		}
		if err := w.runBatchCoordinated(softCtx, hardCtx, workerID, int64(batch), lockTTL, backoff); err != nil && !errors.Is(err, context.Canceled) {
			log.Error("jobs batch", "err", err)
		}
	}

	runOnce()
	for {
		select {
		case <-softCtx.Done():
			return
		case <-tick.C:
			runOnce()
		}
	}
}

func (w *Worker) runBatchCoordinated(softCtx, hardCtx context.Context, workerID string, limit int64, lockTTL time.Duration, backoff BackoffFunc) error {
	select {
	case <-softCtx.Done():
		return softCtx.Err()
	default:
	}

	ids, err := w.Store.ListRunnableIDs(softCtx, limit)
	if err != nil {
		return err
	}
	for _, id := range ids {
		select {
		case <-softCtx.Done():
			return nil
		default:
		}
		if err := w.processJobIDCoordinated(softCtx, hardCtx, workerID, id, lockTTL, backoff); err != nil && !errors.Is(err, context.Canceled) {
			return err
		}
	}
	return nil
}

func (w *Worker) processJobIDCoordinated(softCtx, hardCtx context.Context, workerID string, id int64, lockTTL time.Duration, backoff BackoffFunc) error {
	log := w.Log
	if log == nil {
		log = slog.Default()
	}

	select {
	case <-softCtx.Done():
		return nil
	default:
	}

	reclaimBefore := time.Now().UTC().Add(-lockTTL)
	job, err := w.Store.ClaimJob(softCtx, id, workerID, reclaimBefore)
	if err != nil {
		if err == ErrNotClaimed {
			return nil
		}
		return err
	}

	if job.Attempts >= job.MaxAttempts {
		_ = w.Store.MarkFailed(hardCtx, job.ID, "max attempts exceeded")
		return nil
	}

	if err := w.Registry.Handle(hardCtx, job); err != nil {
		log.Warn("job failed", "id", job.ID, "type", job.Type, "attempts", job.Attempts, "err", err)
		return w.failOrRetry(hardCtx, job, err, backoff)
	}

	if err := w.Store.MarkSucceeded(hardCtx, job.ID); err != nil {
		return err
	}
	log.Info("job succeeded", "id", job.ID, "type", job.Type)
	return nil
}

func (w *Worker) failOrRetry(ctx context.Context, job Job, runErr error, backoff BackoffFunc) error {
	nextAttempts := job.Attempts + 1
	if nextAttempts >= job.MaxAttempts {
		return w.Store.MarkFailed(ctx, job.ID, runErr.Error())
	}
	next := time.Now().UTC().Add(backoff(job.Attempts))
	return w.Store.MarkRetry(ctx, job.ID, runErr.Error(), next)
}
