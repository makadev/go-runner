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

	// FinalizeTimeout bounds the storage write that records a job's outcome.
	// That write is not cancelled by the hard shutdown context. Defaults to DefaultFinalizeTimeout.
	FinalizeTimeout time.Duration
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

	owner := newLockOwner(workerID)
	reclaimBefore := time.Now().UTC().Add(-lockTTL)
	job, err := w.Store.ClaimJob(softCtx, id, owner, reclaimBefore)
	if err != nil {
		if errors.Is(err, ErrNotClaimed) {
			return nil
		}
		return err
	}

	if job.Attempts >= job.MaxAttempts {
		log.Warn("job failed: max attempts exceeded", "id", job.ID, "type", job.Type, "attempts", job.Attempts, "last_error", job.LastError)
		return w.finalize(hardCtx, log, job, func(ctx context.Context) error {
			return w.Store.MarkFailed(ctx, job.ID, owner, "max attempts exceeded")
		})
	}

	if runErr := w.Registry.Handle(hardCtx, job); runErr != nil {
		logHandlerError(log, "job failed", job, runErr)
		return w.finalize(hardCtx, log, job, func(ctx context.Context) error {
			return w.failOrRetry(ctx, job, owner, runErr, backoff, hardCtx.Err() != nil)
		})
	}

	if err := w.finalize(hardCtx, log, job, func(ctx context.Context) error {
		return w.Store.MarkSucceeded(ctx, job.ID, owner)
	}); err != nil {
		return err
	}
	log.Info("job succeeded", "id", job.ID, "type", job.Type)
	return nil
}

// finalize records a job's outcome. The write uses a context detached from hardCtx,
// so the outcome is still persisted during a hard shutdown instead of leaving the job
// in "running" until its lock expires. ErrLockLost is logged and not treated as an error.
func (w *Worker) finalize(hardCtx context.Context, log *slog.Logger, job Job, write func(ctx context.Context) error) error {
	ctx, cancel := finalizeContext(hardCtx, w.FinalizeTimeout)
	defer cancel()
	err := write(ctx)
	if errors.Is(err, ErrLockLost) {
		log.Warn("job lock lost, outcome discarded", "id", job.ID, "type", job.Type)
		return nil
	}
	return err
}

// failOrRetry marks the job failed when its attempts are used up and schedules a retry otherwise.
// When shuttingDown is set the handler was interrupted by the hard shutdown, so the retry is
// scheduled immediately instead of after the backoff delay.
func (w *Worker) failOrRetry(ctx context.Context, job Job, owner string, runErr error, backoff BackoffFunc, shuttingDown bool) error {
	nextAttempts := job.Attempts + 1
	if nextAttempts >= job.MaxAttempts {
		return w.Store.MarkFailed(ctx, job.ID, owner, runErr.Error())
	}
	next := time.Now().UTC()
	if !shuttingDown {
		next = next.Add(backoff(job.Attempts))
	}
	return w.Store.MarkRetry(ctx, job.ID, owner, runErr.Error(), next)
}

func logHandlerError(log *slog.Logger, msg string, job Job, err error) {
	var pe *PanicError
	if errors.As(err, &pe) {
		log.Error(msg+" (panic)", "id", job.ID, "type", job.Type, "attempts", job.Attempts, "panic", pe.Value, "stack", string(pe.Stack))
		return
	}
	log.Warn(msg, "id", job.ID, "type", job.Type, "attempts", job.Attempts, "err", err)
}
