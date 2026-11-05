package runner

import (
	"context"
	"encoding/json"
	"time"
)

// Storage is the minimum contract required by Queue and Worker.
//
// Contract highlights:
//   - ClaimJob must be atomic and return ErrNotClaimed when the job is not claimed.
//   - ClaimJob must allow reclaiming stuck running jobs whose lock is older than
//     reclaimRunningBefore. When it reclaims such a job it must increment Attempts:
//     the previous attempt never finished (crash, OOM, kill) and counts as failed.
//     Without this, a job that reliably kills its worker would be retried forever.
//   - ClaimJob must store lockedBy as the lock owner. lockedBy is unique per claim.
//   - MarkSucceeded, MarkRetry and MarkFailed must only update the job when it is
//     still running and locked by lockedBy. Otherwise they must not change the job
//     and must return ErrLockLost (another claim took over in the meantime).
//   - MarkRetry and MarkFailed must increment Attempts (attempts accounting lives in storage).
//   - CreateJob must enforce idempotency key uniqueness when provided.
//
// Errors may be wrapped; runner compares them with errors.Is.
type Storage interface {
	CreateJob(ctx context.Context, typ string, payload json.RawMessage, opt EnqueueOptions) (Job, error)

	ListRunnableIDs(ctx context.Context, limit int64) ([]int64, error)
	ClaimJob(ctx context.Context, id int64, lockedBy string, reclaimRunningBefore time.Time) (Job, error)

	MarkSucceeded(ctx context.Context, id int64, lockedBy string) error
	MarkRetry(ctx context.Context, id int64, lockedBy string, lastError string, runAt time.Time) error
	MarkFailed(ctx context.Context, id int64, lockedBy string, lastError string) error
}
