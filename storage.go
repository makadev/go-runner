package runner

import (
	"context"
	"encoding/json"
	"time"
)

// Storage is the minimum contract required by Queue and Worker.
//
// Contract highlights:
// - ClaimJob must be atomic and return ErrNotClaimed when the job is not claimed.
// - ClaimJob must allow reclaiming stuck running jobs older than reclaimRunningBefore.
// - MarkRetry and MarkFailed must increment Attempts (attempts accounting lives in storage).
// - CreateJob must enforce idempotency key uniqueness when provided.
type Storage interface {
	CreateJob(ctx context.Context, typ string, payload json.RawMessage, opt EnqueueOptions) (Job, error)

	ListRunnableIDs(ctx context.Context, limit int64) ([]int64, error)
	ClaimJob(ctx context.Context, id int64, lockedBy string, reclaimRunningBefore time.Time) (Job, error)

	MarkSucceeded(ctx context.Context, id int64) error
	MarkRetry(ctx context.Context, id int64, lastError string, runAt time.Time) error
	MarkFailed(ctx context.Context, id int64, lastError string) error
}
