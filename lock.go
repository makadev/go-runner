package runner

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"time"
)

// DefaultFinalizeTimeout bounds the storage write that records a job's outcome
// (MarkSucceeded, MarkRetry, MarkFailed) when no explicit timeout is configured.
const DefaultFinalizeTimeout = 10 * time.Second

// newLockOwner returns a lock owner string that is unique per claim.
// It keeps the worker ID as a readable prefix, e.g. "worker-1/3f9a1c0b7e2d4a65".
// Using a fresh value per claim ensures a worker whose lock expired cannot finalize
// a job that has been reclaimed in the meantime, even when several workers share
// the same WorkerID.
func newLockOwner(workerID string) string {
	var b [8]byte
	_, _ = rand.Read(b[:]) // crypto/rand.Read never returns an error since Go 1.24
	return workerID + "/" + hex.EncodeToString(b[:])
}

// finalizeContext returns a context for recording a job's outcome. It is detached
// from parent's cancellation so the outcome is still written during shutdown, but
// it keeps parent's values and is bounded by timeout.
func finalizeContext(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		timeout = DefaultFinalizeTimeout
	}
	return context.WithTimeout(context.WithoutCancel(parent), timeout)
}
