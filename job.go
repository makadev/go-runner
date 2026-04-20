package runner

import (
	"encoding/json"
	"errors"
	"time"
)

type Status string

const (
	StatusPending   Status = "pending"
	StatusRunning   Status = "running"
	StatusSucceeded Status = "succeeded"
	StatusFailed    Status = "failed"
)

const (
	// DefaultMaxAttempts is used when EnqueueOptions.MaxAttempts is <= 0.
	// This avoids accidentally creating jobs that can never run (MaxAttempts=0).
	DefaultMaxAttempts int64 = 8
)

var (
	// ErrNotClaimed indicates the job could not be claimed, typically because another
	// worker claimed it first or the job is not in a claimable state.
	ErrNotClaimed = errors.New("runner: not claimed")
)

type Job struct {
	ID     int64
	Type   string
	Status Status

	Payload        json.RawMessage
	IdempotencyKey *string

	Attempts    int64
	MaxAttempts int64
	LastError   string

	// RunAt is optional; when set, the job should not be processed before that time.
	RunAt *time.Time

	LockedAt *time.Time
	LockedBy string

	CreatedAt time.Time
	UpdatedAt time.Time

	FinishedAt *time.Time
}

type EnqueueOptions struct {
	RunAt          *time.Time
	MaxAttempts    int64
	IdempotencyKey *string
}

