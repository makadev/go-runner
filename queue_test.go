package runner

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"
)

type recordingStore struct {
	mu sync.Mutex

	createTyp     string
	createPayload json.RawMessage
	createOpt     EnqueueOptions
	createJob     Job
	createErr     error

	claimJob Job
	claimErr error

	markSucceededIDs []int64
	markRetryCalls   []struct {
		id    int64
		err   string
		runAt time.Time
	}
	markFailedCalls []struct {
		id  int64
		err string
	}
}

func (s *recordingStore) CreateJob(ctx context.Context, typ string, payload json.RawMessage, opt EnqueueOptions) (Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.createTyp = typ
	s.createPayload = append(json.RawMessage(nil), payload...)
	s.createOpt = opt
	if s.createErr != nil {
		return Job{}, s.createErr
	}
	return s.createJob, nil
}

func (s *recordingStore) ListRunnableIDs(ctx context.Context, limit int64) ([]int64, error) {
	return nil, nil
}

func (s *recordingStore) ClaimJob(ctx context.Context, id int64, lockedBy string, reclaimRunningBefore time.Time) (Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.claimErr != nil {
		return Job{}, s.claimErr
	}
	return s.claimJob, nil
}

func (s *recordingStore) MarkSucceeded(ctx context.Context, id int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.markSucceededIDs = append(s.markSucceededIDs, id)
	return nil
}

func (s *recordingStore) MarkRetry(ctx context.Context, id int64, lastError string, runAt time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.markRetryCalls = append(s.markRetryCalls, struct {
		id    int64
		err   string
		runAt time.Time
	}{id: id, err: lastError, runAt: runAt})
	return nil
}

func (s *recordingStore) MarkFailed(ctx context.Context, id int64, lastError string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.markFailedCalls = append(s.markFailedCalls, struct {
		id  int64
		err string
	}{id: id, err: lastError})
	return nil
}

func TestQueue_EnqueueValidation(t *testing.T) {
	q := &Queue{}
	if _, err := q.Enqueue(context.Background(), "x", map[string]any{}, EnqueueOptions{}); err == nil {
		t.Fatalf("expected error for nil store")
	}

	q.Store = &recordingStore{createJob: Job{ID: 1, Type: "x"}}
	if _, err := q.Enqueue(context.Background(), "   ", map[string]any{}, EnqueueOptions{}); err == nil {
		t.Fatalf("expected error for empty type")
	}
}

func TestQueue_EnqueuePayloadDefaultEmptyObject(t *testing.T) {
	st := &recordingStore{createJob: Job{ID: 1, Type: "t"}}
	q := &Queue{Store: st}

	if _, err := q.Enqueue(context.Background(), "t", json.RawMessage{}, EnqueueOptions{}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	st.mu.Lock()
	defer st.mu.Unlock()
	if string(st.createPayload) != "{}" {
		t.Fatalf("expected payload {}, got %q", string(st.createPayload))
	}
}

func TestQueue_EnqueueMaxAttemptsDefaults(t *testing.T) {
	st := &recordingStore{createJob: Job{ID: 1, Type: "t"}}
	q := &Queue{Store: st}

	if _, err := q.Enqueue(context.Background(), "t", map[string]any{}, EnqueueOptions{MaxAttempts: 0}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	st.mu.Lock()
	defer st.mu.Unlock()
	if st.createOpt.MaxAttempts != DefaultMaxAttempts {
		t.Fatalf("expected MaxAttempts=%d, got %d", DefaultMaxAttempts, st.createOpt.MaxAttempts)
	}
}

func TestQueue_InlineRequiresRegistry(t *testing.T) {
	st := &recordingStore{createJob: Job{ID: 1, Type: "t"}}
	q := &Queue{Store: st, Inline: true}
	if _, err := q.Enqueue(context.Background(), "t", map[string]any{}, EnqueueOptions{}); err == nil {
		t.Fatalf("expected error for inline without registry")
	}
}

func TestQueue_InlineSuccessMarksSucceeded(t *testing.T) {
	st := &recordingStore{
		createJob: Job{ID: 1, Type: "demo"},
		claimJob:  Job{ID: 1, Type: "demo", Attempts: 0, MaxAttempts: 3},
	}
	reg := NewRegistry()
	reg.Register("demo", func(ctx context.Context, j Job) error { return nil })
	q := &Queue{Store: st, Registry: reg, Inline: true}

	if _, err := q.Enqueue(context.Background(), "demo", map[string]any{"x": 1}, EnqueueOptions{}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	st.mu.Lock()
	defer st.mu.Unlock()
	if len(st.markSucceededIDs) != 1 || st.markSucceededIDs[0] != 1 {
		t.Fatalf("expected MarkSucceeded(1), got %+v", st.markSucceededIDs)
	}
}

func TestQueue_InlineFailureMarksFailed(t *testing.T) {
	runErr := errors.New("boom")
	st := &recordingStore{
		createJob: Job{ID: 1, Type: "demo"},
		claimJob:  Job{ID: 1, Type: "demo", Attempts: 0, MaxAttempts: 2},
	}
	reg := NewRegistry()
	reg.Register("demo", func(ctx context.Context, j Job) error { return runErr })
	q := &Queue{Store: st, Registry: reg, Inline: true}

	_, err := q.Enqueue(context.Background(), "demo", map[string]any{}, EnqueueOptions{})
	if err == nil {
		t.Fatalf("expected error from inline handler")
	}

	st.mu.Lock()
	defer st.mu.Unlock()
	if len(st.markFailedCalls) != 1 {
		t.Fatalf("expected 1 MarkFailed call, got %d", len(st.markFailedCalls))
	}
	if st.markFailedCalls[0].id != 1 {
		t.Fatalf("unexpected failed id %d", st.markFailedCalls[0].id)
	}
}

func TestQueue_InlineFailureNoRetry(t *testing.T) {
	runErr := errors.New("boom")
	st := &recordingStore{
		createJob: Job{ID: 1, Type: "demo"},
		claimJob:  Job{ID: 1, Type: "demo", Attempts: 0, MaxAttempts: 10},
	}
	reg := NewRegistry()
	reg.Register("demo", func(ctx context.Context, j Job) error { return runErr })
	q := &Queue{Store: st, Registry: reg, Inline: true}

	_, err := q.Enqueue(context.Background(), "demo", map[string]any{}, EnqueueOptions{})
	if err == nil {
		t.Fatalf("expected error from inline handler")
	}

	st.mu.Lock()
	defer st.mu.Unlock()
	if len(st.markFailedCalls) != 1 {
		t.Fatalf("expected 1 MarkFailed call, got %d", len(st.markFailedCalls))
	}
	if st.markFailedCalls[0].id != 1 {
		t.Fatalf("unexpected failed id %d", st.markFailedCalls[0].id)
	}
	if len(st.markRetryCalls) != 0 {
		t.Fatalf("expected 0 MarkRetry calls, got %d", len(st.markRetryCalls))
	}
}

func TestQueue_EnqueueBytesPayload(t *testing.T) {
	st := &recordingStore{createJob: Job{ID: 1, Type: "t"}}
	q := &Queue{Store: st}

	raw := []byte(`{"key":"val"}`)
	if _, err := q.Enqueue(context.Background(), "t", raw, EnqueueOptions{}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	st.mu.Lock()
	defer st.mu.Unlock()
	if string(st.createPayload) != `{"key":"val"}` {
		t.Fatalf("expected payload {\"key\":\"val\"}, got %q", string(st.createPayload))
	}
}

func TestQueue_InlineClaimError(t *testing.T) {
	dbErr := errors.New("db gone")
	st := &recordingStore{
		createJob: Job{ID: 1, Type: "demo"},
		claimErr:  dbErr,
	}
	reg := NewRegistry()
	reg.Register("demo", func(ctx context.Context, j Job) error { return nil })
	q := &Queue{Store: st, Registry: reg, Inline: true}

	_, err := q.Enqueue(context.Background(), "demo", map[string]any{}, EnqueueOptions{})
	if !errors.Is(err, dbErr) {
		t.Fatalf("expected db error, got %v", err)
	}
}
