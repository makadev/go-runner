package runner

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"
)

type memStore struct {
	mu sync.Mutex

	job Job

	listIDs []int64

	claimed  bool
	claimErr error

	succeeded []int64
	retried   []struct {
		id    int64
		err   string
		runAt time.Time
	}
	failed []struct {
		id  int64
		err string
	}
}

func (m *memStore) CreateJob(ctx context.Context, typ string, payload json.RawMessage, opt EnqueueOptions) (Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.job = Job{ID: 1, Type: typ, Status: StatusPending, Payload: payload, Attempts: 0, MaxAttempts: 2}
	return m.job, nil
}

func (m *memStore) ListRunnableIDs(ctx context.Context, limit int64) ([]int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]int64(nil), m.listIDs...), nil
}

func (m *memStore) ClaimJob(ctx context.Context, id int64, lockedBy string, reclaimRunningBefore time.Time) (Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.claimErr != nil {
		return Job{}, m.claimErr
	}
	if m.claimed {
		return Job{}, ErrNotClaimed
	}
	m.claimed = true
	return m.job, nil
}

func (m *memStore) MarkSucceeded(ctx context.Context, id int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.succeeded = append(m.succeeded, id)
	return nil
}

func (m *memStore) MarkRetry(ctx context.Context, id int64, lastError string, runAt time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.retried = append(m.retried, struct {
		id    int64
		err   string
		runAt time.Time
	}{id: id, err: lastError, runAt: runAt})
	return nil
}

func (m *memStore) MarkFailed(ctx context.Context, id int64, lastError string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failed = append(m.failed, struct {
		id  int64
		err string
	}{id: id, err: lastError})
	return nil
}

func TestWorker_SuccessMarksSucceeded(t *testing.T) {
	ms := &memStore{listIDs: []int64{1}}
	reg := NewRegistry()
	reg.Register("demo", func(ctx context.Context, j Job) error { return nil })
	ms.job = Job{ID: 1, Type: "demo", Attempts: 0, MaxAttempts: 2}

	w := &Worker{
		Store:           ms,
		Registry:        reg,
		ProcessInterval: time.Hour,
		BatchSize:       10,
		LockTTL:         time.Minute,
	}

	ctx := context.Background()
	_ = w.processJobIDCoordinated(ctx, ctx, "w1", 1, time.Minute, DefaultBackoff)

	if len(ms.succeeded) != 1 || ms.succeeded[0] != 1 {
		t.Fatalf("expected MarkSucceeded(1), got %+v", ms.succeeded)
	}
}

func TestWorker_FailureRetriesThenFails(t *testing.T) {
	ms := &memStore{listIDs: []int64{1}}
	reg := NewRegistry()
	reg.Register("demo", func(ctx context.Context, j Job) error { return context.DeadlineExceeded })
	ms.job = Job{ID: 1, Type: "demo", Attempts: 0, MaxAttempts: 2}

	w := &Worker{
		Store:    ms,
		Registry: reg,
		LockTTL:  time.Minute,
		Backoff:  func(attempts int64) time.Duration { return 123 * time.Millisecond },
	}

	soft := context.Background()
	hard := context.Background()

	// First failure -> retry
	before := time.Now().UTC()
	if err := w.processJobIDCoordinated(soft, hard, "w1", 1, time.Minute, w.Backoff); err != nil {
		t.Fatalf("expected nil error on retry path, got %v", err)
	}
	if len(ms.retried) != 1 {
		t.Fatalf("expected 1 retry, got %d", len(ms.retried))
	}
	// Verify backoff wiring: runAt should be ~now+123ms.
	expectedRunAt := before.Add(123 * time.Millisecond)
	if diff := ms.retried[0].runAt.Sub(expectedRunAt); diff < -50*time.Millisecond || diff > 50*time.Millisecond {
		t.Fatalf("retry runAt off by %v (expected ~%v, got %v)", diff, expectedRunAt, ms.retried[0].runAt)
	}

	// Pretend attempts already incremented in storage, then next run fails terminally.
	ms.mu.Lock()
	ms.claimed = false
	ms.job.Attempts = 1
	ms.mu.Unlock()
	if err := w.processJobIDCoordinated(soft, hard, "w1", 1, time.Minute, w.Backoff); err != nil {
		t.Fatalf("expected nil error on terminal fail path, got %v", err)
	}

	if len(ms.failed) != 1 {
		t.Fatalf("expected 1 failed, got %d", len(ms.failed))
	}
}

func TestWorker_HardCtxCancelsHandler(t *testing.T) {
	ms := &memStore{}
	reg := NewRegistry()

	done := make(chan struct{})
	reg.Register("block", func(ctx context.Context, j Job) error {
		close(done)
		<-ctx.Done()
		return ctx.Err()
	})

	ms.job = Job{ID: 1, Type: "block", Attempts: 0, MaxAttempts: 10}

	w := &Worker{
		Store:    ms,
		Registry: reg,
		LockTTL:  time.Minute,
		Backoff:  func(attempts int64) time.Duration { return 0 },
	}

	soft := context.Background()
	hard, cancelHard := context.WithCancel(context.Background())

	errCh := make(chan error, 1)
	go func() {
		errCh <- w.processJobIDCoordinated(soft, hard, "w1", 1, time.Minute, w.Backoff)
	}()

	<-done
	cancelHard()

	select {
	case <-time.After(2 * time.Second):
		t.Fatalf("worker did not return after hard ctx cancel")
	case err := <-errCh:
		// Handler returns context.Canceled -> failOrRetry marks retry; expect nil from failOrRetry.
		if err != nil {
			t.Fatalf("expected nil error after hard cancel, got %v", err)
		}
	}
}

func TestWorker_RunCoordinated(t *testing.T) {
	ms := &memStore{
		listIDs: []int64{1},
		job:     Job{ID: 1, Type: "demo", Attempts: 0, MaxAttempts: 2},
	}
	reg := NewRegistry()
	reg.Register("demo", func(ctx context.Context, j Job) error { return nil })

	w := &Worker{
		Store:           ms,
		Registry:        reg,
		ProcessInterval: time.Hour, // long interval so only the immediate first batch runs
		BatchSize:       10,
		LockTTL:         time.Minute,
	}

	softCtx, softCancel := context.WithCancel(context.Background())
	hardCtx := context.Background()

	done := make(chan struct{})
	go func() {
		w.RunCoordinated(softCtx, hardCtx)
		close(done)
	}()

	// Give worker time to process the immediate batch, then stop.
	time.Sleep(100 * time.Millisecond)
	softCancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("RunCoordinated did not return after soft cancel")
	}

	ms.mu.Lock()
	defer ms.mu.Unlock()
	if len(ms.succeeded) != 1 || ms.succeeded[0] != 1 {
		t.Fatalf("expected MarkSucceeded(1), got %+v", ms.succeeded)
	}
}

func TestWorker_RunBatchSoftCtxStopsMidBatch(t *testing.T) {
	softCtx, softCancel := context.WithCancel(context.Background())

	callCount := 0
	ms := &memStore{
		listIDs: []int64{1, 2, 3},
		job:     Job{ID: 1, Type: "demo", Attempts: 0, MaxAttempts: 2},
	}
	reg := NewRegistry()
	reg.Register("demo", func(ctx context.Context, j Job) error {
		callCount++
		softCancel() // cancel after first job
		return nil
	})

	w := &Worker{
		Store:    ms,
		Registry: reg,
		LockTTL:  time.Minute,
	}

	_ = w.runBatchCoordinated(softCtx, context.Background(), "w1", 3, time.Minute, DefaultBackoff)

	if callCount != 1 {
		t.Fatalf("expected handler called once before soft cancel, got %d", callCount)
	}
}

func TestWorker_ClaimJobError(t *testing.T) {
	dbErr := errors.New("db connection lost")
	ms := &memStore{
		listIDs:  []int64{1},
		job:      Job{ID: 1, Type: "demo", Attempts: 0, MaxAttempts: 2},
		claimErr: dbErr,
	}
	reg := NewRegistry()
	reg.Register("demo", func(ctx context.Context, j Job) error { return nil })

	w := &Worker{
		Store:    ms,
		Registry: reg,
		LockTTL:  time.Minute,
	}

	err := w.processJobIDCoordinated(context.Background(), context.Background(), "w1", 1, time.Minute, DefaultBackoff)
	if !errors.Is(err, dbErr) {
		t.Fatalf("expected db error, got %v", err)
	}
}

func TestWorker_RunCoordinatedNilGuards(t *testing.T) {
	ctx := context.Background()

	// nil worker
	var w *Worker
	done := make(chan struct{})
	go func() {
		w.RunCoordinated(ctx, ctx)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatalf("nil worker RunCoordinated did not return")
	}

	// nil store
	w = &Worker{}
	done = make(chan struct{})
	go func() {
		w.RunCoordinated(ctx, ctx)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatalf("nil store RunCoordinated did not return")
	}

	// nil registry
	w = &Worker{Store: &memStore{}}
	done = make(chan struct{})
	go func() {
		w.RunCoordinated(ctx, ctx)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatalf("nil registry RunCoordinated did not return")
	}
}
