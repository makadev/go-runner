## runner

`runner` is a small Go library for **database-backed background jobs** plus an in-process **scheduler** for periodic tasks.

- **Durable jobs**: enqueue into your database, process with one or more workers.
- **DB-agnostic**: you implement a small `runner.Storage` interface.
- **Inline mode**: run jobs immediately after enqueue (useful for tests/dev).
- **Scheduler**: periodic tasks that can enqueue durable jobs (schedules are not persisted).

### Install

```bash
go get github.com/makadev/go-runner@latest
```

Examples below assume `import "github.com/makadev/go-runner"`.

### Quickstart

```go
store := mydb.NewStore(db) // implements runner.Storage

reg := runner.NewRegistry()
reg.Register("demo.echo", func(ctx context.Context, j runner.Job) error {
	// decode j.Payload, do work
	return nil
})

q := &runner.Queue{
	Store:    store,
	Registry: reg,   // only required when Inline=true
	Inline:   false, // true in tests/dev if desired
}

w := &runner.Worker{
	Store:           store,
	Registry:        reg,
	WorkerID:        "worker-1",
	ProcessInterval: 10 * time.Second,
	BatchSize:       10,
	LockTTL:         15 * time.Minute,
}

// Soft shutdown: stop claiming/starting new jobs.
// Hard shutdown: cancel in-flight job work after a grace period.
softCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
defer stop()
hardCtx, cancelHard := context.WithTimeout(softCtx, 30*time.Second)
defer cancelHard()

// RunCoordinated processes a batch immediately on start (it does not wait for the first tick).
go w.RunCoordinated(softCtx, hardCtx)

_, _ = q.Enqueue(ctx, "demo.echo", map[string]any{"msg": "hello"}, runner.EnqueueOptions{})
```

### Options and defaults

- **`EnqueueOptions.MaxAttempts`**: when `MaxAttempts <= 0`, runner defaults it to **8**.
- **Inline mode**: when `Queue.Inline=true`, runner attempts the job **once** immediately after enqueue.
  If the handler returns an error, the job is marked **failed** right away (no retry/backoff loop in inline mode).
- **`Worker.WorkerID`**: when empty, runner uses `"worker"` as the lock owner identifier.
- **Retries/backoff**: on handler error in worker mode, runner schedules a retry via `Storage.MarkRetry` using `Worker.Backoff`.
  When `Worker.Backoff` is nil, runner uses `DefaultBackoff`: 1m, 5m, 15m, 1h, then 6h for all subsequent retries.

```go
// Example: override retry backoff.
w.Backoff = func(attempts int64) time.Duration {
	if attempts < 3 {
		return 10 * time.Second
	}
	return time.Minute
}
```

### Scheduler quickstart

```go
s := &runner.Scheduler{}
s.Register(runner.Schedule{
	Name:      "demo.tick",
	Every:     1 * time.Minute,
	RunOnBoot: true,
	Task: func(ctx context.Context) error {
		_, err := q.Enqueue(ctx, "demo.echo", map[string]any{"msg": "scheduled"}, runner.EnqueueOptions{})
		return err
	},
})
go s.Run(softCtx)
```

### Storage contract (important)

The `runner.Storage` interface is the portability boundary. Implementations **must** satisfy:

- **Atomic claim**: `ClaimJob` must ensure only one worker can claim a given job.
- **TTL reclaim**: `ClaimJob` must allow reclaiming stuck `running` jobs where `locked_at <= reclaimRunningBefore`.
- **Attempts accounting**: `MarkRetry` and `MarkFailed` must increment attempts (attempts live in storage).
- **Idempotency**: when `EnqueueOptions.IdempotencyKey` is provided, `CreateJob` must enforce uniqueness.
- **Semantics**: this is **at-least-once** delivery; handlers must be idempotent.
- **Scheduling**: `ListRunnableIDs` must only return jobs that are runnable now (for example, `status='pending'` and `run_at IS NULL OR run_at <= now`).

### SQLite schema (reference)

```sql
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS jobs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  type TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending',
  payload_json TEXT NOT NULL DEFAULT '{}',
  idempotency_key TEXT,
  attempts INTEGER NOT NULL DEFAULT 0,
  max_attempts INTEGER NOT NULL DEFAULT 8,
  last_error TEXT NOT NULL DEFAULT '',
  run_at TEXT,        -- RFC3339Nano UTC (or NULL for immediate)
  locked_at TEXT,     -- RFC3339Nano UTC (or NULL for unlocked)
  locked_by TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  finished_at TEXT    -- RFC3339Nano UTC (or NULL when not finished)
);

CREATE INDEX IF NOT EXISTS jobs_status_runat_id_idx ON jobs (status, run_at, id);
CREATE INDEX IF NOT EXISTS jobs_lockedat_idx ON jobs (locked_at);

CREATE UNIQUE INDEX IF NOT EXISTS jobs_idempotency_uidx
  ON jobs (idempotency_key)
  WHERE idempotency_key IS NOT NULL;
```

### SQLite `Storage` implementation (example)

This is a minimal `database/sql` + `modernc.org/sqlite` implementation of `runner.Storage`, based on the integration test store in `sqlite_integration_test.go`.

```go
type SQLiteStore struct{ DB *sql.DB }

func (s SQLiteStore) CreateJob(ctx context.Context, typ string, payload json.RawMessage, opt runner.EnqueueOptions) (runner.Job, error) {
	// INSERT ... RETURNING job columns (enforce idempotency key uniqueness in DB)
}

func (s SQLiteStore) ListRunnableIDs(ctx context.Context, limit int64) ([]int64, error) {
	// SELECT id FROM jobs WHERE status='pending' AND (run_at IS NULL OR run_at<=now) ORDER BY id ASC LIMIT ?
}

func (s SQLiteStore) ClaimJob(ctx context.Context, id int64, lockedBy string, reclaimRunningBefore time.Time) (runner.Job, error) {
	// UPDATE ... WHERE (pending OR (running AND locked_at<=reclaimBefore)) RETURNING ...
	// If no rows updated/returned: return runner.ErrNotClaimed
}

func (s SQLiteStore) MarkSucceeded(ctx context.Context, id int64) error { /* UPDATE status + clear lock */ return nil }
func (s SQLiteStore) MarkRetry(ctx context.Context, id int64, lastError string, runAt time.Time) error {
	// UPDATE status='pending', attempts=attempts+1, run_at=...
	return nil
}
func (s SQLiteStore) MarkFailed(ctx context.Context, id int64, lastError string) error {
	// UPDATE status='failed', attempts=attempts+1, finished_at=...
	return nil
}
```

### Postgres reference (docs-only)

If you’re implementing `runner.Storage` on Postgres, your schema will typically look like this:

```sql
CREATE TABLE jobs (
  id BIGSERIAL PRIMARY KEY,
  type TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending',
  payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  idempotency_key TEXT NULL,
  attempts BIGINT NOT NULL DEFAULT 0,
  max_attempts BIGINT NOT NULL DEFAULT 8,
  last_error TEXT NOT NULL DEFAULT '',
  run_at TIMESTAMPTZ NULL,
  locked_at TIMESTAMPTZ NULL,
  locked_by TEXT NOT NULL DEFAULT '',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at TIMESTAMPTZ NULL
);

CREATE INDEX jobs_runnable_idx ON jobs (status, run_at, id);
CREATE INDEX jobs_lockedat_idx ON jobs (locked_at);

CREATE UNIQUE INDEX jobs_idempotency_uidx
  ON jobs (idempotency_key)
  WHERE idempotency_key IS NOT NULL;
```

For atomic claim + TTL reclaim, implement `ClaimJob` with a single statement such as `UPDATE ... WHERE ... RETURNING ...` (or `SELECT ... FOR UPDATE SKIP LOCKED`), and return `runner.ErrNotClaimed` when nothing is claimed.

### Limitations / non-goals (v1)

- **No exactly-once guarantees** (at-least-once).
- **No persisted schedules** (scheduler is in-process only).
- **No priorities** (the example schemas/processors are FIFO by ID).
- **No heartbeat/lock extension** (set `LockTTL` to exceed your worst-case handler runtime).
