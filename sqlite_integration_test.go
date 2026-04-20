package runner

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

const sqliteSchema = `PRAGMA foreign_keys = ON;

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
`

type sqliteStore struct {
	db *sql.DB
}

func newSQLiteStore(t *testing.T) (*sqliteStore, func()) {
	t.Helper()

	db, err := sql.Open("sqlite", "file:runner-test?mode=memory&cache=shared")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	if _, err := db.Exec(sqliteSchema); err != nil {
		_ = db.Close()
		t.Fatalf("apply schema: %v", err)
	}
	return &sqliteStore{db: db}, func() { _ = db.Close() }
}

func (s *sqliteStore) CreateJob(ctx context.Context, typ string, payload json.RawMessage, opt EnqueueOptions) (Job, error) {
	maxAttempts := opt.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 8
	}
	var idem any
	if opt.IdempotencyKey != nil && *opt.IdempotencyKey != "" {
		idem = *opt.IdempotencyKey
	}
	var runAt any
	if opt.RunAt != nil && !opt.RunAt.IsZero() {
		runAt = opt.RunAt.UTC().Format(time.RFC3339Nano)
	}

	const q = `
INSERT INTO jobs (type, status, payload_json, idempotency_key, max_attempts, run_at)
VALUES (?, 'pending', ?, ?, ?, ?)
RETURNING id, type, status, payload_json, idempotency_key, attempts, max_attempts, last_error,
  run_at, locked_at, locked_by, created_at, updated_at, finished_at;
`
	row := s.db.QueryRowContext(ctx, q, typ, string(payload), idem, maxAttempts, runAt)
	return scanSQLiteJob(row)
}

func (s *sqliteStore) ListRunnableIDs(ctx context.Context, limit int64) ([]int64, error) {
	const q = `
SELECT id
FROM jobs
WHERE status = 'pending'
  AND (run_at IS NULL OR run_at = '' OR run_at <= strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
ORDER BY id ASC
LIMIT ?;
`
	rows, err := s.db.QueryContext(ctx, q, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

func (s *sqliteStore) ClaimJob(ctx context.Context, id int64, lockedBy string, reclaimRunningBefore time.Time) (Job, error) {
	reclaimBeforeStr := reclaimRunningBefore.UTC().Format(time.RFC3339Nano)
	const q = `
UPDATE jobs
SET
  status = 'running',
  locked_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
  locked_by = ?,
  updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
WHERE id = ?
  AND (
    status = 'pending'
    OR (
      status = 'running'
      AND locked_at IS NOT NULL
      AND locked_at != ''
      AND locked_at <= ?
    )
  )
RETURNING id, type, status, payload_json, idempotency_key, attempts, max_attempts, last_error,
  run_at, locked_at, locked_by, created_at, updated_at, finished_at;
`
	row := s.db.QueryRowContext(ctx, q, lockedBy, id, reclaimBeforeStr)
	j, err := scanSQLiteJob(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Job{}, ErrNotClaimed
		}
		return Job{}, err
	}
	return j, nil
}

func (s *sqliteStore) MarkSucceeded(ctx context.Context, id int64) error {
	_, err := s.db.ExecContext(ctx, `UPDATE jobs SET status='succeeded', last_error='', locked_at=NULL, locked_by='', finished_at=strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), updated_at=strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE id=?`, id)
	return err
}

func (s *sqliteStore) MarkRetry(ctx context.Context, id int64, lastError string, runAt time.Time) error {
	_, err := s.db.ExecContext(ctx, `UPDATE jobs SET status='pending', attempts=attempts+1, last_error=?, run_at=?, locked_at=NULL, locked_by='', updated_at=strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE id=?`, lastError, runAt.UTC().Format(time.RFC3339Nano), id)
	return err
}

func (s *sqliteStore) MarkFailed(ctx context.Context, id int64, lastError string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE jobs SET status='failed', attempts=attempts+1, last_error=?, locked_at=NULL, locked_by='', finished_at=strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), updated_at=strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE id=?`, lastError, id)
	return err
}

type sqliteScanner interface{ Scan(dest ...any) error }

func scanSQLiteJob(row sqliteScanner) (Job, error) {
	var (
		j              Job
		statusStr      string
		payloadStr     string
		idem           sql.NullString
		runAtStr       sql.NullString
		lockedAtStr    sql.NullString
		createdAtStr   string
		updatedAtStr   string
		finishedAtStr  sql.NullString
	)
	err := row.Scan(
		&j.ID, &j.Type, &statusStr, &payloadStr, &idem, &j.Attempts, &j.MaxAttempts, &j.LastError,
		&runAtStr, &lockedAtStr, &j.LockedBy, &createdAtStr, &updatedAtStr, &finishedAtStr,
	)
	if err != nil {
		return Job{}, err
	}
	j.Status = Status(statusStr)
	j.Payload = json.RawMessage(payloadStr)
	if idem.Valid {
		s := idem.String
		j.IdempotencyKey = &s
	}
	j.RunAt = parseNullableTime(runAtStr)
	j.LockedAt = parseNullableTime(lockedAtStr)
	if t, err := time.Parse(time.RFC3339Nano, createdAtStr); err == nil {
		j.CreatedAt = t.UTC()
	}
	if t, err := time.Parse(time.RFC3339Nano, updatedAtStr); err == nil {
		j.UpdatedAt = t.UTC()
	}
	j.FinishedAt = parseNullableTime(finishedAtStr)
	return j, nil
}

func parseNullableTime(ns sql.NullString) *time.Time {
	if !ns.Valid || ns.String == "" {
		return nil
	}
	if t, err := time.Parse(time.RFC3339Nano, ns.String); err == nil {
		tt := t.UTC()
		return &tt
	}
	if t, err := time.Parse("2006-01-02T15:04:05.000Z", ns.String); err == nil {
		tt := t.UTC()
		return &tt
	}
	return nil
}

func TestSQLite_AtomicClaimRace(t *testing.T) {
	st, cleanup := newSQLiteStore(t)
	defer cleanup()

	ctx := context.Background()
	j, err := st.CreateJob(ctx, "demo", json.RawMessage(`{}`), EnqueueOptions{})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	var wg sync.WaitGroup
	wg.Add(2)

	var (
		got1 Job
		got2 Job
		err1 error
		err2 error
	)

	go func() {
		defer wg.Done()
		got1, err1 = st.ClaimJob(ctx, j.ID, "w1", time.Now().Add(-time.Hour))
	}()
	go func() {
		defer wg.Done()
		got2, err2 = st.ClaimJob(ctx, j.ID, "w2", time.Now().Add(-time.Hour))
	}()

	wg.Wait()

	claimed := 0
	if err1 == nil {
		claimed++
		_ = got1
	} else if !errors.Is(err1, ErrNotClaimed) {
		t.Fatalf("unexpected err1: %v", err1)
	}
	if err2 == nil {
		claimed++
		_ = got2
	} else if !errors.Is(err2, ErrNotClaimed) {
		t.Fatalf("unexpected err2: %v", err2)
	}
	if claimed != 1 {
		t.Fatalf("expected exactly 1 claim, got %d (err1=%v err2=%v)", claimed, err1, err2)
	}
}

func TestSQLite_TTLReclaim(t *testing.T) {
	st, cleanup := newSQLiteStore(t)
	defer cleanup()

	ctx := context.Background()
	j, err := st.CreateJob(ctx, "demo", json.RawMessage(`{}`), EnqueueOptions{})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	// First claim to set running + locked_at.
	_, err = st.ClaimJob(ctx, j.ID, "w1", time.Now().Add(-time.Hour))
	if err != nil {
		t.Fatalf("claim: %v", err)
	}

	// Make it look stuck by setting locked_at in the past.
	_, err = st.db.ExecContext(ctx, `UPDATE jobs SET locked_at=? WHERE id=?`, time.Now().Add(-24*time.Hour).UTC().Format(time.RFC3339Nano), j.ID)
	if err != nil {
		t.Fatalf("update locked_at: %v", err)
	}

	// Reclaim should succeed.
	_, err = st.ClaimJob(ctx, j.ID, "w2", time.Now().Add(-time.Minute))
	if err != nil {
		t.Fatalf("reclaim claim: %v", err)
	}
}

func TestSQLite_RunAtFiltering(t *testing.T) {
	st, cleanup := newSQLiteStore(t)
	defer cleanup()

	ctx := context.Background()
	future := time.Now().Add(2 * time.Hour)
	_, err := st.CreateJob(ctx, "demo", json.RawMessage(`{}`), EnqueueOptions{RunAt: &future})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	ids, err := st.ListRunnableIDs(ctx, 10)
	if err != nil {
		t.Fatalf("list runnable: %v", err)
	}
	if len(ids) != 0 {
		t.Fatalf("expected 0 runnable ids, got %v", ids)
	}
}

func TestSQLite_IdempotencyUnique(t *testing.T) {
	st, cleanup := newSQLiteStore(t)
	defer cleanup()

	ctx := context.Background()
	key := "abc"
	_, err := st.CreateJob(ctx, "demo", json.RawMessage(`{}`), EnqueueOptions{IdempotencyKey: &key})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	_, err = st.CreateJob(ctx, "demo", json.RawMessage(`{}`), EnqueueOptions{IdempotencyKey: &key})
	if err == nil {
		t.Fatalf("expected idempotency conflict error")
	}
}

