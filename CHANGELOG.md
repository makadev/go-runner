# Changelog

## Unreleased (planned: v1.1.0)

This release changes the `Storage` interface. Existing `Storage` implementations must be
updated (see "Upgrading" below).

### Changed

- **Breaking for `Storage` implementations:** `MarkSucceeded`, `MarkRetry` and `MarkFailed` take
  the `lockedBy` value of the claim and must only update a job that is still `running` and locked by it.
  Otherwise they must return the new `ErrLockLost`. This stops a worker whose lock expired from
  overwriting the outcome of a job that was reclaimed by another worker.
- **Storage contract:** `ClaimJob` must increment `attempts` when it reclaims a stuck `running` job.
  A job that crashes its worker now ends up `failed` after `MaxAttempts` instead of looping forever.
- Lock owners are unique per claim (`<WorkerID>/<random token>`), so ownership checks also work when
  several workers share a `WorkerID`.
- On hard shutdown the job outcome is written with a context detached from `hardCtx` (bounded by the
  new `Worker.FinalizeTimeout` / `Queue.FinalizeTimeout`, default 10s). Previously the write failed
  with `context.Canceled` and the job stayed `running` until `LockTTL` expired. A handler interrupted
  by the hard shutdown is retried immediately instead of after the backoff delay.
- `ErrNotClaimed` and `ErrLockLost` are matched with `errors.Is`, so stores may wrap them.
- Minimum Go version is 1.26; CI tests Go 1.26 and 1.27.

### Added

- Panics in job handlers (`Registry.Handle`) and scheduled tasks are recovered and reported as
  `*PanicError` (panic value and stack trace). Worker jobs are retried like any other failure.
- `ErrLockLost`, `PanicError`, `DefaultFinalizeTimeout`, `Worker.FinalizeTimeout`, `Queue.FinalizeTimeout`.
- CI: `gofmt`, `go vet`, `go mod tidy` check, race detector and `govulncheck`.
  `make check` and `make vulncheck` run the same checks locally.

### Upgrading

1. Add a `lockedBy string` parameter to `MarkSucceeded`, `MarkRetry` and `MarkFailed` and add
   `AND status = 'running' AND locked_by = ?` to their `UPDATE` statements. When no row was affected,
   return `runner.ErrLockLost`.
2. In `ClaimJob`, increment `attempts` when the claimed row was `running` (a reclaim), for example
   `attempts = attempts + CASE WHEN status = 'running' THEN 1 ELSE 0 END`.
3. If you read `locked_by` elsewhere: it now contains `<WorkerID>/<token>` instead of the bare `WorkerID`.

See `sqlite_integration_test.go` for a complete reference implementation.
