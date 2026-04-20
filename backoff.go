package runner

import "time"

// BackoffFunc returns the delay before retrying a job after the given attempt count.
// Attempts are zero-based for the first failure (i.e. attempts==0 means the first retry).
type BackoffFunc func(attempts int64) time.Duration

// DefaultBackoff matches the backoff curve from the original in-repo implementation.
func DefaultBackoff(attempts int64) time.Duration {
	switch attempts {
	case 0:
		return time.Minute
	case 1:
		return 5 * time.Minute
	case 2:
		return 15 * time.Minute
	case 3:
		return time.Hour
	default:
		return 6 * time.Hour
	}
}

