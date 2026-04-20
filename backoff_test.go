package runner

import (
	"testing"
	"time"
)

func TestDefaultBackoff(t *testing.T) {
	cases := []struct {
		attempts int64
		want     time.Duration
	}{
		{0, time.Minute},
		{1, 5 * time.Minute},
		{2, 15 * time.Minute},
		{3, time.Hour},
		{4, 6 * time.Hour},
		{10, 6 * time.Hour},
	}
	for _, tc := range cases {
		if got := DefaultBackoff(tc.attempts); got != tc.want {
			t.Fatalf("attempts=%d: got %v want %v", tc.attempts, got, tc.want)
		}
	}
}

