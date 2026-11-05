package runner

import (
	"fmt"
	"runtime/debug"
)

// PanicError is returned when a job handler or scheduled task panics.
// The panic is recovered so it cannot crash the process; the job is then
// treated like any other failed attempt.
type PanicError struct {
	// Value is the value passed to panic.
	Value any
	// Stack is the stack trace of the panicking goroutine.
	Stack []byte
}

func (e *PanicError) Error() string {
	return fmt.Sprintf("runner: panic: %v", e.Value)
}

// Unwrap returns Value when it is an error, so errors.Is/As see through the panic.
func (e *PanicError) Unwrap() error {
	if err, ok := e.Value.(error); ok {
		return err
	}
	return nil
}

// callRecover runs fn and converts a panic into a *PanicError.
func callRecover(fn func() error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = &PanicError{Value: r, Stack: debug.Stack()}
		}
	}()
	return fn()
}
