//go:build !windows

package workflow

import (
	"os"
	"syscall"
)

// flock blocks until an exclusive advisory lock on f is acquired. Locks are
// tied to the open file description and released automatically on close or
// process exit, which gives correct semantics even after a crash.
func flock(f *os.File) error {
	return syscall.Flock(int(f.Fd()), syscall.LOCK_EX)
}

// tryFlock acquires the lock only if it is immediately free.
func tryFlock(f *os.File) error {
	return syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
}

func funlock(f *os.File) error {
	return syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
}
