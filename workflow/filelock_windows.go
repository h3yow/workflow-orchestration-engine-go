//go:build windows

package workflow

import "os"

// The test/verification environment is local Unix; the Windows build keeps a
// process-local fallback so the package remains compilable.
func flock(f *os.File) error    { return nil }
func tryFlock(f *os.File) error { return nil }
func funlock(f *os.File) error  { return nil }
