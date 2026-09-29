package workflow

import (
	"fmt"
	"sync"
	"testing"
)

// testLogger is a deterministic logger used by unit tests. It emits key
// inputs, scheduling decisions and final states via t.Logf so they appear
// in `go test -v` output, and captures them for optional assertions.
type testLogger struct {
	t     testing.TB
	mu    sync.Mutex
	lines []string
}

func newTestLogger(t testing.TB) *testLogger {
	t.Helper()
	return &testLogger{t: t}
}

func (l *testLogger) logf(level, format string, args ...any) {
	msg := fmt.Sprintf("%-5s "+format, append([]any{level}, args...)...)
	l.mu.Lock()
	l.lines = append(l.lines, msg)
	l.mu.Unlock()
	l.t.Log(msg)
}

func (l *testLogger) Debugf(format string, args ...any) { l.logf("DEBUG", format, args...) }
func (l *testLogger) Infof(format string, args ...any)  { l.logf("INFO", format, args...) }
func (l *testLogger) Warnf(format string, args ...any)  { l.logf("WARN", format, args...) }
func (l *testLogger) Errorf(format string, args ...any) { l.logf("ERROR", format, args...) }

func (l *testLogger) contains(sub string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, x := range l.lines {
		if containsStr(x, sub) {
			return true
		}
	}
	return false
}

func containsStr(s, sub string) bool {
	return len(sub) == 0 || (len(s) >= len(sub) && indexStr(s, sub) >= 0)
}

func indexStr(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

func newTestEngine(t testing.TB, cfg Config) *Engine {
	t.Helper()
	if cfg.StateDir == "" {
		cfg.StateDir = t.TempDir()
	}
	eng, err := NewEngine(cfg)
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	eng.SetLogger(newTestLogger(t))
	return eng
}
