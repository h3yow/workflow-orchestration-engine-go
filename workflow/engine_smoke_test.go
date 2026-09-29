package workflow

import (
	"bytes"
	"context"
	"sync"
	"testing"
)

// safeBuffer 是并发安全的日志缓冲：工作 goroutine 写、测试 cleanup 读。
type safeBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *safeBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *safeBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func newTestEngine(t *testing.T, dir string) *Engine {
	t.Helper()
	return newTestEngineWith(t, dir, Config{})
}

func newTestEngineWith(t *testing.T, dir string, override Config) *Engine {
	t.Helper()
	buf := &safeBuffer{}
	override.StateDir = dir
	e, err := NewEngine(override)
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	e.WithLogger(NewLogger(buf))
	t.Cleanup(func() { t.Logf("ENGINE LOG:\n%s", buf.String()) })
	return e
}

func TestSmokeHappyPath(t *testing.T) {
	dir := t.TempDir()
	e := newTestEngine(t, dir)

	_ = e.RegisterHandler("mk", func(ec *ExecContext) (map[string]any, error) {
		v := intVal(ec.Params["v"])
		return map[string]any{"n": v}, nil
	})
	_ = e.RegisterHandler("add", func(ec *ExecContext) (map[string]any, error) {
		sum := 0
		for _, r := range ec.Results {
			sum += intVal(r["n"])
		}
		return map[string]any{"n": sum}, nil
	})

	def := &Definition{
		Name: "smoke",
		Tasks: []TaskDefinition{
			{ID: "a", Handler: "mk", Params: map[string]any{"v": 1}},
			{ID: "b", Handler: "mk", Params: map[string]any{"v": 2}},
			{ID: "c", Handler: "add", DependsOn: []string{"a", "b"}},
		},
	}
	st, err := e.Start(context.Background(), def, "wf-1")
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if st.Status != WFSucceeded {
		t.Fatalf("status=%s cause=%s", st.Status, st.FailureCause)
	}
	if got := intVal(st.Tasks["c"].Result["n"]); got != 3 {
		t.Fatalf("c.n=%d want 3", got)
	}
	for _, id := range []string{"a", "b", "c"} {
		if st.Tasks[id].Status != StatusSucceeded {
			t.Fatalf("task %s status=%s", id, st.Tasks[id].Status)
		}
	}
}

func intVal(v any) int {
	switch n := v.(type) {
	case int:
		return n
	case float64:
		return int(n)
	default:
		return 0
	}
}
