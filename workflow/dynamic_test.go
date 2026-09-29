package workflow

import (
	"context"
	"sync/atomic"
	"testing"
)

// TestDynamicSpawn 任务运行中生成两个新任务，其中后者依赖前者，按依赖执行。
func TestDynamicSpawn(t *testing.T) {
	dir := t.TempDir()
	e := newTestEngine(t, dir)

	var order []string
	var mu muRecorder
	_ = e.RegisterHandler("fan", func(ec *ExecContext) (map[string]any, error) {
		mu.Lock()
		order = append(order, "fan")
		mu.Unlock()
		err := ec.Spawn(
			TaskDefinition{ID: "dyn-1", Handler: "leaf"},
			TaskDefinition{ID: "dyn-2", Handler: "leaf", DependsOn: []string{"dyn-1"}},
		)
		return nil, err
	})
	_ = e.RegisterHandler("leaf", func(ec *ExecContext) (map[string]any, error) {
		mu.Lock()
		order = append(order, ec.TaskID)
		mu.Unlock()
		return map[string]any{"ran": ec.TaskID}, nil
	})

	def := &Definition{Name: "dyn", Tasks: []TaskDefinition{
		{ID: "fan", Handler: "fan"},
	}}
	st, err := e.Start(context.Background(), def, "wf-dyn")
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if st.Status != WFSucceeded {
		t.Fatalf("status=%s cause=%s", st.Status, st.FailureCause)
	}
	for _, id := range []string{"dyn-1", "dyn-2"} {
		ts, ok := st.Tasks[id]
		if !ok {
			t.Fatalf("dynamic task %s missing", id)
		}
		if !ts.Definition.Dynamic {
			t.Fatalf("task %s should be marked Dynamic", id)
		}
		if ts.Status != StatusSucceeded {
			t.Fatalf("task %s status=%s", id, ts.Status)
		}
	}
	got := mu.snapshot(&order)
	want := []string{"fan", "dyn-1", "dyn-2"}
	if !equalStrings(got, want) {
		t.Fatalf("execution order=%v want %v", got, want)
	}
}

// TestDynamicSpawnDuplicateID 动态任务 ID 与已有任务冲突：工作流失败，原因可区分。
func TestDynamicSpawnDuplicateID(t *testing.T) {
	dir := t.TempDir()
	e := newTestEngine(t, dir)
	_ = e.RegisterHandler("dup", func(ec *ExecContext) (map[string]any, error) {
		return nil, ec.Spawn(TaskDefinition{ID: "existing", Handler: "noop"})
	})
	var noopCalls int32
	_ = e.RegisterHandler("noop", func(ec *ExecContext) (map[string]any, error) {
		atomic.AddInt32(&noopCalls, 1)
		return nil, nil
	})
	def := &Definition{Name: "dupdyn", Tasks: []TaskDefinition{
		{ID: "dup", Handler: "dup"},
		{ID: "existing", Handler: "noop"},
	}}
	st, err := e.Start(context.Background(), def, "wf-dupdyn")
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if st.Status != WFFailed || st.FailureCause == "" {
		t.Fatalf("status=%s cause=%q want failed with cause", st.Status, st.FailureCause)
	}
}

// TestDynamicSpawnCycle 动态任务制造依赖环：被拒绝，工作流失败。
func TestDynamicSpawnCycle(t *testing.T) {
	dir := t.TempDir()
	e := newTestEngine(t, dir)
	_ = e.RegisterHandler("cyc", func(ec *ExecContext) (map[string]any, error) {
		// spawn x，x 依赖一个将依赖 x 的 y，形成环
		return nil, ec.Spawn(
			TaskDefinition{ID: "x", Handler: "noop", DependsOn: []string{"y"}},
			TaskDefinition{ID: "y", Handler: "noop", DependsOn: []string{"x"}},
		)
	})
	_ = e.RegisterHandler("noop", func(ec *ExecContext) (map[string]any, error) { return nil, nil })
	def := &Definition{Name: "cycdyn", Tasks: []TaskDefinition{{ID: "cyc", Handler: "cyc"}}}
	st, err := e.Start(context.Background(), def, "wf-cyc")
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if st.Status != WFFailed {
		t.Fatalf("status=%s want failed", st.Status)
	}
}

type muRecorder struct {
	ch chan struct{}
}

func (m *muRecorder) Lock()   {}
func (m *muRecorder) Unlock() {}
func (m *muRecorder) snapshot(p *[]string) []string {
	out := make([]string, len(*p))
	copy(out, *p)
	return out
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
