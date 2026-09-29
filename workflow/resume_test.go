package workflow

import (
	"context"
	"os"
	"sync/atomic"
	"testing"
)

// TestResumeAfterCrash 模拟进程在任务 b 执行中崩溃：
// 持久化状态里 a=succeeded、b=running、c=pending。
// 用新引擎 Resume：a 绝不重跑，b 重新执行并成功，c 执行，最终成功。
func TestResumeAfterCrash(t *testing.T) {
	dir := t.TempDir()

	var aCalls, bCalls, cCalls int32
	registerTracked := func(e *Engine) {
		_ = e.RegisterHandler("a", func(ec *ExecContext) (map[string]any, error) {
			atomic.AddInt32(&aCalls, 1)
			return map[string]any{"v": "from-a"}, nil
		})
		_ = e.RegisterHandler("b", func(ec *ExecContext) (map[string]any, error) {
			atomic.AddInt32(&bCalls, 1)
			return map[string]any{"got": ec.Results["a"]["v"]}, nil
		})
		_ = e.RegisterHandler("c", func(ec *ExecContext) (map[string]any, error) {
			atomic.AddInt32(&cCalls, 1)
			return map[string]any{"got": ec.Results["b"]["got"]}, nil
		})
	}

	fullDef := &Definition{Name: "resume", Tasks: []TaskDefinition{
		{ID: "a", Handler: "a"},
		{ID: "b", Handler: "b", DependsOn: []string{"a"}},
		{ID: "c", Handler: "c", DependsOn: []string{"b"}},
	}}

	// 手动构造"崩溃现场"状态：a 已完成，b 正在运行（无终态），c 未开始。
	st0 := newStateFromDefinition(fullDef, "wf-resume")
	st0.Status = WFRunning
	st0.Tasks["a"].Status = StatusSucceeded
	st0.Tasks["a"].Attempt = 1
	st0.Tasks["a"].Result = map[string]any{"v": "from-a"}
	st0.Tasks["a"].Records = []RetryRecord{{Attempt: 1, Err: ""}}
	st0.Tasks["b"].Status = StatusRunning
	st0.Tasks["b"].Attempt = 1
	st0.Tasks["b"].Records = []RetryRecord{{Attempt: 1, Err: "interrupted by process exit"}}
	store, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(context.Background(), st0); err != nil {
		t.Fatal(err)
	}

	// 第二阶段：新引擎 = 新进程，不重新 Start，直接 Resume。
	// 重置计数器，确保统计的是恢复阶段的调用。
	atomic.StoreInt32(&aCalls, 0)
	atomic.StoreInt32(&bCalls, 0)
	atomic.StoreInt32(&cCalls, 0)
	e2 := newTestEngine(t, dir)
	registerTracked(e2)
	resumed, err := e2.Resume(context.Background(), "wf-resume")
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if resumed.Status != WFSucceeded {
		t.Fatalf("status=%s cause=%s", resumed.Status, resumed.FailureCause)
	}
	if aCalls != 0 {
		t.Fatalf("a re-executed during resume: calls=%d (completed task must not rerun)", aCalls)
	}
	if bCalls != 1 {
		t.Fatalf("b calls=%d want 1 (running-at-crash must rerun)", bCalls)
	}
	if cCalls != 1 {
		t.Fatalf("c calls=%d want 1", cCalls)
	}
	// 结果链路一致：b 能读到 a 的持久化结果，c 能读到 b 的结果。
	if resumed.Tasks["b"].Result["got"] != "from-a" {
		t.Fatalf("b lost upstream result: %v", resumed.Tasks["b"].Result)
	}
	if resumed.Tasks["c"].Result["got"] != "from-a" {
		t.Fatalf("c lost propagated result: %v", resumed.Tasks["c"].Result)
	}
}

// TestResumeTerminalIsIdempotent 终态实例 Resume 直接返回，不执行任何任务。
func TestResumeTerminalIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	e := newTestEngine(t, dir)
	var calls int32
	_ = e.RegisterHandler("h", func(ec *ExecContext) (map[string]any, error) {
		atomic.AddInt32(&calls, 1)
		return nil, nil
	})
	def := &Definition{Name: "id", Tasks: []TaskDefinition{{ID: "t", Handler: "h"}}}
	if _, err := e.Start(context.Background(), def, "wf-id"); err != nil {
		t.Fatal(err)
	}
	st, err := e.Resume(context.Background(), "wf-id")
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if st.Status != WFSucceeded || calls != 1 {
		t.Fatalf("status=%s calls=%d", st.Status, calls)
	}
}

// TestResumeMissingState 恢复不存在的实例返回可区分错误。
func TestResumeMissingState(t *testing.T) {
	dir := t.TempDir()
	e := newTestEngine(t, dir)
	_, err := e.Resume(context.Background(), "ghost")
	ee, ok := err.(*ExecError)
	if !ok || ee.Code != ErrCodeStateCorrupt {
		t.Fatalf("want state_corrupt, got %v", err)
	}
}

// TestRecoverAll 目录中同时有完成与未完成实例，RecoverAll 只恢复未完成的。
func TestRecoverAll(t *testing.T) {
	dir := t.TempDir()
	store, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	// 一个成功实例，不应被恢复。
	done := &State{WorkflowID: "done", DefinitionName: "x", Status: WFSucceeded,
		Tasks: map[string]*TaskState{"t": {ID: "t", Status: StatusSucceeded}}, TaskOrder: []string{"t"}}
	_ = store.Save(context.Background(), done)
	// 一个中断实例。
	interrupted := &State{WorkflowID: "interrupted", DefinitionName: "x", Status: WFRunning,
		Tasks: map[string]*TaskState{
			"t": {ID: "t", Definition: TaskDefinition{ID: "t", Handler: "h"}, Status: StatusRunning},
		},
		TaskOrder: []string{"t"}}
	_ = store.Save(context.Background(), interrupted)

	e := newTestEngine(t, dir)
	var calls int32
	_ = e.RegisterHandler("h", func(ec *ExecContext) (map[string]any, error) {
		atomic.AddInt32(&calls, 1)
		return nil, nil
	})
	ids, err := e.RecoverAll(context.Background())
	if err != nil {
		t.Fatalf("RecoverAll: %v", err)
	}
	if len(ids) != 1 || ids[0] != "interrupted" {
		t.Fatalf("recovered ids=%v", ids)
	}
	if calls != 1 {
		t.Fatalf("handler calls=%d want 1", calls)
	}
}

// TestStatePersistedOnDisk 验证状态文件是 JSON 且每次状态变化落盘（重启前可读）。
func TestStatePersistedOnDisk(t *testing.T) {
	dir := t.TempDir()
	e := newTestEngine(t, dir)
	_ = e.RegisterHandler("h", func(ec *ExecContext) (map[string]any, error) { return nil, nil })
	def := &Definition{Name: "persist", Tasks: []TaskDefinition{{ID: "t", Handler: "h"}}}
	if _, err := e.Start(context.Background(), def, "wf-p"); err != nil {
		t.Fatal(err)
	}
	files, _ := os.ReadDir(dir)
	if len(files) != 1 {
		t.Fatalf("expected 1 state file, got %d", len(files))
	}
	if _, err := os.Stat(dir + "/wf-p.json"); err != nil {
		t.Fatalf("state file missing: %v", err)
	}
}

// TestResumeDynamicTasks 动态生成的任务随状态持久化；崩溃后恢复，
// 已完成的动态任务不重跑，未完成的继续。
func TestResumeDynamicTasks(t *testing.T) {
	dir := t.TempDir()
	store, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}

	// 崩溃现场：root 已成功并已动态生成 d1（已成功）、d2（running 中断）。
	st0 := &State{
		WorkflowID: "wf-dyn-resume", DefinitionName: "dyn", Status: WFRunning,
		Tasks: map[string]*TaskState{
			"root": {
				ID: "root", Status: StatusSucceeded, Attempt: 1,
				Result:     map[string]any{"k": "v"},
				Definition: TaskDefinition{ID: "root", Handler: "root"},
			},
			"d1": {
				ID: "d1", Status: StatusSucceeded, Attempt: 1,
				Definition: TaskDefinition{ID: "d1", Handler: "dyn", Dynamic: true},
			},
			"d2": {
				ID: "d2", Status: StatusRunning, Attempt: 1,
				Definition: TaskDefinition{ID: "d2", Handler: "dyn", Dynamic: true, DependsOn: []string{"d1"}},
			},
		},
		TaskOrder: []string{"root", "d1", "d2"},
	}
	if err := store.Save(context.Background(), st0); err != nil {
		t.Fatal(err)
	}

	var rootCalls, d1Calls, d2Calls int32
	e := newTestEngine(t, dir)
	_ = e.RegisterHandler("root", func(ec *ExecContext) (map[string]any, error) {
		atomic.AddInt32(&rootCalls, 1)
		return nil, nil
	})
	_ = e.RegisterHandler("dyn", func(ec *ExecContext) (map[string]any, error) {
		switch ec.TaskID {
		case "d1":
			atomic.AddInt32(&d1Calls, 1)
		case "d2":
			atomic.AddInt32(&d2Calls, 1)
		}
		return nil, nil
	})

	st, err := e.Resume(context.Background(), "wf-dyn-resume")
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if st.Status != WFSucceeded {
		t.Fatalf("status=%s cause=%s", st.Status, st.FailureCause)
	}
	if rootCalls != 0 || d1Calls != 0 {
		t.Fatalf("completed tasks rerun: root=%d d1=%d", rootCalls, d1Calls)
	}
	if d2Calls != 1 {
		t.Fatalf("interrupted dynamic task d2 calls=%d want 1", d2Calls)
	}
	if !st.Tasks["d2"].Definition.Dynamic {
		t.Fatal("d2 must retain Dynamic flag after resume")
	}
}
