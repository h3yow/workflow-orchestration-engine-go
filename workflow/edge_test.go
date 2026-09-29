package workflow

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// TestCancelDuringBackoff 任务首次失败进入退避，此时取消工作流：
// 不进行后续尝试，任务 canceled，工作流 canceled。
func TestCancelDuringBackoff(t *testing.T) {
	dir := t.TempDir()
	e := newTestEngine(t, dir)

	_ = e.RegisterHandler("alwaysfail", func(ec *ExecContext) (map[string]any, error) {
		return nil, errBoom{}
	})
	def := &Definition{Name: "backoffcancel", Tasks: []TaskDefinition{
		{ID: "t", Handler: "alwaysfail",
			Retry: &RetryPolicy{MaxAttempts: 100, InitialBackoff: 10 * time.Second}},
	}}
	done := make(chan struct{})
	var st *State
	go func() {
		s, err := e.Start(context.Background(), def, "wf-bc")
		if err != nil {
			t.Errorf("Start: %v", err)
		}
		st = s
		close(done)
	}()
	// 等待第一次尝试失败并进入长退避。
	time.Sleep(50 * time.Millisecond)
	if err := e.Cancel("wf-bc"); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("workflow stuck in backoff after cancel")
	}
	if st.Status != WFCanceled {
		t.Fatalf("status=%s want canceled", st.Status)
	}
	if st.Tasks["t"].Status != StatusCanceled {
		t.Fatalf("t=%s want canceled (cancelled during backoff)", st.Tasks["t"].Status)
	}
	if st.Tasks["t"].Attempt != 1 {
		t.Fatalf("attempts=%d want 1 (no retry after cancel)", st.Tasks["t"].Attempt)
	}
}

// TestCancelNonRunning Cancel 一个未运行的实例返回可区分错误。
func TestCancelNonRunning(t *testing.T) {
	dir := t.TempDir()
	e := newTestEngine(t, dir)
	err := e.Cancel("does-not-exist")
	ee, ok := err.(*ExecError)
	if !ok || ee.Code != ErrCodeCanceled {
		t.Fatalf("want canceled error, got %v", err)
	}
}

// TestResumeRejectsConcurrentRun Resume 与正在进行的 Start 并发时被拒绝。
func TestResumeRejectsConcurrentRun(t *testing.T) {
	dir := t.TempDir()
	e := newTestEngine(t, dir)
	release := make(chan struct{})
	_ = e.RegisterHandler("block", func(ec *ExecContext) (map[string]any, error) {
		<-release
		return nil, nil
	})
	def := &Definition{Name: "lock", Tasks: []TaskDefinition{{ID: "t", Handler: "block"}}}
	started := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		close(started)
		_, _ = e.Start(context.Background(), def, "wf-lock")
	}()
	<-started
	time.Sleep(30 * time.Millisecond)
	_, err := e.Resume(context.Background(), "wf-lock")
	ee, ok := err.(*ExecError)
	if !ok || ee.Code != ErrCodeInstanceRunning {
		t.Fatalf("want instance_already_running, got %v", err)
	}
	close(release)
	<-finished // 等待后台执行完全结束，避免日志 goroutine 越过测试 cleanup
}

// TestResultsSnapshotIsolation 处理器拿到的 Results 是快照，修改不影响引擎状态。
func TestResultsSnapshotIsolation(t *testing.T) {
	dir := t.TempDir()
	e := newTestEngine(t, dir)
	var mutated int32
	_ = e.RegisterHandler("producer", func(ec *ExecContext) (map[string]any, error) {
		return map[string]any{"k": "v"}, nil
	})
	_ = e.RegisterHandler("mutator", func(ec *ExecContext) (map[string]any, error) {
		if r := ec.Results["producer"]; r != nil {
			r["k"] = "tampered"
			atomic.StoreInt32(&mutated, 1)
		}
		return nil, nil
	})
	_ = e.RegisterHandler("reader", func(ec *ExecContext) (map[string]any, error) {
		if ec.Results["producer"]["k"] != "v" {
			t.Errorf("result snapshot leaked mutation: %v", ec.Results)
		}
		return nil, nil
	})
	def := &Definition{Name: "snapshot", Tasks: []TaskDefinition{
		{ID: "producer", Handler: "producer"},
		{ID: "m1", Handler: "mutator", DependsOn: []string{"producer"}},
		{ID: "m2", Handler: "mutator", DependsOn: []string{"producer"}},
		{ID: "reader", Handler: "reader", DependsOn: []string{"m1", "m2", "producer"}},
	}}
	// reader 直接依赖 producer，同时 m1/m2 也依赖 producer；reader 的 producer 结果
	// 必须是原始值（m1/m2 拿到的是各自独立快照）。
	st, err := e.Start(context.Background(), def, "wf-snap")
	if err != nil {
		t.Fatal(err)
	}
	if st.Status != WFSucceeded {
		t.Fatalf("status=%s", st.Status)
	}
	if mutated != 1 {
		t.Fatal("mutator did not run")
	}
}
