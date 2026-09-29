package workflow

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestConcurrencyLimit 验证并发上限：MaxConcurrency=2 时任意时刻在执行的任务不超过 2。
func TestConcurrencyLimit(t *testing.T) {
	dir := t.TempDir()
	e := newTestEngineWith(t, dir, Config{MaxConcurrency: 2})

	var current, maxSeen int32
	_ = e.RegisterHandler("slow", func(ec *ExecContext) (map[string]any, error) {
		cur := atomic.AddInt32(&current, 1)
		for {
			m := atomic.LoadInt32(&maxSeen)
			if cur <= m || atomic.CompareAndSwapInt32(&maxSeen, m, cur) {
				break
			}
		}
		time.Sleep(30 * time.Millisecond)
		atomic.AddInt32(&current, -1)
		return nil, nil
	})

	var tasks []TaskDefinition
	for i := 0; i < 6; i++ {
		tasks = append(tasks, TaskDefinition{ID: "t" + string(rune('a'+i)), Handler: "slow"})
	}
	st, err := e.Start(context.Background(), &Definition{Name: "conc", Tasks: tasks}, "wf-c")
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if st.Status != WFSucceeded {
		t.Fatalf("status=%s", st.Status)
	}
	if maxSeen > 2 {
		t.Fatalf("max concurrent=%d, limit=2", maxSeen)
	}
	if maxSeen < 2 {
		t.Fatalf("expected parallelism to reach 2, got %d", maxSeen)
	}
}

// TestConcurrentDuplicateTrigger 同一实例并发触发：只能有一个在跑，其余被拒绝。
func TestConcurrentDuplicateTrigger(t *testing.T) {
	dir := t.TempDir()
	e := newTestEngine(t, dir)

	release := make(chan struct{})
	var started int32
	_ = e.RegisterHandler("block", func(ec *ExecContext) (map[string]any, error) {
		atomic.AddInt32(&started, 1)
		select {
		case <-release:
		case <-ec.Ctx.Done():
		}
		return nil, nil
	})

	def := &Definition{Name: "dup", Tasks: []TaskDefinition{{ID: "only", Handler: "block"}}}
	var wg sync.WaitGroup
	var ok, rejected int32
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := e.Start(context.Background(), def, "same-id")
			if err == nil {
				atomic.AddInt32(&ok, 1)
				return
			}
			ee, isExec := err.(*ExecError)
			if isExec && ee.Code == ErrCodeInstanceRunning {
				atomic.AddInt32(&rejected, 1)
			} else {
				t.Errorf("unexpected error: %v", err)
			}
		}()
	}
	time.Sleep(50 * time.Millisecond)
	close(release)
	wg.Wait()

	if ok != 1 {
		t.Fatalf("accepted runs=%d want 1", ok)
	}
	if rejected != 7 {
		t.Fatalf("rejected=%d want 7", rejected)
	}
	if started != 1 {
		t.Fatalf("handler invocations=%d want 1 (no duplicate results)", started)
	}
}

// TestRerunTerminalRejected 终态实例再次触发必须被拒绝（不得产生重复结果）。
func TestRerunTerminalRejected(t *testing.T) {
	dir := t.TempDir()
	e := newTestEngine(t, dir)
	var calls int32
	_ = e.RegisterHandler("once", func(ec *ExecContext) (map[string]any, error) {
		atomic.AddInt32(&calls, 1)
		return nil, nil
	})
	def := &Definition{Name: "term", Tasks: []TaskDefinition{{ID: "x", Handler: "once"}}}
	if _, err := e.Start(context.Background(), def, "wf-t"); err != nil {
		t.Fatalf("first start: %v", err)
	}
	_, err := e.Start(context.Background(), def, "wf-t")
	ee, ok := err.(*ExecError)
	if !ok || ee.Code != ErrCodeInstanceTerminal {
		t.Fatalf("want instance_terminal error, got %v", err)
	}
	if calls != 1 {
		t.Fatalf("handler called %d times, want 1", calls)
	}
}
