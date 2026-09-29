package workflow

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// TestCancel 运行中取消：在途任务变 canceled，未开始任务也变 canceled，不算成功。
func TestCancel(t *testing.T) {
	dir := t.TempDir()
	e := newTestEngine(t, dir)

	entered := make(chan struct{})
	_ = e.RegisterHandler("waitcancel", func(ec *ExecContext) (map[string]any, error) {
		close(entered)
		<-ec.Ctx.Done()
		return nil, ec.Ctx.Err()
	})
	var neverRan int32
	_ = e.RegisterHandler("never", func(ec *ExecContext) (map[string]any, error) {
		atomic.AddInt32(&neverRan, 1)
		return nil, nil
	})

	def := &Definition{Name: "cancel", Tasks: []TaskDefinition{
		{ID: "blocking", Handler: "waitcancel"},
		{ID: "after", Handler: "never", DependsOn: []string{"blocking"}},
	}}
	done := make(chan struct{})
	var st *State
	go func() {
		s, err := e.Start(context.Background(), def, "wf-cancel")
		if err != nil {
			t.Errorf("Start: %v", err)
		}
		st = s
		close(done)
	}()

	<-entered
	if err := e.Cancel("wf-cancel"); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("workflow did not stop after cancel")
	}

	if st.Status != WFCanceled {
		t.Fatalf("status=%s want canceled", st.Status)
	}
	if st.Tasks["blocking"].Status != StatusCanceled {
		t.Fatalf("blocking=%s want canceled", st.Tasks["blocking"].Status)
	}
	if st.Tasks["after"].Status != StatusCanceled {
		t.Fatalf("after=%s want canceled", st.Tasks["after"].Status)
	}
	if neverRan != 0 {
		t.Fatalf("downstream must never run, got %d", neverRan)
	}
}

// TestCancelContext 父 ctx 取消同样触发工作流取消。
func TestCancelContext(t *testing.T) {
	dir := t.TempDir()
	e := newTestEngine(t, dir)
	_ = e.RegisterHandler("wait", func(ec *ExecContext) (map[string]any, error) {
		<-ec.Ctx.Done()
		return nil, ec.Ctx.Err()
	})
	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan struct{})
	go func() {
		_, _ = e.Start(ctx, &Definition{Name: "c2", Tasks: []TaskDefinition{{ID: "t", Handler: "wait"}}}, "wf-c2")
		close(finished)
	}()
	time.Sleep(30 * time.Millisecond)
	cancel()
	select {
	case <-finished:
	case <-time.After(2 * time.Second):
		t.Fatal("parent context cancel did not stop workflow")
	}
}
