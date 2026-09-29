package workflow

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestConcurrencyLimit verifies that at most MaxParallel tasks execute at
// once. Each h-gate handler blocks until released, inside an instrumented
// critical section tracking in-flight count.
func TestConcurrencyLimit(t *testing.T) {
	eng := newTestEngine(t, Config{MaxParallel: 2, TaskTimeout: time.Second})

	const n = 6
	var inFlight, maxSeen int32
	var mu sync.Mutex
	release := make(chan struct{})
	eng.Registry().MustRegister("h-gate", func(ctx context.Context, in TaskInput) (TaskResult, error) {
		cur := atomic.AddInt32(&inFlight, 1)
		for {
			old := atomic.LoadInt32(&maxSeen)
			if cur <= old || atomic.CompareAndSwapInt32(&maxSeen, old, cur) {
				break
			}
		}
		select {
		case <-release:
		case <-ctx.Done():
			atomic.AddInt32(&inFlight, -1)
			return TaskResult{}, ctx.Err()
		}
		atomic.AddInt32(&inFlight, -1)
		return Result(in.TaskID), nil
	})

	var tasks []TaskSpec
	for i := 0; i < n; i++ {
		tasks = append(tasks, task(fmt.Sprintf("t%d", i), "h-gate"))
	}
	spec := WorkflowSpec{Name: "fan", MaxParallel: 2, Tasks: tasks}

	done := make(chan struct{})
	go func() {
		defer close(done)
		time.Sleep(30 * time.Millisecond) // let workers reach the gate
		mu.Lock()
		got := atomic.LoadInt32(&maxSeen)
		mu.Unlock()
		close(release)
		_ = got
	}()

	inst, err := eng.Start(context.Background(), spec, "inst-concurrency")
	<-done
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if inst.Status != InstanceSucceeded {
		t.Fatalf("status=%s", inst.Status)
	}
	if maxSeen > 2 {
		t.Fatalf("max in-flight=%d exceeds limit 2", maxSeen)
	}
	if maxSeen == 0 {
		t.Fatalf("no tasks observed in flight")
	}
	t.Logf("max in-flight tasks = %d (limit 2), all %d tasks succeeded", maxSeen, n)
}

// TestDuplicateTrigger_Concurrent fires many Start calls with the same
// instance id simultaneously. Exactly one may run; the rest must fail with
// ErrInstanceActive, and the task must execute exactly once.
func TestDuplicateTrigger_Concurrent(t *testing.T) {
	eng := newTestEngine(t, Config{MaxParallel: 4, TaskTimeout: time.Second})

	var started int32
	proceed := make(chan struct{})
	var execs atomic.Int32
	eng.Registry().MustRegister("h-slow", func(ctx context.Context, in TaskInput) (TaskResult, error) {
		atomic.AddInt32(&started, 1)
		select {
		case <-proceed:
		case <-ctx.Done():
			return TaskResult{}, ctx.Err()
		}
		execs.Add(1)
		return Result("done"), nil
	})
	spec := specWith(task("only", "h-slow"))

	const callers = 8
	var wg sync.WaitGroup
	var okCount, activeCount, otherCount int32
	var mu sync.Mutex
	barrier := make(chan struct{})
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-barrier
			_, err := eng.Start(context.Background(), spec, "inst-dup")
			switch {
			case err == nil:
				atomic.AddInt32(&okCount, 1)
			case errors.Is(err, ErrInstanceActive):
				atomic.AddInt32(&activeCount, 1)
			default:
				atomic.AddInt32(&otherCount, 1)
				t.Errorf("unexpected error: %v", err)
			}
		}()
	}
	close(barrier)
	// Wait until one caller's run has started the handler, then release.
	deadline := time.Now().Add(2 * time.Second)
	for atomic.LoadInt32(&started) == 0 && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	close(proceed)
	wg.Wait()
	mu.Lock()
	defer mu.Unlock()

	if okCount != 1 {
		t.Fatalf("successful runs=%d want exactly 1 (active=%d other=%d)", okCount, activeCount, otherCount)
	}
	if activeCount != callers-1 {
		t.Fatalf("rejected active=%d want %d", activeCount, callers-1)
	}
	if otherCount != 0 {
		t.Fatalf("unexpected errors=%d", otherCount)
	}
	if execs.Load() != 1 {
		t.Fatalf("handler executed %d times, want 1 (no duplicate results)", execs.Load())
	}
	t.Logf("concurrent triggers=%d -> 1 run, %d rejected ErrInstanceActive, executions=1",
		callers, activeCount)
}

// TestDuplicateTrigger_AfterCompletion ensures a finished instance can never
// run again and returns ErrInstanceTerminal.
func TestDuplicateTrigger_AfterCompletion(t *testing.T) {
	eng, _, _, _ := setupEngine(t, Config{TaskTimeout: time.Second})
	spec := specWith(task("a", hOK))
	if _, err := eng.Start(context.Background(), spec, "inst-once"); err != nil {
		t.Fatalf("first Start: %v", err)
	}
	_, err := eng.Start(context.Background(), spec, "inst-once")
	if !errors.Is(err, ErrInstanceTerminal) {
		t.Fatalf("second Start err=%v want ErrInstanceTerminal", err)
	}
}
