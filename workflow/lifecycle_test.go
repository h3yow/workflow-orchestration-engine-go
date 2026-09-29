package workflow

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestTaskTimeout marks a task timed_out (never success); downstream is
// skipped and the instance fails with ErrTaskTimedOut.
func TestTaskTimeout(t *testing.T) {
	eng := newTestEngine(t, Config{TaskTimeout: time.Second})
	eng.Registry().MustRegister("h-sleep", func(ctx context.Context, in TaskInput) (TaskResult, error) {
		select {
		case <-time.After(200 * time.Millisecond):
		case <-ctx.Done():
			// Simulate a handler that returns promptly when its deadline fires.
		}
		return Result("late"), nil
	})
	eng.Registry().MustRegister("h-after", func(ctx context.Context, in TaskInput) (TaskResult, error) {
		return Result("after"), nil
	})
	spec := specWith(
		TaskSpec{ID: "slow", Handler: "h-sleep", Timeout: 30 * time.Millisecond,
			Retry: RetryPolicy{MaxAttempts: 1}},
		task("after", "h-after", "slow"),
	)
	inst, err := eng.Start(context.Background(), spec, "inst-timeout")
	if !errors.Is(err, ErrTaskTimedOut) {
		t.Fatalf("err=%v want ErrTaskTimedOut", err)
	}
	if inst.Tasks["slow"].Status != TaskTimedOut {
		t.Fatalf("slow=%s want timed_out", inst.Tasks["slow"].Status)
	}
	if inst.Tasks["after"].Status != TaskSkipped {
		t.Fatalf("after=%s want skipped", inst.Tasks["after"].Status)
	}
	a := inst.Tasks["slow"].Attempts[0]
	if a.Outcome != "timeout" {
		t.Fatalf("attempt outcome=%q want timeout", a.Outcome)
	}
	t.Logf("slow=%s (%s) after=%s", inst.Tasks["slow"].Status, a.Error, inst.Tasks["after"].Status)
}

// TestCancelRunningWorkflow cancels a blocked run: the running task becomes
// canceled (never success), pending tasks are canceled at finalization, and
// the error wraps ErrTaskCanceled.
func TestCancelRunningWorkflow(t *testing.T) {
	eng := newTestEngine(t, Config{MaxParallel: 1, TaskTimeout: 5 * time.Second})
	entered := make(chan struct{})
	sawCancel := make(chan struct{})
	eng.Registry().MustRegister("h-block", func(ctx context.Context, in TaskInput) (TaskResult, error) {
		close(entered)
		<-ctx.Done()
		close(sawCancel)
		return TaskResult{}, ctx.Err()
	})
	eng.Registry().MustRegister("h-later", func(ctx context.Context, in TaskInput) (TaskResult, error) {
		return Result("never"), nil
	})
	spec := specWith(
		task("blocker", "h-block"),
		task("later", "h-later", "blocker"),
	)
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	instCh := make(chan *Instance, 1)
	go func() {
		inst, err := eng.Start(ctx, spec, "inst-cancel")
		instCh <- inst
		errCh <- err
	}()
	<-entered
	cancel()
	// The handler must observe cancellation before the run is declared done.
	select {
	case <-sawCancel:
	case <-time.After(2 * time.Second):
		t.Fatal("handler never observed context cancellation")
	}
	err := <-errCh
	inst := <-instCh
	if !errors.Is(err, ErrTaskCanceled) {
		t.Fatalf("err=%v want ErrTaskCanceled", err)
	}
	if inst.Status != InstanceCanceled {
		t.Fatalf("instance=%s want canceled", inst.Status)
	}
	if inst.Tasks["blocker"].Status != TaskCanceled {
		t.Fatalf("blocker=%s want canceled", inst.Tasks["blocker"].Status)
	}
	if inst.Tasks["later"].Status != TaskCanceled {
		t.Fatalf("later=%s want canceled", inst.Tasks["later"].Status)
	}
	t.Logf("canceled: blocker=%s later=%s", inst.Tasks["blocker"].Status, inst.Tasks["later"].Status)
}

// TestConditionBranch: only the taken branch runs; the false branch is a
// benign skip and its dependent cascades as skipped; the instance succeeds.
func TestConditionBranch(t *testing.T) {
	eng, _, calls, _ := setupEngine(t, Config{TaskTimeout: time.Second})
	spec := specWith(
		TaskSpec{ID: "a", Handler: hOK, Params: map[string]any{"v": "go"}},
		TaskSpec{ID: "take", Handler: hOK, DependsOn: []string{"a"},
			Condition: &Condition{Op: "eq", Ref: "a", Value: "go"}},
		TaskSpec{ID: "skip", Handler: hOK, DependsOn: []string{"a"},
			Condition: &Condition{Op: "eq", Ref: "a", Value: "nogo"}},
		TaskSpec{ID: "after-skip", Handler: hOK, DependsOn: []string{"skip"}},
	)
	inst, err := eng.Start(context.Background(), spec, "inst-branch")
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if inst.Tasks["take"].Status != TaskSucceeded {
		t.Fatalf("take=%s", inst.Tasks["take"].Status)
	}
	if inst.Tasks["skip"].Status != TaskSkipped {
		t.Fatalf("skip=%s want skipped", inst.Tasks["skip"].Status)
	}
	if inst.Tasks["after-skip"].Status != TaskSkipped {
		t.Fatalf("after-skip=%s want skipped (cascade)", inst.Tasks["after-skip"].Status)
	}
	for _, c := range calls.get() {
		if c == "skip" || c == "after-skip" {
			t.Fatalf("skipped branch executed handler: %v", calls.get())
		}
	}
	t.Logf("calls=%v", calls.get())
}

// TestAllowSkip lets a downstream task still run when a dependency is
// skipped (condition false), receiving only successful outputs.
func TestAllowSkip(t *testing.T) {
	eng := newTestEngine(t, Config{TaskTimeout: time.Second})
	var saw struct {
		sync.Mutex
		outputs map[string]any
	}
	eng.Registry().MustRegister("h-src", func(ctx context.Context, in TaskInput) (TaskResult, error) {
		return Result("src-out"), nil
	})
	eng.Registry().MustRegister("h-gated", func(ctx context.Context, in TaskInput) (TaskResult, error) {
		return Result("gated"), nil
	})
	eng.Registry().MustRegister("h-join", func(ctx context.Context, in TaskInput) (TaskResult, error) {
		saw.Lock()
		saw.outputs = in.Outputs
		saw.Unlock()
		return Result(fmt.Sprintf("%v", in.Outputs["src"])), nil
	})
	spec := specWith(
		task("src", "h-src"),
		TaskSpec{ID: "gated", Handler: "h-gated", DependsOn: []string{"src"},
			Condition: &Condition{Op: "eq", Ref: "src", Value: "never"}},
		TaskSpec{ID: "join", Handler: "h-join", DependsOn: []string{"src", "gated"}, AllowSkip: true},
	)
	inst, err := eng.Start(context.Background(), spec, "inst-allowskip")
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if inst.Tasks["gated"].Status != TaskSkipped {
		t.Fatalf("gated=%s", inst.Tasks["gated"].Status)
	}
	if inst.Tasks["join"].Status != TaskSucceeded {
		t.Fatalf("join=%s want succeeded with AllowSkip: %s", inst.Tasks["join"].Status, inst.Tasks["join"].Error)
	}
	saw.Lock()
	defer saw.Unlock()
	if _, present := saw.outputs["gated"]; present {
		t.Fatalf("join saw output of skipped gated task: %v", saw.outputs)
	}
	if saw.outputs["src"] != "src-out" {
		t.Fatalf("join did not receive src output: %v", saw.outputs)
	}
	t.Logf("join output=%v", saw.outputs)
}
