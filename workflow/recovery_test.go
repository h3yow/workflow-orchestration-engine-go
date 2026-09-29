package workflow

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"
)

// TestRecovery_AfterCrash simulates a process crash: task "done" succeeded
// before the crash and "mid" was left running. On Resume, "done" must never
// re-execute and "mid" is re-attempted (retries remain), then the workflow
// completes.
func TestRecovery_AfterCrash(t *testing.T) {
	dir := t.TempDir()
	eng := newTestEngine(t, Config{StateDir: dir, MaxParallel: 2, TaskTimeout: time.Second})

	var doneCalls, midCalls atomic.Int32
	eng.Registry().MustRegister("h-done", func(ctx context.Context, in TaskInput) (TaskResult, error) {
		doneCalls.Add(1)
		return Result("done-v1"), nil
	})
	eng.Registry().MustRegister("h-mid", func(ctx context.Context, in TaskInput) (TaskResult, error) {
		n := midCalls.Add(1)
		// First execution crashes (simulated externally). On recovery it
		// succeeds.
		return Result(fmt.Sprintf("mid-after-%d", n)), nil
	})
	eng.Registry().MustRegister("h-end", func(ctx context.Context, in TaskInput) (TaskResult, error) {
		return Result(in.Outputs["mid"]), nil
	})

	spec := specWith(
		task("done", "h-done"),
		TaskSpec{ID: "mid", Handler: "h-mid", DependsOn: []string{"done"},
			Retry: RetryPolicy{MaxAttempts: 3, Backoff: time.Millisecond}},
		task("end", "h-end", "mid"),
	)

	// Build a state file that looks like a crash: done=succeeded,
	// mid=running with an open attempt, end=pending; instance=running.
	crashed := newInstance(spec, "inst-crash")
	crashed.Status = InstanceRunning
	now := time.Now().UTC()
	crashed.Tasks["done"].Status = TaskSucceeded
	crashed.Tasks["done"].Output = "done-v1"
	crashed.Tasks["done"].UpdatedAt = now
	crashed.Tasks["done"].Attempts = []Attempt{{N: 1, StartedAt: now.Add(-time.Second),
		EndedAt: ptrTime(now.Add(-time.Second)), Outcome: "success"}}

	crashed.Tasks["mid"].Status = TaskRunning
	crashed.Tasks["mid"].StartedAt = &now
	crashed.Tasks["mid"].Attempts = []Attempt{{N: 1, StartedAt: now.Add(-time.Second)}}

	store, err := NewFileStore(dir)
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	if err := store.Create(context.Background(), crashed); err != nil {
		t.Fatalf("seed state: %v", err)
	}
	t.Logf("seeded crashed state at %s", dir)

	// Fresh engine instance = simulated restarted process.
	eng2 := newTestEngine(t, Config{StateDir: dir, MaxParallel: 2, TaskTimeout: time.Second})
	eng2.Registry().MustRegister("h-done", eng.Registry().mustGet(t, "h-done"))
	eng2.Registry().MustRegister("h-mid", eng.Registry().mustGet(t, "h-mid"))
	eng2.Registry().MustRegister("h-end", eng.Registry().mustGet(t, "h-end"))

	inst, err := eng2.Resume(context.Background(), "inst-crash")
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if inst.Status != InstanceSucceeded {
		t.Fatalf("status=%s err=%q", inst.Status, inst.Error)
	}
	if doneCalls.Load() != 0 {
		t.Fatalf("completed task re-executed %d times after restart", doneCalls.Load())
	}
	if midCalls.Load() != 1 {
		t.Fatalf("mid executed %d times after resume, want 1", midCalls.Load())
	}
	// The interrupted attempt is recorded as such, then a new attempt ran.
	attempts := inst.Tasks["mid"].Attempts
	if attempts[0].Outcome != "interrupted" {
		t.Fatalf("first attempt outcome=%q want interrupted", attempts[0].Outcome)
	}
	if attempts[len(attempts)-1].Outcome != "success" {
		t.Fatalf("last attempt outcome=%q want success", attempts[len(attempts)-1].Outcome)
	}
	if inst.Tasks["end"].Output != "mid-after-1" {
		t.Fatalf("end output=%v", inst.Tasks["end"].Output)
	}
	t.Logf("recovered: done not rerun, mid attempts=%v end=%v",
		attemptOutcomes(attempts), inst.Tasks["end"].Output)
}

// TestRecovery_InterruptedTaskRetriesExhausted: a task already on its last
// attempt when the crash happens is failed after recovery; downstream skips.
func TestRecovery_InterruptedTaskRetriesExhausted(t *testing.T) {
	dir := t.TempDir()
	spec := specWith(
		TaskSpec{ID: "only", Handler: "h-x", Retry: RetryPolicy{MaxAttempts: 1}},
		task("down", "h-y", "only"),
	)
	crashed := newInstance(spec, "inst-crash2")
	crashed.Status = InstanceRunning
	now := time.Now().UTC()
	crashed.Tasks["only"].Status = TaskRunning
	crashed.Tasks["only"].StartedAt = &now
	crashed.Tasks["only"].Attempts = []Attempt{{N: 1, StartedAt: now.Add(-time.Second)}}

	store, _ := NewFileStore(dir)
	if err := store.Create(context.Background(), crashed); err != nil {
		t.Fatalf("seed: %v", err)
	}

	eng := newTestEngine(t, Config{StateDir: dir, TaskTimeout: time.Second})
	eng.Registry().MustRegister("h-x", func(ctx context.Context, in TaskInput) (TaskResult, error) {
		return Result("x"), nil
	})
	eng.Registry().MustRegister("h-y", func(ctx context.Context, in TaskInput) (TaskResult, error) {
		return Result("y"), nil
	})
	inst, err := eng.Resume(context.Background(), "inst-crash2")
	if !errors.Is(err, ErrTaskFailed) {
		t.Fatalf("err=%v want ErrTaskFailed", err)
	}
	if inst.Tasks["only"].Status != TaskFailed {
		t.Fatalf("only=%s want failed after exhausted retries post-restart", inst.Tasks["only"].Status)
	}
	if inst.Tasks["down"].Status != TaskSkipped {
		t.Fatalf("down=%s want skipped", inst.Tasks["down"].Status)
	}
	if inst.Tasks["only"].Attempts[0].Outcome != "interrupted" {
		t.Fatalf("attempt outcome=%q", inst.Tasks["only"].Attempts[0].Outcome)
	}
}

// TestRecovery_StateFileSurvivesRoundTrip writes via the store and reloads
// with a separate handle, asserting full traceability (attempts, outputs).
func TestRecovery_StateFileRoundTrip(t *testing.T) {
	dir := t.TempDir()
	s1, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	spec := specWith(task("a", "h-a"))
	inst := newInstance(spec, "inst-rt")
	inst.Status = InstanceFailed
	now := time.Now().UTC()
	inst.Tasks["a"].Status = TaskFailed
	inst.Tasks["a"].Error = "boom"
	inst.Tasks["a"].Attempts = []Attempt{
		{N: 1, StartedAt: now, EndedAt: ptrTime(now.Add(time.Millisecond)), Outcome: "error", Error: "boom"},
	}
	if err := s1.Save(context.Background(), inst); err != nil {
		t.Fatal(err)
	}
	s2, _ := NewFileStore(dir)
	got, err := s2.Load(context.Background(), "inst-rt")
	if err != nil {
		t.Fatal(err)
	}
	if got.Tasks["a"].Attempts[0].Error != "boom" || got.Status != InstanceFailed {
		t.Fatalf("round-trip lost traceability: %+v", got.Tasks["a"])
	}
	if _, err := os.Stat(dir + "/inst-rt.json"); err != nil {
		t.Fatalf("state file missing: %v", err)
	}
}

// TestRecovery_StartOnUnfinishedActsAsResume covers the requirement that a
// repeated Start after a crash does not duplicate completed work.
func TestRecovery_StartOnUnfinishedActsAsResume(t *testing.T) {
	dir := t.TempDir()
	spec := specWith(
		task("a", "h-r"),
		task("b", "h-r", "a"),
	)
	crashed := newInstance(spec, "inst-restart-start")
	crashed.Status = InstanceRunning
	now := time.Now().UTC()
	crashed.Tasks["a"].Status = TaskSucceeded
	crashed.Tasks["a"].Output = "A"
	crashed.Tasks["a"].Attempts = []Attempt{{N: 1, StartedAt: now, EndedAt: ptrTime(now), Outcome: "success"}}

	store, _ := NewFileStore(dir)
	if err := store.Create(context.Background(), crashed); err != nil {
		t.Fatal(err)
	}
	eng := newTestEngine(t, Config{StateDir: dir, TaskTimeout: time.Second})
	var aCalls, bCalls atomic.Int32
	eng.Registry().MustRegister("h-r", func(ctx context.Context, in TaskInput) (TaskResult, error) {
		if in.TaskID == "a" {
			aCalls.Add(1)
		} else {
			bCalls.Add(1)
		}
		return Result(in.TaskID), nil
	})
	inst, err := eng.Start(context.Background(), spec, "inst-restart-start")
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if aCalls.Load() != 0 {
		t.Fatalf("already-succeeded task ran again %d times", aCalls.Load())
	}
	if bCalls.Load() != 1 {
		t.Fatalf("b ran %d times, want 1", bCalls.Load())
	}
	if inst.Status != InstanceSucceeded {
		t.Fatalf("status=%s", inst.Status)
	}
}

func ptrTime(t time.Time) *time.Time { return &t }

func attemptOutcomes(as []Attempt) []string {
	out := make([]string, len(as))
	for i, a := range as {
		out[i] = a.Outcome
	}
	return out
}

// mustGet fetches a registered handler for cross-registry reuse in tests.
func (r *Registry) mustGet(t testing.TB, name string) TaskHandler {
	t.Helper()
	h, err := r.Get(name)
	if err != nil {
		t.Fatal(err)
	}
	return h
}
