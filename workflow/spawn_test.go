package workflow

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// TestDynamicSpawn verifies tasks generated at runtime are validated,
// persisted, scheduled by dependency and visible in final topo order.
func TestDynamicSpawn(t *testing.T) {
	eng := newTestEngine(t, Config{MaxParallel: 4, TaskTimeout: time.Second})
	eng.Registry().MustRegister("h-plan", func(ctx context.Context, in TaskInput) (TaskResult, error) {
		var tasks []TaskSpec
		for i := 0; i < 3; i++ {
			tasks = append(tasks, TaskSpec{
				ID:        fmt.Sprintf("gen-%d", i),
				Handler:   "h-unit",
				DependsOn: []string{"plan"},
				Params:    map[string]any{"i": i},
			})
		}
		tasks = append(tasks, TaskSpec{
			ID:        "gen-join",
			Handler:   "h-unit",
			DependsOn: []string{"gen-0", "gen-1", "gen-2"},
			Params:    map[string]any{"i": 99},
		})
		return TaskResult{}, &SpawnTasksError{Requests: []SpawnRequest{{Tasks: tasks}}}
	})
	var execs atomic.Int32
	eng.Registry().MustRegister("h-unit", func(ctx context.Context, in TaskInput) (TaskResult, error) {
		execs.Add(1)
		return Result(in.Params["i"]), nil
	})
	spec := specWith(task("plan", "h-plan"))
	inst, err := eng.Start(context.Background(), spec, "inst-spawn")
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if execs.Load() != 4 {
		t.Fatalf("dynamic tasks executed=%d want 4", execs.Load())
	}
	for _, id := range []string{"gen-0", "gen-1", "gen-2", "gen-join"} {
		if inst.Tasks[id] == nil || inst.Tasks[id].Status != TaskSucceeded {
			t.Fatalf("dynamic task %s state=%v", id, inst.Tasks[id])
		}
	}
	if inst.Tasks["gen-join"].Output.(float64) != 99 {
		t.Fatalf("join output=%v", inst.Tasks["gen-join"].Output)
	}
	// Topo order: plan precedes gen-*, gen-* precede gen-join.
	pos := map[string]int{}
	for i, id := range inst.TopoOrder {
		pos[id] = i
	}
	if !(pos["plan"] < pos["gen-0"] && pos["gen-0"] < pos["gen-join"] && pos["gen-2"] < pos["gen-join"]) {
		t.Fatalf("topo order invalid after spawn: %v", inst.TopoOrder)
	}
	// Spawning task retains its own successful output semantics.
	if inst.Tasks["plan"].Status != TaskSucceeded {
		t.Fatalf("plan=%s", inst.Tasks["plan"].Status)
	}
	t.Logf("topo after spawn: %v", inst.TopoOrder)
}

// TestDynamicSpawn_RejectedInvalid makes an atomic rejection: a spawn
// referencing an unknown task fails the spawning task and adds nothing.
func TestDynamicSpawn_RejectedInvalid(t *testing.T) {
	eng := newTestEngine(t, Config{TaskTimeout: time.Second})
	eng.Registry().MustRegister("h-badspawn", func(ctx context.Context, in TaskInput) (TaskResult, error) {
		return TaskResult{}, &SpawnTasksError{Requests: []SpawnRequest{{Tasks: []TaskSpec{
			{ID: "bad", Handler: "h-unit", DependsOn: []string{"ghost"}},
		}}}}
	})
	spec := specWith(TaskSpec{ID: "sp", Handler: "h-badspawn",
		Retry: RetryPolicy{MaxAttempts: 1}})
	inst, err := eng.Start(context.Background(), spec, "inst-badspawn")
	if !errors.Is(err, ErrTaskFailed) {
		t.Fatalf("err=%v want ErrTaskFailed", err)
	}
	if _, exists := inst.Tasks["bad"]; exists {
		t.Fatal("rejected dynamic task must not be added to state")
	}
	if inst.Status != InstanceFailed {
		t.Fatalf("status=%s", inst.Status)
	}
	a := inst.Tasks["sp"].Attempts[0]
	if a.Outcome != "error" {
		t.Fatalf("attempt outcome=%q want error", a.Outcome)
	}
	t.Logf("spawn rejected atomically: %s", a.Error)
}

// TestDynamicSpawn_DuplicateID rejects a spawn whose id collides.
func TestDynamicSpawn_DuplicateID(t *testing.T) {
	eng := newTestEngine(t, Config{TaskTimeout: time.Second})
	eng.Registry().MustRegister("h-dup", func(ctx context.Context, in TaskInput) (TaskResult, error) {
		return TaskResult{}, &SpawnTasksError{Requests: []SpawnRequest{{Tasks: []TaskSpec{
			{ID: "sp", Handler: "h-whatever"},
		}}}}
	})
	spec := specWith(TaskSpec{ID: "sp", Handler: "h-dup", Retry: RetryPolicy{MaxAttempts: 1}})
	inst, err := eng.Start(context.Background(), spec, "inst-dupspawn")
	if !errors.Is(err, ErrTaskFailed) {
		t.Fatalf("err=%v", err)
	}
	if len(inst.TopoOrder) != 1 {
		t.Fatalf("topo=%v, no task may be added", inst.TopoOrder)
	}
}
