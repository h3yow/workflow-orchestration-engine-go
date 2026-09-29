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

const hOK = "h-ok"
const hFail = "h-fail"
const hFlaky = "h-flaky"

type orderedCalls struct {
	mu    sync.Mutex
	calls []string
}

func (o *orderedCalls) add(id string) {
	o.mu.Lock()
	o.calls = append(o.calls, id)
	o.mu.Unlock()
}

func (o *orderedCalls) get() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := append([]string{}, o.calls...)
	return out
}

// setupEngine builds an engine with standard deterministic handlers.
func setupEngine(t *testing.T, cfg Config) (*Engine, *atomic.Int32, *orderedCalls, *atomic.Int32) {
	t.Helper()
	eng := newTestEngine(t, cfg)
	runs := &atomic.Int32{}
	failCalls := &atomic.Int32{}
	calls := &orderedCalls{}

	eng.Registry().MustRegister(hOK, func(ctx context.Context, in TaskInput) (TaskResult, error) {
		runs.Add(1)
		calls.add(in.TaskID)
		v, _ := in.Params["v"]
		return Result(v), nil
	})
	eng.Registry().MustRegister(hFail, func(ctx context.Context, in TaskInput) (TaskResult, error) {
		failCalls.Add(1)
		calls.add(in.TaskID + ":fail")
		return TaskResult{}, fmt.Errorf("deterministic failure in %s", in.TaskID)
	})
	eng.Registry().MustRegister(hFlaky, func(ctx context.Context, in TaskInput) (TaskResult, error) {
		calls.add(fmt.Sprintf("%s#%d", in.TaskID, in.Attempt))
		failN := 2
		if v, ok := in.Params["failN"].(int); ok {
			failN = v
		}
		if in.Attempt <= failN {
			return TaskResult{}, fmt.Errorf("flaky attempt %d/%d", in.Attempt, failN)
		}
		return Result("recovered"), nil
	})
	return eng, runs, calls, failCalls
}

func TestEngine_BasicDAG_Success(t *testing.T) {
	eng, runs, calls, _ := setupEngine(t, Config{MaxParallel: 2, TaskTimeout: time.Second})
	spec := specWith(
		task("a", hOK),
		task("b", hOK, "a"),
		task("c", hOK, "a"),
		task("d", hOK, "b", "c"),
	)
	for i := range spec.Tasks {
		if spec.Tasks[i].ID == "a" {
			spec.Tasks[i].Params = map[string]any{"v": "A-out"}
		}
	}

	inst, err := eng.Start(context.Background(), spec, "inst-basic")
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if inst.Status != InstanceSucceeded {
		t.Fatalf("status=%s want succeeded; err field=%q", inst.Status, inst.Error)
	}
	if runs.Load() != 4 {
		t.Fatalf("handler executions=%d want 4", runs.Load())
	}
	if got := inst.Tasks["a"].Output; got != "A-out" {
		t.Fatalf("a output=%v want A-out", got)
	}
	// d sees outputs of both succeeded dependencies.
	hd := inst.Tasks["d"]
	if hd.Status != TaskSucceeded {
		t.Fatalf("d status=%s", hd.Status)
	}
	seq := calls.get()
	idx := map[string]int{}
	for i, c := range seq {
		if _, ok := idx[c]; !ok {
			idx[c] = i
		}
	}
	if !(idx["a"] < idx["b"] && idx["a"] < idx["c"] && idx["b"] < idx["d"] && idx["c"] < idx["d"]) {
		t.Fatalf("execution order violates DAG: %v", seq)
	}
	t.Logf("execution order: %v; final=%s", seq, inst.Status)
}

func TestEngine_FailurePropagatesAsSkip(t *testing.T) {
	eng, _, calls, _ := setupEngine(t, Config{MaxParallel: 2, TaskTimeout: time.Second})
	spec := specWith(
		task("a", hFail),
		task("b", hOK, "a"), // must skip
		task("c", hOK),      // independent, still succeeds
		task("d", hOK, "b"), // transitive skip
	)
	inst, err := eng.Start(context.Background(), spec, "inst-fail")
	if err == nil || !errors.Is(err, ErrTaskFailed) {
		t.Fatalf("expected ErrTaskFailed, got %v", err)
	}
	if inst.Status != InstanceFailed {
		t.Fatalf("status=%s want failed", inst.Status)
	}
	if inst.Tasks["a"].Status != TaskFailed {
		t.Fatalf("a=%s want failed", inst.Tasks["a"].Status)
	}
	if inst.Tasks["b"].Status != TaskSkipped || inst.Tasks["d"].Status != TaskSkipped {
		t.Fatalf("b=%s d=%s, both want skipped", inst.Tasks["b"].Status, inst.Tasks["d"].Status)
	}
	if inst.Tasks["c"].Status != TaskSucceeded {
		t.Fatalf("c=%s want succeeded", inst.Tasks["c"].Status)
	}
	if len(inst.Tasks["a"].Attempts) != 1 {
		t.Fatalf("a attempts=%d want 1", len(inst.Tasks["a"].Attempts))
	}
	t.Logf("calls=%v statuses a=%s b=%s c=%s d=%s", calls.get(),
		inst.Tasks["a"].Status, inst.Tasks["b"].Status, inst.Tasks["c"].Status, inst.Tasks["d"].Status)
}

func TestEngine_RetryExhausted(t *testing.T) {
	eng, _, _, _ := setupEngine(t, Config{MaxParallel: 1, TaskTimeout: time.Second})
	spec := specWith(
		TaskSpec{
			ID:      "x",
			Handler: hFlaky,
			Params:  map[string]any{"failN": 5},
			Retry:   RetryPolicy{MaxAttempts: 3, Backoff: time.Millisecond, Factor: 2},
		},
	)
	inst, err := eng.Start(context.Background(), spec, "inst-retry-exhausted")
	if err == nil || !errors.Is(err, ErrTaskFailed) {
		t.Fatalf("expected ErrTaskFailed, got %v", err)
	}
	ts := inst.Tasks["x"]
	if ts.Status != TaskFailed {
		t.Fatalf("x=%s want failed", ts.Status)
	}
	if len(ts.Attempts) != 3 {
		t.Fatalf("attempts=%d want 3", len(ts.Attempts))
	}
	for i, a := range ts.Attempts {
		if a.Outcome != "error" {
			t.Fatalf("attempt %d outcome=%q want error", i+1, a.Outcome)
		}
		if a.StartedAt.IsZero() || a.EndedAt == nil {
			t.Fatalf("attempt %d missing timestamps", i+1)
		}
	}
	// Exponential backoff recorded: 1ms, 2ms.
	if ts.Attempts[0].Backoff != time.Millisecond || ts.Attempts[1].Backoff != 2*time.Millisecond {
		t.Fatalf("backoffs=%s,%s want 1ms,2ms", ts.Attempts[0].Backoff, ts.Attempts[1].Backoff)
	}
	t.Logf("x attempts=%d errors=%q final=%s", len(ts.Attempts), ts.Error, inst.Status)
}

func TestEngine_RetryThenSucceed(t *testing.T) {
	eng, _, _, _ := setupEngine(t, Config{TaskTimeout: time.Second})
	spec := specWith(TaskSpec{
		ID:      "x",
		Handler: hFlaky,
		Params:  map[string]any{"failN": 2},
		Retry:   RetryPolicy{MaxAttempts: 4, Backoff: time.Millisecond},
	})
	inst, err := eng.Start(context.Background(), spec, "inst-retry-ok")
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	ts := inst.Tasks["x"]
	if ts.Status != TaskSucceeded {
		t.Fatalf("x=%s want succeeded: %s", ts.Status, ts.Error)
	}
	if len(ts.Attempts) != 3 {
		t.Fatalf("attempts=%d want 3", len(ts.Attempts))
	}
	if ts.Attempts[2].Outcome != "success" {
		t.Fatalf("last attempt outcome=%q want success", ts.Attempts[2].Outcome)
	}
	t.Logf("x recovered after %d attempts, output=%v", len(ts.Attempts), ts.Output)
}

func TestEngine_StatusListAndRegistry(t *testing.T) {
	eng, _, _, _ := setupEngine(t, Config{TaskTimeout: time.Second})
	if _, err := eng.Status(context.Background(), "missing"); !errors.Is(err, ErrInstanceNotFound) {
		t.Fatalf("Status missing err=%v want ErrInstanceNotFound", err)
	}
	spec := specWith(task("a", hOK))
	if _, err := eng.Start(context.Background(), spec, "inst-status"); err != nil {
		t.Fatal(err)
	}
	st, err := eng.Status(context.Background(), "inst-status")
	if err != nil || st.Status != InstanceSucceeded {
		t.Fatalf("Status err=%v status=%s", err, st.Status)
	}
	ids, err := eng.ListInstances(context.Background())
	if err != nil || len(ids) != 1 || ids[0] != "inst-status" {
		t.Fatalf("List=%v err=%v", ids, err)
	}

	// Registry rejection reasons.
	r := NewRegistry()
	if err := r.Register("", func(context.Context, TaskInput) (TaskResult, error) { return TaskResult{}, nil }); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("empty name err=%v", err)
	}
	r.MustRegister("x", func(context.Context, TaskInput) (TaskResult, error) { return TaskResult{}, nil })
	if err := r.Register("x", func(context.Context, TaskInput) (TaskResult, error) { return TaskResult{}, nil }); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("dup register err=%v", err)
	}
	if _, err := r.Get("missing"); !errors.Is(err, ErrUnknownHandler) {
		t.Fatalf("Get missing err=%v", err)
	}
}

func TestCondition_TruthinessAndErrors(t *testing.T) {
	outputs := map[string]any{
		"n":   float64(0),
		"n2":  float64(7),
		"s":   "",
		"s2":  "x",
		"lst": []any{1},
		"b":   true,
	}
	cases := []struct {
		ref string
		op  string
		ok  bool
	}{
		{"n", "truthy", false}, {"n2", "truthy", true},
		{"s", "truthy", false}, {"s2", "truthy", true},
		{"lst", "truthy", true}, {"b", "falsy", false},
	}
	for _, c := range cases {
		got, err := (&Condition{Op: c.op, Ref: c.ref}).Evaluate(outputs)
		if err != nil || got != c.ok {
			t.Fatalf("%s %s = %v,%v want %v", c.op, c.ref, got, err, c.ok)
		}
	}
	if _, err := (&Condition{Op: "bogus", Ref: "b"}).Evaluate(outputs); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("bad op err=%v", err)
	}
	if _, err := (&Condition{Op: "eq", Ref: ".bad"}).Evaluate(outputs); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("bad ref err=%v", err)
	}
}
