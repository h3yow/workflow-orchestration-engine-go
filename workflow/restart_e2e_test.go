package workflow

import (
	"context"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"
)

// This test re-executes the test binary to simulate a real process crash:
//
//	phase=run1: starts a workflow; "step1" succeeds, then "crasher" invokes
//	           os.Exit(42) mid-task (no defers), leaving durable state on disk.
//	phase=run2: a brand-new process resumes the instance and must finish
//	           without re-running step1.
func TestRealProcessRestart_Resume(t *testing.T) {
	if os.Getenv("WF_PHASE") == "run1" {
		e2eRunPhase1(t)
		return
	}
	if os.Getenv("WF_PHASE") == "run2" {
		e2eRunPhase2(t)
		return
	}

	dir := t.TempDir()

	// Phase 1: crash.
	cmd1 := exec.Command(os.Args[0], "-test.run=TestRealProcessRestart_Resume", "-test.v")
	cmd1.Env = append(os.Environ(), "WF_PHASE=run1", "WF_DIR="+dir)
	out1, err := cmd1.CombinedOutput()
	if err == nil {
		t.Fatalf("expected phase-1 process to crash, got success; output:\n%s", out1)
	}
	if exitErr, ok := err.(*exec.ExitError); !ok || exitErr.ExitCode() != 42 {
		t.Fatalf("phase-1 exit=%v want 42; output:\n%s", err, out1)
	}
	if !strings.Contains(string(out1), "step1 -> succeeded") {
		t.Fatalf("phase1 did not complete step1:\n%s", out1)
	}
	t.Logf("phase-1 crashed as expected")

	// Phase 2: fresh process resumes.
	cmd2 := exec.Command(os.Args[0], "-test.run=TestRealProcessRestart_Resume", "-test.v")
	cmd2.Env = append(os.Environ(), "WF_PHASE=run2", "WF_DIR="+dir)
	out2, err := cmd2.CombinedOutput()
	if err != nil {
		t.Fatalf("phase-2 resume failed: %v\noutput:\n%s", err, out2)
	}
	out := string(out2)
	for _, want := range []string{
		"recover task=crasher requeue",
		"crasher -> succeeded",
		"after -> succeeded",
		"status=succeeded",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("phase-2 output missing %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "run task=step1") {
		t.Fatalf("phase-2 re-executed completed step1:\n%s", out)
	}
	t.Logf("phase-2 resumed successfully after real process restart")
}

func e2eRunPhase1(t *testing.T) {
	dir := os.Getenv("WF_DIR")
	eng := newTestEngine(t, Config{StateDir: dir, MaxParallel: 2, TaskTimeout: time.Second})
	eng.Registry().MustRegister("h-step", func(ctx context.Context, in TaskInput) (TaskResult, error) {
		return Result(in.TaskID), nil
	})
	eng.Registry().MustRegister("h-crash", func(ctx context.Context, in TaskInput) (TaskResult, error) {
		// Hard crash: defers do not run, exactly like kill -9 / panic at OS.
		os.Exit(42)
		return TaskResult{}, nil
	})
	spec := specWith(
		task("step1", "h-step"),
		TaskSpec{ID: "crasher", Handler: "h-crash", DependsOn: []string{"step1"},
			Retry: RetryPolicy{MaxAttempts: 3}},
		task("after", "h-step", "crasher"),
	)
	_, _ = eng.Start(context.Background(), spec, "E2E")
	t.Fatal("phase1 should have crashed")
}

func e2eRunPhase2(t *testing.T) {
	dir := os.Getenv("WF_DIR")
	eng := newTestEngine(t, Config{StateDir: dir, MaxParallel: 2, TaskTimeout: time.Second})
	eng.Registry().MustRegister("h-step", func(ctx context.Context, in TaskInput) (TaskResult, error) {
		return Result(in.TaskID), nil
	})
	eng.Registry().MustRegister("h-crash", func(ctx context.Context, in TaskInput) (TaskResult, error) {
		return Result("crasher-recovered-attempt-" + strconv.Itoa(in.Attempt)), nil
	})
	inst, err := eng.Resume(context.Background(), "E2E")
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if inst.Status != InstanceSucceeded {
		t.Fatalf("status=%s", inst.Status)
	}
	// Emit a marker the parent process can search for.
	t.Logf("E2E status=succeeded crasher attempts=%d", len(inst.Tasks["crasher"].Attempts))
}
