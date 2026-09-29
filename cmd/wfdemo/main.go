// Command wfdemo runs a deterministic local workflow end-to-end, printing
// key inputs, scheduling decisions and final state. It uses only the local
// filesystem (./.wf-state) and no external services.
//
// Usage:
//
//	go run ./cmd/wfdemo            # normal run (flaky task retries, succeeds)
//	go run ./cmd/wfdemo -rerun     # attempt triggering the same instance again
//	go run ./cmd/wfdemo -resume    # resume an existing unfinished instance
//	go run ./cmd/wfdemo -id DEMO   # custom instance id
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	wf "github.com/h3yow/workflow-orchestration-engine-go/workflow"
)

func main() {
	id := flag.String("id", "DEMO", "instance id")
	rerun := flag.Bool("rerun", false, "trigger the same instance a second time")
	resume := flag.Bool("resume", false, "resume an existing instance")
	flag.Parse()

	dir := "./.wf-state"
	eng, err := wf.NewEngine(wf.Config{StateDir: dir, MaxParallel: 3})
	must(err)

	var flakyAttempts int32
	eng.Registry().MustRegister("prepare", func(ctx context.Context, in wf.TaskInput) (wf.TaskResult, error) {
		fmt.Printf("  [handler] prepare attempt=%d\n", in.Attempt)
		return wf.Result(map[string]any{"ready": true, "n": 3}), nil
	})
	eng.Registry().MustRegister("flaky", func(ctx context.Context, in wf.TaskInput) (wf.TaskResult, error) {
		n := atomic.AddInt32(&flakyAttempts, 1)
		fmt.Printf("  [handler] flaky attempt=%d (fails first 2 times)\n", in.Attempt)
		time.Sleep(10 * time.Millisecond)
		if n <= 2 {
			return wf.TaskResult{}, fmt.Errorf("simulated failure #%d", n)
		}
		return wf.Result("ok-after-retries"), nil
	})
	eng.Registry().MustRegister("branch-deploy", func(ctx context.Context, in wf.TaskInput) (wf.TaskResult, error) {
		fmt.Printf("  [handler] branch-deploy (taken branch)\n")
		return wf.Result("deployed"), nil
	})
	eng.Registry().MustRegister("branch-skip", func(ctx context.Context, in wf.TaskInput) (wf.TaskResult, error) {
		return wf.Result("should-never-run"), nil
	})
	eng.Registry().MustRegister("fanout", func(ctx context.Context, in wf.TaskInput) (wf.TaskResult, error) {
		fmt.Printf("  [handler] fanout: dynamically spawning 3 work tasks\n")
		var reqs []wf.SpawnRequest
		var tasks []wf.TaskSpec
		for i := 0; i < 3; i++ {
			tasks = append(tasks, wf.TaskSpec{
				ID:        fmt.Sprintf("work-%d", i),
				Handler:   "unit",
				DependsOn: []string{"fanout"},
				Params:    map[string]any{"i": i},
			})
		}
		reqs = append(reqs, wf.SpawnRequest{Tasks: tasks})
		return wf.Result(3), &wf.SpawnTasksError{Requests: reqs}
	})
	eng.Registry().MustRegister("unit", func(ctx context.Context, in wf.TaskInput) (wf.TaskResult, error) {
		fmt.Printf("  [handler] unit %v attempt=%d\n", in.Params["i"], in.Attempt)
		return wf.Result(in.Params["i"]), nil
	})

	spec := wf.WorkflowSpec{
		Name:        "demo-pipeline",
		MaxParallel: 3,
		Tasks: []wf.TaskSpec{
			{ID: "prepare", Handler: "prepare"},
			{
				ID:        "flaky",
				Handler:   "flaky",
				DependsOn: []string{"prepare"},
				Retry:     wf.RetryPolicy{MaxAttempts: 3, Backoff: 20 * time.Millisecond, Factor: 2},
			},
			{
				ID:        "branch-deploy",
				Handler:   "branch-deploy",
				DependsOn: []string{"flaky"},
				Condition: &wf.Condition{Op: "eq", Ref: "flaky", Value: "ok-after-retries"},
			},
			{
				ID:        "branch-skip",
				Handler:   "branch-skip",
				DependsOn: []string{"flaky"},
				Condition: &wf.Condition{Op: "eq", Ref: "flaky", Value: "different"},
			},
			{
				ID:        "fanout",
				Handler:   "fanout",
				DependsOn: []string{"branch-deploy"},
				AllowSkip: true,
			},
		},
	}

	ctx := context.Background()

	switch {
	case *resume:
		printRun(eng.Resume(ctx, *id))
	case *rerun:
		// First trigger runs to completion; the duplicate is rejected.
		_, _ = eng.Start(ctx, spec, *id)
		_, err := eng.Start(ctx, spec, *id)
		fmt.Printf("\nrepeat trigger error: %v\n", err)
		if !errors.Is(err, wf.ErrInstanceTerminal) {
			fmt.Println("expected ErrInstanceTerminal")
			os.Exit(1)
		}
		fmt.Println("OK: duplicate terminal trigger rejected")
	default:
		printRun(eng.Start(ctx, spec, *id))
	}
}

func printRun(inst *wf.Instance, err error) {
	must(err)
	fmt.Println("\n=== final instance state ===")
	fmt.Printf("instance=%s status=%s\n", inst.ID, inst.Status)
	for _, id := range inst.TopoOrder {
		ts := inst.Tasks[id]
		fmt.Printf("  %-14s %-9s attempts=%d output=%v %s\n",
			ts.ID, ts.Status, len(ts.Attempts), ts.Output, errSuffix(ts.Error))
	}
}

func errSuffix(s string) string {
	if s == "" {
		return ""
	}
	return "(" + s + ")"
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
