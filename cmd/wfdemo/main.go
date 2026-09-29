// wfdemo 是工作流引擎的本地确定性演示程序，不依赖任何外部服务。
//
//	go run ./cmd/wfdemo diamond    # 依赖调度 + 条件分支 + 动态生成
//	go run ./cmd/wfdemo retry      # 失败重试后成功
//	go run ./cmd/wfdemo fail       # 重试耗尽 -> 工作流失败
//	go run ./cmd/wfdemo resume <state-dir> <workflow-id>
//
// 状态文件默认写入 ./.wfstate。
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	wf "github.com/h3yow/workflow-orchestration-engine-go/workflow"
)

func main() {
	mode := "diamond"
	if len(os.Args) > 1 {
		mode = os.Args[1]
	}
	stateDir := "./.wfstate"
	instanceID := "demo-" + mode

	ctx := context.Background()
	switch mode {
	case "resume":
		if len(os.Args) < 4 {
			fmt.Println("usage: wfdemo resume <state-dir> <workflow-id>")
			os.Exit(2)
		}
		stateDir, instanceID = os.Args[2], os.Args[3]
		eng := mustEngine(stateDir)
		register(eng)
		st, err := eng.Resume(ctx, instanceID)
		fail(err)
		printState(st)
		return
	case "recover":
		if len(os.Args) > 2 {
			stateDir = os.Args[2]
		}
		eng := mustEngine(stateDir)
		register(eng)
		ids, err := eng.RecoverAll(ctx)
		fail(err)
		fmt.Printf("recovered: %v\n", ids)
		return
	}

	eng := mustEngine(stateDir)
	register(eng)
	def := build(mode)
	st, err := eng.Start(ctx, def, instanceID)
	fail(err)
	printState(st)
}

func mustEngine(dir string) *wf.Engine {
	eng, err := wf.NewEngine(wf.Config{
		StateDir:       dir,
		MaxConcurrency: 4,
		DefaultTimeout: 2 * time.Second,
	})
	fail(err)
	return eng
}

func register(eng *wf.Engine) {
	must(eng.RegisterHandler("echo", func(ec *wf.ExecContext) (map[string]any, error) {
		fmt.Printf("  -> run task %s params=%v\n", ec.TaskID, ec.Params)
		return map[string]any{"echo": ec.Params["msg"]}, nil
	}))
	var flaky int32
	must(eng.RegisterHandler("flaky", func(ec *wf.ExecContext) (map[string]any, error) {
		n := atomic.AddInt32(&flaky, 1)
		fmt.Printf("  -> flaky attempt #%d\n", n)
		if n < 3 {
			return nil, errors.New("transient failure")
		}
		return map[string]any{"ok": true}, nil
	}))
	must(eng.RegisterHandler("alwaysfail", func(ec *wf.ExecContext) (map[string]any, error) {
		fmt.Printf("  -> alwaysfail %s\n", ec.TaskID)
		return nil, errors.New("permanent failure")
	}))
	must(eng.RegisterHandler("spawner", func(ec *wf.ExecContext) (map[string]any, error) {
		fmt.Printf("  -> spawner dynamically creates dyn-1, dyn-2\n")
		return nil, ec.Spawn(
			wf.TaskDefinition{ID: "dyn-1", Handler: "echo", Params: map[string]any{"msg": "dyn-1"}},
			wf.TaskDefinition{ID: "dyn-2", Handler: "echo", DependsOn: []string{"dyn-1"}, Params: map[string]any{"msg": "dyn-2"}},
		)
	}))
	must(eng.RegisterHandler("cleanup", func(ec *wf.ExecContext) (map[string]any, error) {
		fmt.Printf("  -> cleanup runs because checker failed (failure branch)\n")
		return nil, nil
	}))
}

func build(mode string) *wf.Definition {
	retryOK := &wf.RetryPolicy{MaxAttempts: 5, InitialBackoff: 50 * time.Millisecond, MaxBackoff: time.Second, Multiplier: 2}
	retryBad := &wf.RetryPolicy{MaxAttempts: 2, InitialBackoff: 20 * time.Millisecond}

	switch mode {
	case "retry":
		return &wf.Definition{Name: "retry-demo", Tasks: []wf.TaskDefinition{
			{ID: "flaky", Handler: "flaky", Retry: retryOK},
			{ID: "after", Handler: "echo", DependsOn: []string{"flaky"}, Params: map[string]any{"msg": "after flaky"}},
		}}
	case "fail":
		return &wf.Definition{Name: "fail-demo", Tasks: []wf.TaskDefinition{
			{ID: "bad", Handler: "alwaysfail", Retry: retryBad, FailureBranch: true},
			{ID: "cleanup", Handler: "cleanup", DependsOn: []string{"bad"}, Condition: "bad:failed"},
			{ID: "happy", Handler: "echo", DependsOn: []string{"bad"}, Condition: "bad:succeeded", Params: map[string]any{"msg": "never"}},
		}}
	default:
		// diamond：根任务 -> 两个并发分支 -> 汇聚；汇聚后动态生成两个任务。
		return &wf.Definition{Name: "diamond-demo", DefaultRetry: wf.RetryPolicy{MaxAttempts: 1}, Tasks: []wf.TaskDefinition{
			{ID: "start", Handler: "echo", Params: map[string]any{"msg": "start"}},
			{ID: "left", Handler: "echo", DependsOn: []string{"start"}, Params: map[string]any{"msg": "left"}},
			{ID: "right", Handler: "echo", DependsOn: []string{"start"}, Params: map[string]any{"msg": "right"}},
			{ID: "join", Handler: "spawner", DependsOn: []string{"left", "right"}},
		}}
	}
}

func printState(st *wf.State) {
	fmt.Printf("\nworkflow %s => %s\n", st.WorkflowID, st.Status)
	if st.FailureCause != "" {
		fmt.Printf("failure_cause: %s\n", st.FailureCause)
	}
	for _, id := range st.TaskOrder {
		ts := st.Tasks[id]
		line := fmt.Sprintf("  - %-8s %-9s attempts=%d", id, ts.Status, ts.Attempt)
		if ts.Err != "" {
			line += " err=" + ts.Err
		}
		fmt.Println(line)
	}
}

func must(err error) {
	if err != nil {
		fail(err)
	}
}

func fail(err error) {
	if err != nil {
		var de *wf.DefinitionError
		if errors.As(err, &de) {
			fmt.Fprintf(os.Stderr, "definition rejected [%s]: %s\n", de.Code, de.Message)
		} else {
			fmt.Fprintln(os.Stderr, "error:", err)
		}
		os.Exit(1)
	}
}
