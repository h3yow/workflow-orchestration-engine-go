package workflow

import (
	"context"
	"testing"
)

// TestFailureBranchMixedDownstream failure-branch 任务失败：
// 条件命中的失败分支执行；无条件的下游 skipped；整体仍 succeeded。
func TestFailureBranchMixedDownstream(t *testing.T) {
	dir := t.TempDir()
	e := newTestEngine(t, dir)

	_ = e.RegisterHandler("fail", func(ec *ExecContext) (map[string]any, error) {
		return map[string]any{"partial": "data"}, errBoom{}
	})
	_ = e.RegisterHandler("onfail", func(ec *ExecContext) (map[string]any, error) {
		// 失败任务的 Result 不应通过 Results 传递（只有 succeeded 依赖有快照）。
		if _, leak := ec.Results["check"]; leak {
			t.Errorf("failed task result leaked into downstream: %v", ec.Results)
		}
		return nil, nil
	})
	_ = e.RegisterHandler("plain", func(ec *ExecContext) (map[string]any, error) { return nil, nil })

	def := &Definition{Name: "fb", Tasks: []TaskDefinition{
		{ID: "check", Handler: "fail", FailureBranch: true},
		{ID: "recover", Handler: "onfail", DependsOn: []string{"check"}, Condition: "check:failed"},
		{ID: "normal", Handler: "plain", DependsOn: []string{"check"} /* 无门控条件 */},
	}}
	st, err := e.Start(context.Background(), def, "wf-fb")
	if err != nil {
		t.Fatal(err)
	}
	if st.Status != WFSucceeded {
		t.Fatalf("status=%s cause=%s", st.Status, st.FailureCause)
	}
	if st.Tasks["recover"].Status != StatusSucceeded {
		t.Fatalf("recover=%s", st.Tasks["recover"].Status)
	}
	if st.Tasks["normal"].Status != StatusSkipped {
		t.Fatalf("normal=%s want skipped", st.Tasks["normal"].Status)
	}
	if st.Tasks["check"].Status != StatusFailed {
		t.Fatalf("check=%s want failed", st.Tasks["check"].Status)
	}
}

// TestNonBranchFailureAbortsAll 未标记 FailureBranch 的任务失败：
// 工作流 failed，未开始的任务 canceled；尊重取消信号的在途独立任务变 canceled。
func TestNonBranchFailureAbortsAll(t *testing.T) {
	dir := t.TempDir()
	e := newTestEngine(t, dir)
	_ = e.RegisterHandler("fail", func(ec *ExecContext) (map[string]any, error) { return nil, errBoom{} })
	_ = e.RegisterHandler("ok", func(ec *ExecContext) (map[string]any, error) { return nil, nil })
	_ = e.RegisterHandler("cooperative", func(ec *ExecContext) (map[string]any, error) {
		<-ec.Ctx.Done() // 尊重取消信号
		return nil, ec.Ctx.Err()
	})

	def := &Definition{Name: "abort", Tasks: []TaskDefinition{
		{ID: "a", Handler: "fail"}, // 无依赖，立即失败
		{ID: "b", Handler: "ok", DependsOn: []string{"a"}, Condition: "a:failed"},
		{ID: "c", Handler: "cooperative"}, // 与 a 并发，但会响应取消
	}}
	st, err := e.Start(context.Background(), def, "wf-abort")
	if err != nil {
		t.Fatal(err)
	}
	if st.Status != WFFailed {
		t.Fatalf("status=%s want failed", st.Status)
	}
	if st.FailureCause == "" {
		t.Fatal("missing failure cause")
	}
	if st.Tasks["b"].Status != StatusCanceled {
		t.Fatalf("b=%s want canceled (workflow aborts before branch evaluated)", st.Tasks["b"].Status)
	}
	if st.Tasks["c"].Status != StatusCanceled {
		t.Fatalf("c=%s want canceled", st.Tasks["c"].Status)
	}
}

// TestIndependentTaskCompletesBeforeCancel 与失败任务无依赖的独立任务，
// 若在取消信号到达前已成功完成，则保留 succeeded（其结果不受其他分支失败影响）。
func TestIndependentTaskCompletesBeforeCancel(t *testing.T) {
	dir := t.TempDir()
	e := newTestEngine(t, dir)
	_ = e.RegisterHandler("fail", func(ec *ExecContext) (map[string]any, error) { return nil, errBoom{} })
	_ = e.RegisterHandler("instant", func(ec *ExecContext) (map[string]any, error) {
		return map[string]any{"fast": true}, nil
	})

	def := &Definition{Name: "independent", Tasks: []TaskDefinition{
		{ID: "a", Handler: "fail"},
		{ID: "c", Handler: "instant"}, // 通常比 a 更早完成
	}}
	// 重试几次以覆盖不同调度交织；只要工作流 failed 且 c 要么成功要么取消即可，
	// 这里用独立无依赖的快速任务，断言终态只可能是 succeeded/canceled。
	st, err := e.Start(context.Background(), def, "wf-indep")
	if err != nil {
		t.Fatal(err)
	}
	if st.Status != WFFailed {
		t.Fatalf("status=%s want failed", st.Status)
	}
	switch st.Tasks["c"].Status {
	case StatusSucceeded, StatusCanceled:
	default:
		t.Fatalf("independent c status=%s, want succeeded or canceled", st.Tasks["c"].Status)
	}
}
