package workflow

import (
	"context"
	"sync/atomic"
	"testing"
)

// TestConditionBranch 条件表达式决定下游是否执行：
// a 失败；b 带 a:succeeded 条件（跳过）；c 带 a:failed 条件（执行）。
func TestConditionBranch(t *testing.T) {
	dir := t.TempDir()
	e := newTestEngine(t, dir)

	_ = e.RegisterHandler("fail", func(ec *ExecContext) (map[string]any, error) {
		return nil, errBoom{}
	})
	var okRan, skippedRan int32
	_ = e.RegisterHandler("onfail", func(ec *ExecContext) (map[string]any, error) {
		atomic.AddInt32(&okRan, 1)
		return map[string]any{"branch": "fail"}, nil
	})
	_ = e.RegisterHandler("onsuccess", func(ec *ExecContext) (map[string]any, error) {
		atomic.AddInt32(&skippedRan, 1)
		return nil, nil
	})

	// 自定义条件：任意任务失败即执行
	_ = e.RegisterCondition("any-failed", func(tasks map[string]*TaskState) bool {
		for _, ts := range tasks {
			if ts.Status == StatusFailed {
				return true
			}
		}
		return false
	})

	def := &Definition{Name: "branch", Tasks: []TaskDefinition{
		{ID: "a", Handler: "fail", FailureBranch: true},
		{ID: "b", Handler: "onsuccess", DependsOn: []string{"a"}, Condition: "a:succeeded"},
		{ID: "c", Handler: "onfail", DependsOn: []string{"a"}, Condition: "a:failed"},
		{ID: "d", Handler: "onfail", DependsOn: []string{"a"}, Condition: "cond:any-failed"},
	}}
	st, err := e.Start(context.Background(), def, "wf-branch")
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if st.Status != WFSucceeded {
		t.Fatalf("status=%s cause=%s (failure-branch a should be handled by conditions)", st.Status, st.FailureCause)
	}
	if st.Tasks["a"].Status != StatusFailed {
		t.Fatalf("a=%s want failed", st.Tasks["a"].Status)
	}
	if st.Tasks["b"].Status != StatusSkipped {
		t.Fatalf("b=%s want skipped", st.Tasks["b"].Status)
	}
	if st.Tasks["c"].Status != StatusSucceeded {
		t.Fatalf("c=%s want succeeded", st.Tasks["c"].Status)
	}
	if st.Tasks["d"].Status != StatusSucceeded {
		t.Fatalf("d=%s want succeeded (custom condition)", st.Tasks["d"].Status)
	}
	if okRan != 2 || skippedRan != 0 {
		t.Fatalf("okRan=%d skippedRan=%d", okRan, skippedRan)
	}
}
