package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestStateFileContentAndTraceability 验证落盘 JSON 可解析、无 tmp 残留，
// 且重试记录 / 失败原因 / 时间戳 / 任务顺序均可追溯。
func TestStateFileContentAndTraceability(t *testing.T) {
	dir := t.TempDir()
	e := newTestEngine(t, dir)

	var n int
	_ = e.RegisterHandler("flaky", func(ec *ExecContext) (map[string]any, error) {
		n++
		return nil, errors.New("disk full")
	})
	def := &Definition{Name: "trace", Tasks: []TaskDefinition{
		{ID: "x", Handler: "flaky",
			Retry: &RetryPolicy{MaxAttempts: 2, InitialBackoff: time.Millisecond}},
	}}
	st, err := e.Start(context.Background(), def, "trace-1")
	if err != nil {
		t.Fatal(err)
	}
	if st.Status != WFFailed {
		t.Fatalf("status=%s", st.Status)
	}

	path := filepath.Join(dir, "trace-1.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("state file: %v", err)
	}
	var parsed State
	if err := json.Unmarshal(raw, &parsed); err != nil {
		t.Fatalf("state file is not valid JSON: %v", err)
	}
	if parsed.FailureCause == "" || !strings.Contains(parsed.FailureCause, "disk full") {
		t.Fatalf("failure_cause not traceable: %q", parsed.FailureCause)
	}
	x := parsed.Tasks["x"]
	if x == nil || x.Status != StatusFailed || len(x.Records) != 2 {
		t.Fatalf("task state not traceable: %+v", x)
	}
	if !x.Records[1].EndedAt.After(x.Records[0].StartedAt) {
		t.Fatal("record timestamps not ordered")
	}
	if len(parsed.TaskOrder) != 1 || parsed.TaskOrder[0] != "x" {
		t.Fatalf("task_order=%v", parsed.TaskOrder)
	}
	// 不得残留临时文件。
	if entries, _ := os.ReadDir(dir); len(entries) != 1 {
		names := []string{}
		for _, en := range entries {
			names = append(names, en.Name())
		}
		t.Fatalf("dir should contain exactly 1 file, got %v", names)
	}
}

// TestFileStoreAtomicRepeatedSave 多次 Save 后文件始终合法。
func TestFileStoreAtomicRepeatedSave(t *testing.T) {
	dir := t.TempDir()
	st, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	state := &State{WorkflowID: "w1", Status: WFRunning, Tasks: map[string]*TaskState{}, TaskOrder: nil}
	for i := 0; i < 20; i++ {
		state.FailureCause = ""
		if err := st.Save(context.Background(), state); err != nil {
			t.Fatal(err)
		}
		got, err := st.Load(context.Background(), "w1")
		if err != nil || got == nil || got.WorkflowID != "w1" {
			t.Fatalf("load after save %d: %v %+v", i, err, got)
		}
	}
}

// TestCorruptStateRejected 损坏的状态文件返回 state_corrupt 错误。
func TestCorruptStateRejected(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "bad.json"), []byte("{not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	st, _ := NewFileStore(dir)
	_, err := st.Load(context.Background(), "bad")
	ee, ok := err.(*ExecError)
	if !ok || ee.Code != ErrCodeStateCorrupt {
		t.Fatalf("want state_corrupt, got %v", err)
	}
}
