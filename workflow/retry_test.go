package workflow

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// TestRetryExhausted 任务按策略重试，耗尽后工作流失败，RetryRecord 可追溯。
func TestRetryExhausted(t *testing.T) {
	dir := t.TempDir()
	e := newTestEngine(t, dir)

	var attempts int32
	_ = e.RegisterHandler("flaky", func(ec *ExecContext) (map[string]any, error) {
		n := atomic.AddInt32(&attempts, 1)
		return nil, errBoom{at: n}
	})
	var downstream int32
	_ = e.RegisterHandler("down", func(ec *ExecContext) (map[string]any, error) {
		atomic.AddInt32(&downstream, 1)
		return nil, nil
	})

	def := &Definition{
		Name: "retry",
		Tasks: []TaskDefinition{
			{
				ID: "bad", Handler: "flaky",
				Retry: &RetryPolicy{MaxAttempts: 3, InitialBackoff: time.Millisecond, MaxBackoff: 5 * time.Millisecond, Multiplier: 2},
			},
			{ID: "after", Handler: "down", DependsOn: []string{"bad"}},
		},
	}
	st, err := e.Start(context.Background(), def, "wf-r")
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if st.Status != WFFailed {
		t.Fatalf("status=%s want failed", st.Status)
	}
	if attempts != 3 {
		t.Fatalf("attempts=%d want 3", attempts)
	}
	if downstream != 0 {
		t.Fatalf("downstream ran %d times; must never run after upstream failure", downstream)
	}
	bad := st.Tasks["bad"]
	if bad.Status != StatusFailed {
		t.Fatalf("bad status=%s want failed", bad.Status)
	}
	if len(bad.Records) != 3 {
		t.Fatalf("retry records=%d want 3", len(bad.Records))
	}
	for i, rec := range bad.Records {
		if rec.Attempt != i+1 || rec.Err == "" {
			t.Fatalf("record %d malformed: %+v", i, rec)
		}
		if !rec.EndedAt.After(rec.StartedAt) && !rec.EndedAt.Equal(rec.StartedAt) {
			t.Fatalf("record %d time order wrong", i)
		}
	}
	if st.Tasks["after"].Status != StatusCanceled {
		t.Fatalf("downstream status=%s want canceled", st.Tasks["after"].Status)
	}
	if st.FailureCause == "" {
		t.Fatal("failure cause must be recorded")
	}
}

// TestRetrySucceedsEventually 前两次失败、第三次成功：结果成功且仅执行一次下游。
func TestRetrySucceedsEventually(t *testing.T) {
	dir := t.TempDir()
	e := newTestEngine(t, dir)

	var attempts int32
	_ = e.RegisterHandler("flaky", func(ec *ExecContext) (map[string]any, error) {
		if atomic.AddInt32(&attempts, 1) < 3 {
			return nil, errBoom{}
		}
		return map[string]any{"ok": true}, nil
	})
	var downs int32
	_ = e.RegisterHandler("down", func(ec *ExecContext) (map[string]any, error) {
		atomic.AddInt32(&downs, 1)
		return nil, nil
	})
	def := &Definition{Name: "retryok", Tasks: []TaskDefinition{
		{ID: "u", Handler: "flaky", Retry: &RetryPolicy{MaxAttempts: 5, InitialBackoff: time.Millisecond}},
		{ID: "d", Handler: "down", DependsOn: []string{"u"}},
	}}
	st, err := e.Start(context.Background(), def, "wf-r2")
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if st.Status != WFSucceeded {
		t.Fatalf("status=%s cause=%s", st.Status, st.FailureCause)
	}
	if attempts != 3 || downs != 1 {
		t.Fatalf("attempts=%d downs=%d", attempts, downs)
	}
	if st.Tasks["u"].Attempt != 3 || len(st.Tasks["u"].Records) != 3 {
		t.Fatalf("u attempt=%d records=%d", st.Tasks["u"].Attempt, len(st.Tasks["u"].Records))
	}
}

// TestTimeout 任务超时不算成功，按失败处理；依赖它的下游被取消。
func TestTimeout(t *testing.T) {
	dir := t.TempDir()
	e := newTestEngine(t, dir)

	_ = e.RegisterHandler("hang", func(ec *ExecContext) (map[string]any, error) {
		select {
		case <-time.After(5 * time.Second):
			return nil, nil
		case <-ec.Ctx.Done():
			return nil, ec.Ctx.Err()
		}
	})
	var downstreamRan int32
	_ = e.RegisterHandler("down", func(ec *ExecContext) (map[string]any, error) {
		atomic.AddInt32(&downstreamRan, 1)
		return nil, nil
	})
	def := &Definition{Name: "timeout", Tasks: []TaskDefinition{
		{ID: "h", Handler: "hang", Timeout: 40 * time.Millisecond},
		{ID: "d", Handler: "down", DependsOn: []string{"h"}},
	}}
	st, err := e.Start(context.Background(), def, "wf-timeout")
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if st.Status != WFFailed {
		t.Fatalf("status=%s", st.Status)
	}
	if st.Tasks["h"].Status != StatusFailed {
		t.Fatalf("h status=%s want failed (timeout must not be success)", st.Tasks["h"].Status)
	}
	if st.Tasks["d"].Status != StatusCanceled {
		t.Fatalf("d status=%s want canceled", st.Tasks["d"].Status)
	}
}

type errBoom struct {
	at int32
}

func (e errBoom) Error() string { return "boom" }
