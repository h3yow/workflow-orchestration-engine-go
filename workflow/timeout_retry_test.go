package workflow

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// TestTimeoutRetryThenSuccess 首次调用超过任务超时，重试后快速成功。
func TestTimeoutRetryThenSuccess(t *testing.T) {
	dir := t.TempDir()
	e := newTestEngine(t, dir)

	var n int32
	_ = e.RegisterHandler("slowfirst", func(ec *ExecContext) (map[string]any, error) {
		if atomic.AddInt32(&n, 1) == 1 {
			select {
			case <-time.After(300 * time.Millisecond):
			case <-ec.Ctx.Done():
				return nil, ec.Ctx.Err()
			}
		}
		return map[string]any{"ok": true}, nil
	})
	def := &Definition{Name: "tr", Tasks: []TaskDefinition{
		{ID: "t", Handler: "slowfirst", Timeout: 50 * time.Millisecond,
			Retry: &RetryPolicy{MaxAttempts: 3, InitialBackoff: time.Millisecond}},
	}}
	st, err := e.Start(context.Background(), def, "wf-tr")
	if err != nil {
		t.Fatal(err)
	}
	if st.Status != WFSucceeded {
		t.Fatalf("status=%s cause=%s", st.Status, st.FailureCause)
	}
	if n != 2 {
		t.Fatalf("attempts=%d want 2", n)
	}
	// 第一条记录应是超时错误。
	if st.Tasks["t"].Records[0].Err != ErrTimeout.Error() {
		t.Fatalf("first record err=%q want timeout", st.Tasks["t"].Records[0].Err)
	}
}

func TestComputeBackoff(t *testing.T) {
	r := &RetryPolicy{InitialBackoff: 10 * time.Millisecond, MaxBackoff: 100 * time.Millisecond, Multiplier: 2}
	cases := []struct {
		failedAttempt int
		want          time.Duration
	}{
		{1, 10 * time.Millisecond},
		{2, 20 * time.Millisecond},
		{3, 40 * time.Millisecond},
		{4, 80 * time.Millisecond},
		{5, 100 * time.Millisecond}, // capped
		{9, 100 * time.Millisecond},
	}
	for _, c := range cases {
		if got := computeBackoff(r, c.failedAttempt); got != c.want {
			t.Errorf("failedAttempt=%d got=%v want=%v", c.failedAttempt, got, c.want)
		}
	}
	if got := computeBackoff(&RetryPolicy{InitialBackoff: 0}, 3); got != 0 {
		t.Errorf("no backoff policy got=%v want 0", got)
	}
}
