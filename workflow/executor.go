package workflow

import (
	"context"
	"errors"
	"time"
)

// ErrCanceled / ErrTimeout 供处理器区分取消与失败（同时也通过 ctx 感知）。
var (
	ErrCanceled = errors.New("task canceled")
	ErrTimeout  = errors.New("task timed out")
)

// execute 在独立 goroutine 中运行单个任务，封装：ready→running 持久化、
// 每任务超时、失败指数退避重试、取消处理、动态任务收集。
// 全程只操作 in.ts 的私有副本，通过 event 把最终状态交回调度循环。
func (e *Engine) execute(wfCtx context.Context, in execInput, events chan<- event) {
	ts := in.ts
	var spawned []TaskDefinition

	finish := func() {
		// 终态事件必须送达（events 有缓冲，且调度循环始终排空终态）。
		events <- event{kind: evFinished, taskID: ts.ID, ts: ts, spawned: spawned}
	}

	// emitProgress 发送 running 心跳并等待"状态已落盘"确认，之后才允许调用处理器。
	// 若等待期间工作流被取消，则返回 false（不再执行该次尝试）。
	emitProgress := func() bool {
		ack := make(chan struct{})
		select {
		case events <- event{kind: evProgress, taskID: ts.ID, ts: cloneTaskState(ts), ack: ack}:
		case <-wfCtx.Done():
			return false
		}
		select {
		case <-ack:
			return true
		case <-wfCtx.Done():
			return false
		}
	}

	td := ts.Definition
	retry := td.Retry
	if retry == nil {
		retry = &defaultRetry
	}
	maxAttempts := retry.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 1
	}
	timeout := td.Timeout
	if timeout == 0 {
		timeout = e.cfg.DefaultTimeout
	}

	handler, ok := e.lookupHandler(td.Handler)
	if !ok {
		ts.Status = StatusFailed
		ts.Err = "handler not registered: " + td.Handler
		finish()
		return
	}

	var result map[string]any
	var lastErr error
	var canceled bool

	for ts.Attempt < maxAttempts {
		// 工作流级取消（用户取消或兄弟任务失败触发）：不再发起新尝试。
		if wfCtx.Err() != nil {
			canceled = true
			lastErr = ErrCanceled
			break
		}

		ts.Attempt++
		now := time.Now().UTC()
		ts.StartedAt = &now
		ts.Status = StatusRunning
		rec := RetryRecord{Attempt: ts.Attempt, StartedAt: now}
		if !emitProgress() {
			canceled = true
			lastErr = ErrCanceled
			break
		}
		e.logger.Info(wfCtx, "task attempt started",
			"task_id", ts.ID, "attempt", ts.Attempt, "params", td.Params)

		taskCtx := wfCtx
		var taskCancel context.CancelFunc
		if timeout > 0 {
			taskCtx, taskCancel = context.WithTimeout(wfCtx, timeout)
		}

		type hresult struct {
			res map[string]any
			err error
		}
		done := make(chan hresult, 1)
		var attemptSpawn []TaskDefinition
		spawn := func(tasks ...TaskDefinition) error {
			for _, nt := range tasks {
				if nt.ID == "" || nt.Handler == "" {
					return errors.New("spawned task must have id and handler")
				}
			}
			attemptSpawn = append(attemptSpawn, tasks...)
			return nil
		}

		ec := &ExecContext{
			Ctx:        taskCtx,
			WorkflowID: workflowIDFromCtx(wfCtx),
			TaskID:     ts.ID,
			Params:     td.Params,
			Results:    in.results,
			Spawn:      spawn,
			Logger:     e.logger,
		}

		go func() {
			r, err := handler(ec)
			done <- hresult{r, err}
		}()

		var hr hresult
		select {
		case hr = <-done:
		case <-taskCtx.Done():
			// 处理器与计时器同时就绪时优先采用处理器结果，避免误判超时/取消。
			select {
			case hr = <-done:
			default:
				if wfCtx.Err() != nil {
					hr = hresult{nil, ErrCanceled}
				} else {
					hr = hresult{nil, ErrTimeout}
				}
			}
		}
		if taskCancel != nil {
			taskCancel()
		}

		end := time.Now().UTC()
		ts.EndedAt = &end
		rec.EndedAt = end

		if hr.err == nil {
			rec.Err = ""
			ts.Records = append(ts.Records, rec)
			result = hr.res
			spawned = attemptSpawn
			ts.Status = StatusSucceeded
			ts.Result = result
			ts.Err = ""
			e.logger.Info(wfCtx, "task attempt succeeded",
				"task_id", ts.ID, "attempt", ts.Attempt, "spawned", len(spawned))
			finish()
			return
		}

		// 失败 / 超时 / 取消的归类（取消与超时都不算成功）。
		lastErr = hr.err
		if errors.Is(hr.err, context.Canceled) || wfCtx.Err() != nil {
			canceled = true
			lastErr = ErrCanceled
		} else if errors.Is(hr.err, context.DeadlineExceeded) {
			lastErr = ErrTimeout
		}
		rec.Err = lastErr.Error()
		ts.Records = append(ts.Records, rec)
		e.logger.Warn(wfCtx, "task attempt failed",
			"task_id", ts.ID, "attempt", ts.Attempt,
			"max_attempts", maxAttempts, "err", lastErr)

		if canceled {
			break
		}
		if ts.Attempt >= maxAttempts {
			break
		}

		// 指数退避后重试；退避期间被取消则立即终止。
		backoff := computeBackoff(retry, ts.Attempt)
		if backoff > 0 {
			e.logger.Info(wfCtx, "retry scheduled",
				"task_id", ts.ID, "next_attempt", ts.Attempt+1, "backoff", backoff)
			if !sleepCtx(wfCtx, backoff) {
				canceled = true
				lastErr = ErrCanceled
				break
			}
		}
	}

	if canceled {
		ts.Status = StatusCanceled
	} else {
		ts.Status = StatusFailed
	}
	if lastErr != nil {
		ts.Err = lastErr.Error()
	}
	if !canceled && ts.Attempt >= 1 {
		e.logger.Warn(wfCtx, "retries exhausted",
			"task_id", ts.ID, "attempts", ts.Attempt, "err", ts.Err)
	}
	finish()
}

// defaultRetry 是任务未单独配置时使用的策略（不重试）。
var defaultRetry = RetryPolicy{MaxAttempts: 1}

// computeBackoff 计算第 failedAttempt 次失败后的退避时长（指数、封顶）。
func computeBackoff(r *RetryPolicy, failedAttempt int) time.Duration {
	if r == nil || r.InitialBackoff <= 0 {
		return 0
	}
	mult := r.Multiplier
	if mult < 1 {
		mult = 1
	}
	d := r.InitialBackoff
	for i := 1; i < failedAttempt; i++ {
		d = time.Duration(float64(d) * mult)
		if r.MaxBackoff > 0 && d > r.MaxBackoff {
			d = r.MaxBackoff
			break
		}
	}
	if r.MaxBackoff > 0 && d > r.MaxBackoff {
		d = r.MaxBackoff
	}
	return d
}

func workflowIDFromCtx(ctx context.Context) string {
	if id, ok := ctx.Value(workflowIDKey).(string); ok {
		return id
	}
	return ""
}
