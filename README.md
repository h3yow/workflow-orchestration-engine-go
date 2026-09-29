# 本地工作流编排引擎（Go）

一个**纯本地、确定性、可持久化**的工作流编排组件。用本地任务定义（DAG）与本地文件状态跑通真实调度，
不依赖任何外部服务（无数据库 / MQ / HTTP），重点保证：

- 按依赖关系调度、条件分支、运行中动态生成后续任务；
- 非法定义（环、重复 ID、缺失依赖、无效配置、未知 handler）**按可区分的错误原因拒绝**；
- 并发上限、任务超时、运行取消、指数退避重试；
- 同一实例重复触发**不并发执行、不产生重复结果**；
- 进程重启后**已完成任务不重跑**，未完成任务按重试策略继续或终止；
- 最终状态、失败原因、每次尝试（attempt）与退避记录全程可追溯。

模块路径：`github.com/h3yow/workflow-orchestration-engine-go`，包：`workflow`。

---

## 1. 快速开始

```go
eng, _ := workflow.NewEngine(workflow.Config{StateDir: "./.wf-state", MaxParallel: 4})

eng.Registry().MustRegister("build", func(ctx context.Context, in workflow.TaskInput) (workflow.TaskResult, error) {
    // 本地确定性工作单元；应及时响应 ctx.Done()
    return workflow.Result(map[string]any{"artifact": "app.tar"}), nil
})

spec := workflow.WorkflowSpec{
    Name: "pipeline", MaxParallel: 4,
    Tasks: []workflow.TaskSpec{
        {ID: "build", Handler: "build", Timeout: 5 * time.Second,
         Retry: workflow.RetryPolicy{MaxAttempts: 3, Backoff: 100 * time.Millisecond, MaxBackoff: time.Second, Factor: 2}},
        {ID: "deploy", Handler: "deploy", DependsOn: []string{"build"},
         Condition: &workflow.Condition{Op: "truthy", Ref: "build.artifact"}},
    },
}

inst, err := eng.Start(ctx, spec, "instance-001")
// err == nil 且 inst.Status == succeeded；失败/超时/取消时返回终态实例 + 包装错误
```

运行可观测 Demo：

```bash
go run ./cmd/wfdemo            # 含重试、条件分支、动态 fan-out 的完整流程
go run ./cmd/wfdemo -rerun     # 演示重复触发被 ErrInstanceTerminal 拒绝
go run ./cmd/wfdemo -resume    # 从磁盘恢复未完成实例
```

---

## 2. 定义模型

| 概念 | 说明 |
|---|---|
| `WorkflowSpec` | `Name` + `Tasks[]` + `MaxParallel`（并发上限，`<=0` 视为 1） |
| `TaskSpec.ID` | 实例内唯一任务标识；空/重复会被拒绝 |
| `TaskSpec.Handler` | 注册到 `Registry` 的本地函数名；未知会被拒绝 |
| `TaskSpec.DependsOn` | 依赖的任务 ID；自依赖 / 指向不存在 / 重复列出都会被拒绝 |
| `TaskSpec.Params` | 静态入参，原样传给 handler |
| `TaskSpec.Timeout` | 任务级截止时间；`0` 表示用引擎默认值 |
| `TaskSpec.Retry` | 见重试策略 |
| `TaskSpec.Condition` | 依赖全部满足后、执行前求值；为假则**良性跳过** |
| `TaskSpec.AllowSkip` | 为真时，允许在有非成功（跳过）依赖时仍运行，仅收到成功依赖的输出 |

### 条件分支

`Condition{Op, Ref, Value}`，纯函数、确定性：

- `Ref` 是以任务 ID 开头的点路径：`build` 表示 build 的整个输出，`build.artifact.version` 表示深入字段；
- `Op`：`truthy` / `falsy` / `exists` / `eq` / `ne`（数字经 JSON 解码后按 `float64` 归一化比较）。

### 动态任务

handler 返回 `*workflow.SpawnTasksError{Result, Requests}` 作为**控制流信号**：
任务本身视为成功（输出取 `Result`），引擎对「旧 DAG + 新任务」**整体重新校验**（重复 ID、
未知依赖、环、未知 handler 等），校验通过后原子落盘并调度；任何一项不合法则**什么都不加**，
该任务记为一次错误尝试并按策略重试 / 失败。动态任务通过 `DependsOn` 接回 DAG。

---

## 3. 调度语义

1. **就绪判定**：仅当一个任务的全部依赖都处于终态时才参与调度；拓扑序稳定（定义序打破并列），
   保证调度与日志可解释、可复现。
2. **依赖结果一致性规则**：
   - 全部依赖 `succeeded` → 任务可运行，handler 收到成功依赖的输出（`TaskInput.Outputs`）；
   - 存在 `failed / timed_out / canceled / skipped` 依赖且未设置 `AllowSkip` → 任务 `skipped`
     （原因写入 `TaskState.Error`，如 `dependency "x" did not succeed (status=failed)`）并向下游级联；
   - 设置 `AllowSkip` 且无硬失败阻断 → 仍可运行，只看到成功依赖的输出。
3. **条件为假** → 任务 `skipped`（良性跳过），其下游按同一依赖规则级联跳过；**条件分支不导致实例失败**。
4. **成功定义**：handler 返回 `nil` 且其任务上下文未到期/未取消才算成功。handler 在截止时间之后
   才返回的结果（即使是 nil）一律按超时/取消处理，**绝不可能把超时或取消记成成功**。
5. **并发上限**：同时在执行（含退避等待）的任务数不超过 `MaxParallel`。
6. **实例终态**：
   - 有任何 `failed` / `timed_out` 任务 → 实例 `failed`（`Start/Resume` 返回包装了
     `ErrTaskFailed` / `ErrTaskTimedOut` 的错误，但返回的 `*Instance` 是完整终态）；
   - 否则若运行 context 被取消 → 实例 `canceled`（错误包装 `ErrTaskCanceled`）；
   - 否则（含良性跳过的分支）→ 实例 `succeeded`。

### 重试策略 `RetryPolicy`

- `MaxAttempts`：含首次的总尝试次数，`<=1` 表示不重试；
- `Backoff` / `MaxBackoff` / `Factor`：指数退避，第 n 次失败后等待
  `min(Backoff * Factor^(n-1), MaxBackoff)`；
- **普通错误与任务超时**会消耗尝试次数并按策略重试；
- **运行被取消**不重试（取消是确定性终止语义）；
- 每次尝试的起止时间、`success|error|timeout|canceled|interrupted` 结果、错误信息和退避时长
  都记录在 `TaskState.Attempts` 中。

---

## 4. 重复触发与幂等

- 运行锁 = 进程内 per-instance 互斥量 + `<id>.lock` 上的跨进程 `flock(2)`；
- `Start` 使用**非阻塞 `TryLock`**：实例正在运行（本进程 goroutine 或另一个进程）→
  立即返回包装 `ErrInstanceActive`，不会排队、不会并发跑第二份；
- 实例已处于终态后再次 `Start` → `ErrInstanceTerminal`，**不会重跑、不产生第二份结果**；
- 若发现实例处于**非终态**（上次进程崩溃）→ `Start` 自动等价于 `Resume`（见下节）。

---

## 5. 状态持久化与重启恢复规则

每个实例一个 JSON 文件 `<StateDir>/<id>.json`：

- 写入是**崩溃安全原子写**：同目录临时文件 → `fsync` → 硬链接创建（Create-only-if-absent）/
  `rename` 覆盖（Save）→ 目录 `fsync`；
- 任何状态迁移（派发、尝试结束、跳过、成功、失败、spawn 落盘、终态）后立即持久化；
- 运行结束即使是因为 context 取消，最终状态也使用独立 context 强制落盘。

### 崩溃 / kill -9 后的恢复（`Resume`，或对非终态实例再 `Start`）

1. `succeeded` 任务：**绝不重新执行**（输出沿用磁盘记录，下游可见）；
2. 崩溃时停在 `running` 的任务：其未闭合的最后一次 attempt 标记为
   `interrupted`（错误信息 `process restarted before attempt completed`），然后：
   - 已用尝试次数 `< MaxAttempts` → 回到 `pending` 重新派发，继续走重试策略；
   - 已耗尽 → 置为 `failed`（`retries exhausted after process restart`），下游按依赖规则跳过；
3. `skipped / failed / timed_out / canceled` 终态保持不变，不重跑；
4. 崩溃期间未派发的 `pending` 任务正常继续；
5. 动态任务一旦已落盘就属于 DAG 的一部分，恢复后照常调度。

测试中的真实进程重启验证：`TestRealProcessRestart_Resume` 会 re-exec 测试二进制，
子进程在任务中途 `os.Exit(42)`，由新进程 `Resume` 收尾并断言 `step1` 不重跑。

---

## 6. 可区分的错误原因

所有错误都可用 `errors.Is` 判定哨兵原因，定义类错误还可 `errors.As` 成 `*ValidationError`
（含 `TaskID` / `Detail` / `Related`，环检测会给出环路成员）：

| 哨兵 | 触发场景 |
|---|---|
| `ErrInvalidConfig` | 空名/空任务、空 ID/handler、负超时或负重试参数、坏条件表达式、重复列出依赖等 |
| `ErrDuplicateTaskID` | 静态或动态任务 ID 重复 |
| `ErrMissingDependency` | 依赖指向不存在的任务 |
| `ErrSelfDependency` | 任务依赖自身 |
| `ErrCyclicDependency` | DAG 中存在环 |
| `ErrUnknownHandler` | handler 名未注册 |
| `ErrInstanceActive` | 实例正在运行（含跨进程持锁） |
| `ErrInstanceTerminal` | 对已终态实例再次触发/恢复 |
| `ErrInstanceNotFound` / `ErrInvalidState` | 实例不存在 / 状态文件损坏 |
| `ErrTaskFailed` / `ErrTaskTimedOut` / `ErrTaskCanceled` | 实例终态分类 |

---

## 7. 配置项 `workflow.Config`

| 字段 | 含义 | 默认/说明 |
|---|---|---|
| `StateDir` | 持久化目录（不存在会创建） | 必填，空目录返回 `ErrInvalidConfig` |
| `MaxParallel` | 全局默认并发上限 | spec 上的 `MaxParallel` 优先；最终 `<=0` 按 1 |
| `TaskTimeout` | 任务默认超时 | 任务自身 `Timeout>0` 时优先；`0` 表示不设默认超时 |
| `RecoveryResume` | 预留给“打开引擎时自动恢复”开关 | 当前以显式 `Resume` / 崩溃后 `Start` 为准 |

日志：默认输出到 stderr（`[workflow]` 前缀，含时间戳），可用 `eng.SetLogger(...)` 替换；
测试使用的 `testLogger` 通过 `t.Log` 打印关键输入、调度决策与最终状态（`go test -v` 可见）。

---

## 8. 本地验证方法（无外部依赖）

```bash
# 全部单测，带竞态检测；多轮重复以验证取消/并发场景的稳定性
go test ./... -race -count=5

# 覆盖率
go test ./workflow/ -cover

# 只跑重点场景
go test ./workflow -race -v -run 'TestDuplicateTrigger'        # 并发重复触发
go test ./workflow -race -v -run 'TestEngine_Retry'            # 重试耗尽 / 重试后成功
go test ./workflow -race -v -run 'TestCancelRunningWorkflow'   # 取消
go test ./workflow -race -v -run 'TestRecovery'                # 状态恢复
go test ./workflow -race -v -run 'TestRealProcessRestart'      # 真进程重启
go test ./workflow -race -v -run 'TestValidate'                # 非法定义拒绝

go vet ./...
gofmt -l .   # 应无输出

# 端到端手工验证
rm -rf .wf-state && go run ./cmd/wfdemo
```

测试覆盖的场景：合法/非法 DAG 校验（14 个拒绝原因）、条件求值、依赖成功/失败传播、
重试耗尽与重试恢复、任务超时（迟到的成功不算成功）、运行取消、条件分支与级联跳过、
`AllowSkip`、动态 spawn 成功/非法拒绝/ID 冲突、并发上限（实测最大在飞数）、
8 路并发重复触发只允许 1 次执行、文件锁进程内与跨“进程”（独立 store 句柄）互斥、
原子写往返、崩溃恢复（已完成不重跑 / 中断任务重试或耗尽失败）以及真实进程重启 E2E。

## 9. 状态文件结构（可追溯性）

```jsonc
{
  "id": "DEMO", "spec_name": "demo-pipeline", "status": "succeeded",
  "run_id": "b8492d2c3c92dd08",
  "topo_order": ["prepare", "flaky", ...],
  "tasks": {
    "flaky": {
      "id": "flaky", "status": "succeeded",
      "attempts": [
        {"n":1,"started_at":"...","ended_at":"...","outcome":"error","error":"...","backoff":20000000},
        {"n":2,...,"outcome":"error","backoff":40000000},
        {"n":3,...,"outcome":"success"}
      ],
      "output": "ok-after-retries",
      "spec": { /* 完整任务定义，恢复时自包含 */ }
    }
  }
}
```
