# 本地工作流编排引擎（Go）

一个**无外部依赖**的本地工作流编排组件：用本地任务定义（具名处理器）和本地 JSON 状态文件，
跑通真实调度场景，保证并发执行、失败重试、取消/超时语义一致，并支持进程重启后的状态恢复、
结果可解释、执行可追溯。

- 纯 Go 标准库实现，无数据库 / 无网络 / 无第三方服务。
- 状态原子落盘（临时文件 + rename），每次任务状态变化都可持久化。
- 处理器按名称注册，定义与状态均可 JSON 序列化，因此动态生成的任务也能跨进程恢复。

## 1. 快速开始

```go
eng, _ := workflow.NewEngine(workflow.Config{
    StateDir:       "./.wfstate", // 状态文件目录；留空则用内存存储（仅测试）
    MaxConcurrency: 4,            // 单实例内同时执行的任务上限，<=0 不限
    DefaultTimeout: 5 * time.Second,
})

_ = eng.RegisterHandler("echo", func(ec *workflow.ExecContext) (map[string]any, error) {
    return map[string]any{"msg": ec.Params["msg"]}, nil
})

def := &workflow.Definition{
    Name: "demo",
    Tasks: []workflow.TaskDefinition{
        {ID: "a", Handler: "echo", Params: map[string]any{"msg": "hello"}},
        {ID: "b", Handler: "echo", DependsOn: []string{"a"},
         Retry: &workflow.RetryPolicy{MaxAttempts: 3, InitialBackoff: 100 * time.Millisecond,
             MaxBackoff: 2 * time.Second, Multiplier: 2}},
    },
}
st, err := eng.Start(context.Background(), def, "instance-001")
```

运行演示程序：

```bash
go run ./cmd/wfdemo diamond     # 依赖调度（菱形 DAG）+ 动态生成任务
go run ./cmd/wfdemo retry       # 任务前两次失败、第三次成功
go run ./cmd/wfdemo fail        # 失败分支 + 条件下游（重试耗尽）
go run ./cmd/wfdemo resume <state-dir> <workflow-id>  # 恢复中断实例
go run ./cmd/wfdemo recover <state-dir>               # 恢复目录内所有非终态实例
```

## 2. 核心概念

| 概念 | 说明 |
| --- | --- |
| `Definition` | 工作流静态定义：任务列表 + 默认重试策略。 |
| `TaskDefinition` | 任务节点：`ID`、具名 `Handler`、`Params`、`DependsOn`、`Condition`、`Retry`、`Timeout`、`FailureBranch`。 |
| `State` | 实例的完整持久化状态：工作流状态、每个任务的状态/结果/尝试记录、任务加入顺序。 |
| `ExecContext` | 运行期上下文：取消信号、入参、**已成功依赖的结果快照**、`Spawn`（动态生成）、日志器。 |

任务状态机：

```
pending ──> ready ──> running ──> succeeded
                        │  ^         │
                        └──┘(重试)   ├──> failed    （重试耗尽或非取消性错误）
                        └───────────┴──> canceled  （工作流取消 / 超时归并的失败取消）
pending（依赖门控不通过） ──> skipped
```

## 3. 调度语义

### 3.1 依赖调度

- 任务仅当其 **所有 `DependsOn` 依赖都进入终态**后才参与门控判断。
- 引擎用 Kahn 拓扑排序校验定义；存在环、自依赖、缺失依赖直接拒绝（见第 6 节错误码）。
- 同层无依赖关系的任务**并发执行**，并发度受 `MaxConcurrency` 限制（带缓冲信号量）。
- 上游结果通过 `ExecContext.Results` 以**只读快照**传入（仅包含 succeeded 的依赖）。

### 3.2 默认门控规则（无条件时）

- 所有依赖均为 `succeeded` → 执行本任务。
- 任一依赖为 `failed / skipped / canceled` → 本任务标记 `skipped`（不执行，也不算失败）。

### 3.3 条件分支

`TaskDefinition.Condition` 支持两种形式：

1. 状态表达式 `"taskID:status"`，status ∈ `succeeded|failed|skipped|canceled`。
   当且仅当被引用任务处于指定状态时执行，否则 `skipped`。
2. 具名条件 `"cond:name"`，对应 `eng.RegisterCondition(name, func)`，输入为全部任务状态快照。

**失败分支标记 `FailureBranch: true`**：该任务失败**不会**中止整个工作流，而是保留
`failed` 终态，交由条件下游（如 `Condition: "check:failed"`）决定走哪个分支；不带条件的
默认下游仍按"依赖未成功 → skipped"处理。未标记 `FailureBranch` 的任务一旦失败，工作流
立即进入 `failed`，在途兄弟任务收到取消信号，所有未开始任务标记 `canceled`。

### 3.4 运行中动态生成任务

处理器可调用 `ec.Spawn(taskDefs...)` 在运行时追加后续任务：

- 动态任务与静态任务一样进入持久化状态（标记 `Dynamic: true`），支持跨进程恢复。
- 接入前复用定义校验：**空 ID/Handler、重复 ID、缺失依赖、成环、未注册处理器一律拒绝**，
  该工作流置为 `failed`，`failure_cause` 写明具体原因，结果可解释。

### 3.5 重试策略

`RetryPolicy{MaxAttempts, InitialBackoff, MaxBackoff, Multiplier}`：

- `MaxAttempts` 含首次执行；退避为指数 `InitialBackoff * Multiplier^(n-1)` 并封顶 `MaxBackoff`。
- 每次尝试都写入一条 `RetryRecord{attempt, started_at, ended_at, err}`，可追溯。
- **退避期间响应取消**；被取消的尝试不重试。
- 任务级 `Retry` 非空时覆盖工作流 `DefaultRetry`。

### 3.6 超时与取消

- 任务级 `Timeout` 覆盖引擎 `DefaultTimeout`；超时返回 `ErrTimeout`，按普通失败走重试，
  重试耗尽后 `failed`，**绝不算成功**。
- `eng.Cancel(workflowID)` 或父 `context.Context` 取消：在途任务收到 ctx 取消并以
  `canceled` 收尾，未开始任务标记 `canceled`，工作流状态为 `canceled`。
- 某任务失败导致工作流中止时，同样通过取消信号通知在途兄弟任务，避免产生多余副作用。
  与失败任务**无依赖关系**的在途任务：若尊重 ctx 则在收到取消后以 `canceled` 收尾；
  若在取消信号到达前已执行完成，则保留其真实终态（`succeeded`）。尚未开始的任务
  一律 `canceled`。

### 3.7 重复触发的幂等保护

- 每个工作流实例有独立互斥锁（`TryLock`）：同一实例并发 `Start`，只有一个执行，
  其余返回 `ExecError{Code: instance_already_running}`，处理器不会被并发调用，无重复结果。
- 已在终态（succeeded/failed/canceled）的实例再次 `Start` 返回
  `instance_terminal`；对终态实例 `Resume` 直接幂等返回原状态。
- 进程崩溃后残留 `running` 的实例，`Start` 会被拒绝并提示使用 `Resume`/`RecoverAll`。

## 4. 状态保存与恢复规则

状态文件：`<StateDir>/<url-path-escape(workflowID)>.json`，写入方式为
**写临时文件 → fsync 级 rename**，崩溃不会产生半截 JSON。

恢复规则（`Resume` / `RecoverAll`）：

1. `succeeded / failed / skipped / canceled` 的任务**绝不重复执行**，其 `Result` 原样可用。
2. 崩溃时处于 `running / ready` 的任务没有终态记录，恢复时重置为 `pending`、
   `Attempt` 清零，重新按重试策略执行。
3. 动态生成的任务定义内嵌在各自的 `TaskState` 中，因此恢复**不依赖原始 Definition 对象**。
4. 恢复后重新进行正常调度：未完成任务继续跑，直到所有任务终态；终态实例 `Resume` 无副作用。
5. `RecoverAll` 扫描状态目录，对所有非终态实例依次恢复。

> **执行语义为 at-least-once**：`running` 心跳在调用处理器前同步落盘并等待确认，但
> "处理器已执行"与"结果已落盘"之间的崩溃窗口客观存在，恢复可能导致该任务重跑一次。
> 需要严格 exactly-once 的处理器应自行基于输入做幂等。

`State` 中可追溯的字段：`status`、`failure_cause`、每个任务的 `status/attempt/err/
result/started_at/ended_at/records[]`，以及 `task_order`（稳定的执行/遍历顺序）。

## 5. 配置项

| `Config` 字段 | 含义 |
| --- | --- |
| `StateDir` | JSON 状态目录；为空时使用进程内内存存储（仅适合测试）。 |
| `MaxConcurrency` | 同一工作流实例内并发任务上限；`<=0` 不限。 |
| `DefaultTimeout` | 任务默认超时；`0` 表示不超时（仍受 ctx 取消控制）。 |

任务级覆盖：`TaskDefinition.Timeout`、`TaskDefinition.Retry`。

## 6. 错误码（原因可区分）

定义被拒绝时返回 `*DefinitionError`，`Code` 取值：

| Code | 触发原因 |
| --- | --- |
| `duplicate_task_id` | 多个任务使用相同 ID。 |
| `missing_dependency` | `DependsOn` 或条件表达式引用了不存在的任务。 |
| `cyclic_dependency` | 依赖图含环（含自依赖）。 |
| `invalid_config` | 名称/处理器为空、重试或超时参数非法、条件语法非法、依赖列表内重复等。 |

运行期返回 `*ExecError`：`instance_already_running`、`instance_terminal`、
`task_timeout`（处理器侧错误值）、`canceled`、`task_failed`、`state_corrupt`。

## 7. 日志

`NewLogger(io.Writer)` 输出结构化文本行，包含：关键输入（params）、调度决策
（dispatched / skipped by gate / dynamic task accepted）、每次尝试结果与退避、
最终状态与失败原因。测试中把日志接入 `testing.T.Log`，失败时自动打印完整调度轨迹。

## 8. 本地验证

```bash
go build ./...
go vet ./...
go test ./...            # 全部场景单测
go test -race ./...      # 竞争检测
go test -v -run TestRetryExhausted ./workflow
```

测试覆盖场景（全部使用确定性本地环境，`t.TempDir()` 状态目录，无外部服务、
无真实时钟依赖之外的不确定性）：

- 非法定义：重复 ID / 缺失依赖 / 环（自依赖、双节点、三节点）/ 各类非法配置，断言错误码。
- 并发：并发上限不被突破且能打满；同实例 8 路并发触发恰好执行一次、其余被拒绝；终态拒绝重跑。
- 重试：重试耗尽后失败、`RetryRecord` 完整、下游不执行；前两次失败第三次成功。
- 超时：超时不算成功，依赖者被取消。
- 取消：`Cancel` 与父 ctx 取消两条路径，在途与未开始任务均 `canceled`。
- 条件分支：状态表达式 + 具名条件 + `FailureBranch` + 默认 skipped 规则。
- 动态生成：链式动态任务按依赖执行；重复 ID / 成环被拒绝。
- 重启恢复：手工构造崩溃现场（succeeded + running + pending 混合），新引擎恢复后
  已完成任务零重跑、结果链路完整；终态幂等；`RecoverAll` 只恢复非终态实例；
  动态任务的恢复。
