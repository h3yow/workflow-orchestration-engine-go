package workflow

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"
)

// Logger 最小日志接口，便于测试注入确定性输出。
type Logger interface {
	Info(ctx context.Context, msg string, kv ...any)
	Warn(ctx context.Context, msg string, kv ...any)
	Error(ctx context.Context, msg string, kv ...any)
}

type ctxKey int

const workflowIDKey ctxKey = iota

// WithWorkflowID 把工作流实例 ID 放入 context，日志会自动带上。
func WithWorkflowID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, workflowIDKey, id)
}

// stdLogger 结构化文本日志：[时间] LEVEL msg key=value ...
type stdLogger struct {
	l *log.Logger
}

// NewStdLogger 创建输出到标准错误的日志器。
func NewStdLogger() Logger {
	return NewLogger(os.Stderr)
}

// NewLogger 创建写入任意 io.Writer 的日志器（测试可传入 buffer 做确定性断言）。
func NewLogger(w io.Writer) Logger {
	return &stdLogger{l: log.New(w, "", 0)}
}

// noopLogger 丢弃所有日志。
type noopLogger struct{}

// NewNopLogger 返回不输出任何内容的日志器。
func NewNopLogger() Logger { return noopLogger{} }

func (noopLogger) Info(context.Context, string, ...any)  {}
func (noopLogger) Warn(context.Context, string, ...any)  {}
func (noopLogger) Error(context.Context, string, ...any) {}

func (g *stdLogger) write(level string, ctx context.Context, msg string, kv []any) {
	var sb strings.Builder
	sb.WriteString(time.Now().UTC().Format("2006-01-02T15:04:05.000Z07:00"))
	sb.WriteString(" ")
	sb.WriteString(level)
	if id, ok := ctx.Value(workflowIDKey).(string); ok && id != "" {
		fmt.Fprintf(&sb, " workflow_id=%q", id)
	}
	sb.WriteString(" ")
	sb.WriteString(msg)
	for i := 0; i+1 < len(kv); i += 2 {
		fmt.Fprintf(&sb, " %v=%v", kv[i], kv[i+1])
	}
	if len(kv)%2 == 1 {
		fmt.Fprintf(&sb, " UNPAIRED=%v", kv[len(kv)-1])
	}
	g.l.Println(sb.String())
}

func (g *stdLogger) Info(ctx context.Context, msg string, kv ...any) {
	g.write("INFO ", ctx, msg, kv)
}
func (g *stdLogger) Warn(ctx context.Context, msg string, kv ...any) {
	g.write("WARN ", ctx, msg, kv)
}
func (g *stdLogger) Error(ctx context.Context, msg string, kv ...any) {
	g.write("ERROR", ctx, msg, kv)
}
