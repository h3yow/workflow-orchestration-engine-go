package workflow

import (
	"fmt"
	"log"
	"os"
)

// Logger is the minimal logging surface used by the engine. Tests use it to
// print key inputs, scheduling decisions and final states.
type Logger interface {
	Debugf(format string, args ...any)
	Infof(format string, args ...any)
	Warnf(format string, args ...any)
	Errorf(format string, args ...any)
}

// stdLogger writes to stderr with a prefix.
type stdLogger struct{ l *log.Logger }

func newStdLogger() Logger {
	return &stdLogger{l: log.New(os.Stderr, "[workflow] ", log.LstdFlags|log.Lmicroseconds)}
}

func (s *stdLogger) Debugf(format string, args ...any) {}
func (s *stdLogger) Infof(format string, args ...any) {
	s.l.Output(2, fmt.Sprintf(format, args...))
}
func (s *stdLogger) Warnf(format string, args ...any) {
	s.l.Output(2, "WARN "+fmt.Sprintf(format, args...))
}
func (s *stdLogger) Errorf(format string, args ...any) {
	s.l.Output(2, "ERROR "+fmt.Sprintf(format, args...))
}

// noopLogger discards everything.
type noopLogger struct{}

func (noopLogger) Debugf(string, ...any) {}
func (noopLogger) Infof(string, ...any)  {}
func (noopLogger) Warnf(string, ...any)  {}
func (noopLogger) Errorf(string, ...any) {}
