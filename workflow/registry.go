package workflow

import (
	"fmt"
	"sync"
)

// Registry maps handler names to deterministic local task handlers.
type Registry struct {
	mu       sync.RWMutex
	handlers map[string]TaskHandler
}

// NewRegistry creates an empty handler registry.
func NewRegistry() *Registry {
	return &Registry{handlers: make(map[string]TaskHandler)}
}

// Register binds a handler name. Registering the same name twice or an empty
// name/nil handler returns an error wrapping ErrInvalidConfig.
func (r *Registry) Register(name string, h TaskHandler) error {
	if name == "" {
		return fmt.Errorf("%w: handler name is empty", ErrInvalidConfig)
	}
	if h == nil {
		return fmt.Errorf("%w: handler %q is nil", ErrInvalidConfig, name)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.handlers[name]; exists {
		return fmt.Errorf("%w: handler %q already registered", ErrInvalidConfig, name)
	}
	r.handlers[name] = h
	return nil
}

// MustRegister is like Register but panics on error (for package-level setup).
func (r *Registry) MustRegister(name string, h TaskHandler) {
	if err := r.Register(name, h); err != nil {
		panic(err)
	}
}

// Get returns the handler for a name, or an error wrapping ErrUnknownHandler.
func (r *Registry) Get(name string) (TaskHandler, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	h, ok := r.handlers[name]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrUnknownHandler, name)
	}
	return h, nil
}
