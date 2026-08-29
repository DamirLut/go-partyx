package command

import (
	"fmt"
	"sync"
)

// Registry maps method opcodes to handlers; Register is safe to call
// concurrently with Get.
type Registry struct {
	mu       sync.RWMutex
	handlers map[uint16]Handler
}

func NewRegistry() *Registry {
	return &Registry{
		handlers: make(map[uint16]Handler),
	}
}

// Register panics on a duplicate opcode: a collision is a programming error
// best caught at startup.
func (r *Registry) Register(op uint16, handler Handler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.handlers[op]; exists {
		panic(fmt.Sprintf("command: opcode %d already registered", op))
	}
	r.handlers[op] = handler
}

func (r *Registry) Get(op uint16) (Handler, bool) {
	r.mu.RLock()
	h, ok := r.handlers[op]
	r.mu.RUnlock()
	return h, ok
}

func (r *Registry) Has(op uint16) bool {
	_, ok := r.Get(op)
	return ok
}
