package eventbus

import (
	"log/slog"
	"sync"
)

type Topic struct {
	mu          sync.RWMutex
	subscribers map[uint64]Subscriber
	logger      *slog.Logger
}

func newTopic(logger *slog.Logger) *Topic {
	return &Topic{
		subscribers: make(map[uint64]Subscriber),
		logger:      logger,
	}
}

func (t *Topic) add(sub Subscriber) {
	t.mu.Lock()
	t.subscribers[sub.ID()] = sub
	t.mu.Unlock()
}

func (t *Topic) remove(sub Subscriber) {
	t.mu.Lock()
	delete(t.subscribers, sub.ID())
	t.mu.Unlock()
}

func (t *Topic) isEmpty() bool {
	t.mu.RLock()
	empty := len(t.subscribers) == 0
	t.mu.RUnlock()
	return empty
}

// send snapshots subscribers under the lock and delivers outside it, so a
// slow subscriber does not block subscribe/unsubscribe, and a panicking
// subscriber does not take down the publisher.
func (t *Topic) send(topic string, event Event) {
	t.mu.RLock()
	subs := make([]Subscriber, 0, len(t.subscribers))
	for _, sub := range t.subscribers {
		subs = append(subs, sub)
	}
	t.mu.RUnlock()

	for _, sub := range subs {
		t.deliver(topic, event, sub)
	}
}

func (t *Topic) deliver(topic string, event Event, sub Subscriber) {
	defer func() {
		if r := recover(); r != nil {
			t.logger.Error("eventbus: subscriber panicked", "subscriber", sub.ID(), "topic", topic, "panic", r)
		}
	}()
	sub.Send(topic, event)
}
