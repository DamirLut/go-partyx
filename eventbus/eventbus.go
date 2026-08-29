package eventbus

import (
	"log/slog"
	"sync"
)

type EventBus struct {
	topics sync.Map
	logger *slog.Logger
}

// New builds an EventBus. A nil logger falls back to slog.Default().
func New(logger *slog.Logger) *EventBus {
	if logger == nil {
		logger = slog.Default()
	}
	return &EventBus{logger: logger}
}

func (b *EventBus) getTopic(name string) *Topic {
	v, _ := b.topics.LoadOrStore(name, newTopic(b.logger))
	return v.(*Topic)
}

// Subscribe adds sub to the topic. If a concurrent Unsubscribe removed the
// empty topic in the meantime, the subscription is retried on a fresh topic
// instance so it is never lost.
func (b *EventBus) Subscribe(topic string, sub Subscriber) {
	for {
		t := b.getTopic(topic)
		t.add(sub)
		if v, ok := b.topics.Load(topic); ok && v == t {
			return
		}
		t.remove(sub)
	}
}

// Unsubscribe removes sub from the topic and deletes the topic once it has
// no subscribers left, so the bus does not grow unboundedly.
func (b *EventBus) Unsubscribe(topic string, sub Subscriber) {
	v, ok := b.topics.Load(topic)
	if !ok {
		return
	}
	t := v.(*Topic)
	t.remove(sub)
	if t.isEmpty() {
		// Deletes only if the map still holds this exact topic instance.
		b.topics.CompareAndDelete(topic, t)
	}
}

func (b *EventBus) Publish(topic string, event Event) {
	if v, ok := b.topics.Load(topic); ok {
		v.(*Topic).send(topic, event)
	}
}
