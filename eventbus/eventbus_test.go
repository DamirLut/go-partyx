package eventbus

import (
	"sync"
	"testing"

	"github.com/damirlut/go-partyx/protocol"
)

type testSub struct {
	id     uint64
	mu     sync.Mutex
	got    []Event
	panics bool
}

func (s *testSub) ID() uint64 { return s.id }

func (s *testSub) Send(topic string, e Event) {
	if s.panics {
		panic("boom")
	}
	s.mu.Lock()
	s.got = append(s.got, e)
	s.mu.Unlock()
}

func (s *testSub) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.got)
}

func TestPublishDelivers(t *testing.T) {
	b := New(nil)
	sub := &testSub{id: 1}
	b.Subscribe("room:1", sub)

	b.Publish("room:1", NewEvent(uint16(protocol.EventPlayerJoined), &protocol.PlayerJoined{PlayerID: 7}))

	if sub.count() != 1 {
		t.Fatalf("got %d events, want 1", sub.count())
	}
	e := sub.got[0]
	if e.Op != uint16(protocol.EventPlayerJoined) {
		t.Fatalf("op = %d, want %d", e.Op, protocol.EventPlayerJoined)
	}
	var decoded protocol.PlayerJoined
	if _, err := decoded.Unmarshal(e.Payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if decoded.PlayerID != 7 {
		t.Fatalf("PlayerID = %d, want 7", decoded.PlayerID)
	}
}

func TestPublishUnknownTopic(t *testing.T) {
	b := New(nil)
	b.Publish("missing", NewEvent(1, nil)) // must not panic
}

func TestNilMessageYieldsEmptyPayload(t *testing.T) {
	e := NewEvent(42, nil)
	if len(e.Payload) != 0 {
		t.Fatalf("payload = %v, want empty", e.Payload)
	}
}

func TestUnsubscribeStopsDelivery(t *testing.T) {
	b := New(nil)
	sub := &testSub{id: 1}
	b.Subscribe("room:1", sub)
	b.Unsubscribe("room:1", sub)

	b.Publish("room:1", NewEvent(1, nil))
	if sub.count() != 0 {
		t.Fatalf("got %d events after unsubscribe, want 0", sub.count())
	}
}

func TestEmptyTopicIsRemoved(t *testing.T) {
	b := New(nil)
	sub := &testSub{id: 1}
	b.Subscribe("room:1", sub)
	b.Unsubscribe("room:1", sub)

	if _, ok := b.topics.Load("room:1"); ok {
		t.Fatal("empty topic should be removed")
	}
}

func TestTopicWithSubscribersIsKept(t *testing.T) {
	b := New(nil)
	s1 := &testSub{id: 1}
	s2 := &testSub{id: 2}
	b.Subscribe("room:1", s1)
	b.Subscribe("room:1", s2)
	b.Unsubscribe("room:1", s1)

	if _, ok := b.topics.Load("room:1"); !ok {
		t.Fatal("topic with subscribers should be kept")
	}
	b.Publish("room:1", NewEvent(1, nil))
	if s1.count() != 0 || s2.count() != 1 {
		t.Fatalf("s1=%d s2=%d, want 0 and 1", s1.count(), s2.count())
	}
}

func TestPanickingSubscriberDoesNotBreakPublish(t *testing.T) {
	b := New(nil)
	bad := &testSub{id: 1, panics: true}
	good := &testSub{id: 2}
	b.Subscribe("room:1", bad)
	b.Subscribe("room:1", good)

	b.Publish("room:1", NewEvent(1, nil)) // must not panic

	if good.count() != 1 {
		t.Fatalf("good subscriber got %d events, want 1", good.count())
	}
}
