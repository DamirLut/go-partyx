package eventbus

type Subscriber interface {
	ID() uint64
	Send(topic string, event Event)
}
