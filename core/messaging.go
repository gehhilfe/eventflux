package core

type UnsubscribeFunc func() error

func (u UnsubscribeFunc) Unsubscribe() error {
	return u()
}

type Unsubscriber interface {
	Unsubscribe() error
}

type MessageBus interface {
	Publish(subject string, message []byte) error
	Subscribe(subject string, handler func(message []byte, metadata map[string]string) error) (Unsubscriber, error)
}
