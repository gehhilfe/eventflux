package bus

import (
	"errors"

	"github.com/gehhilfe/eventflux/core"
)

type InMemoryMessageBus struct {
	subscriptions map[string][]func(message []byte, metadata core.Metadata) error
}

func NewInMemoryMessageBus() *InMemoryMessageBus {
	return &InMemoryMessageBus{
		subscriptions: make(map[string][]func(message []byte, metadata core.Metadata) error),
	}
}

func (b *InMemoryMessageBus) Publish(subject string, message []byte) error {
	for _, handler := range b.subscriptions[subject] {
		if err := handler(message, core.Metadata{}); err != nil {
			return err
		}
	}
	return nil
}

func (b *InMemoryMessageBus) Subscribe(subject string, handler func(message []byte, metadata core.Metadata) error) (core.Unsubscriber, error) {
	b.subscriptions[subject] = append(b.subscriptions[subject], handler)
	return &unsubscriber{
		bus:     b,
		subject: subject,
		handler: &handler,
	}, nil
}

type unsubscriber struct {
	bus     *InMemoryMessageBus
	subject string
	handler *func(message []byte, metadata core.Metadata) error
}

func (u *unsubscriber) Unsubscribe() error {
	handlers := u.bus.subscriptions[u.subject]
	for i, handler := range handlers {
		if &handler == u.handler {
			u.bus.subscriptions[u.subject] = append(handlers[:i], handlers[i+1:]...)
			return nil
		}
	}
	return errors.New("subscription not found")
}
