package eventflux

import (
	"encoding/json"

	fluxcore "github.com/gehhilfe/eventflux/core"
)

type typedMessageBus struct {
	bus fluxcore.MessageBus
}

type Typer interface {
	Type() string
}

func NewTypedMessageBus(
	bus fluxcore.MessageBus,
) *typedMessageBus {
	return &typedMessageBus{
		bus: bus,
	}
}

func (b *typedMessageBus) Publish(message Typer) error {
	data, err := json.Marshal(message)
	if err != nil {
		return err
	}
	return b.bus.Publish(message.Type(), data)
}

type typedMessageHandler struct {
	Commited      func(message *MessageCommitedEvent, metadata map[string]string) error
	RequestResync func(message *MessageRequestResync, metadata map[string]string) error
	ResyncEvents  func(message *MessageResyncEvents, metadata map[string]string) error
	HeartBeat     func(message *MessageHeartBeat, metadata map[string]string) error
}

func (b *typedMessageBus) Subscribe(
	handler typedMessageHandler,
) (fluxcore.Unsubscriber, error) {
	unsuber := &unsubscriber{
		subscribers: make([]fluxcore.Unsubscriber, 0, 4),
	}

	subCommited, err := b.bus.Subscribe((&MessageCommitedEvent{}).Type(), func(message []byte, metadata map[string]string) error {
		var msg MessageCommitedEvent
		if err := json.Unmarshal(message, &msg); err != nil {
			return err
		}
		return handler.Commited(&msg, metadata)
	})
	if err != nil {
		return nil, err
	}
	unsuber.subscribers = append(unsuber.subscribers, subCommited)

	subResync, err := b.bus.Subscribe((&MessageRequestResync{}).Type(), func(message []byte, metadata map[string]string) error {
		var msg MessageRequestResync
		if err := json.Unmarshal(message, &msg); err != nil {
			return err
		}
		return handler.RequestResync(&msg, metadata)
	})
	if err != nil {
		unsuber.Unsubscribe()
		return nil, err
	}
	unsuber.subscribers = append(unsuber.subscribers, subResync)

	subResyncEvents, err := b.bus.Subscribe((&MessageResyncEvents{}).Type(), func(message []byte, metadata map[string]string) error {
		var msg MessageResyncEvents
		if err := json.Unmarshal(message, &msg); err != nil {
			return err
		}
		return handler.ResyncEvents(&msg, metadata)
	})
	if err != nil {
		unsuber.Unsubscribe()
		return nil, err
	}
	unsuber.subscribers = append(unsuber.subscribers, subResyncEvents)

	subHeartBeat, err := b.bus.Subscribe((&MessageHeartBeat{}).Type(), func(message []byte, metadata map[string]string) error {
		var msg MessageHeartBeat
		if err := json.Unmarshal(message, &msg); err != nil {
			return err
		}
		return handler.HeartBeat(&msg, metadata)
	})
	if err != nil {
		unsuber.Unsubscribe()
		return nil, err
	}
	unsuber.subscribers = append(unsuber.subscribers, subHeartBeat)

	return unsuber, nil
}

type unsubscriber struct {
	subscribers []fluxcore.Unsubscriber
}

func (u *unsubscriber) Unsubscribe() error {
	for _, sub := range u.subscribers {
		if err := sub.Unsubscribe(); err != nil {
			return err
		}
	}
	return nil
}
