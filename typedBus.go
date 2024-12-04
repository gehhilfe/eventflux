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
	CommittedHandler     func(message *MessageCommittedEvent, metadata fluxcore.Metadata) error
	RequestResyncHandler func(message *MessageRequestResync, metadata fluxcore.Metadata) error
	ResyncEventsHandler  func(message *MessageResyncEvents, metadata fluxcore.Metadata) error
	HeartBeatHandler     func(message *MessageHeartBeat, metadata fluxcore.Metadata) error
}

func (t *typedMessageHandler) Committed(message *MessageCommittedEvent, metadata fluxcore.Metadata) error {
	return t.CommittedHandler(message, metadata)
}

func (t *typedMessageHandler) RequestResync(message *MessageRequestResync, metadata fluxcore.Metadata) error {
	return t.RequestResyncHandler(message, metadata)
}

func (t *typedMessageHandler) ResyncEvents(message *MessageResyncEvents, metadata fluxcore.Metadata) error {
	return t.ResyncEventsHandler(message, metadata)
}

func (t *typedMessageHandler) HeartBeat(message *MessageHeartBeat, metadata fluxcore.Metadata) error {
	return t.HeartBeatHandler(message, metadata)
}

type messageHandler interface {
	Committed(message *MessageCommittedEvent, metadata fluxcore.Metadata) error
	RequestResync(message *MessageRequestResync, metadata fluxcore.Metadata) error
	ResyncEvents(message *MessageResyncEvents, metadata fluxcore.Metadata) error
	HeartBeat(message *MessageHeartBeat, metadata fluxcore.Metadata) error
}

func (b *typedMessageBus) Subscribe(
	handler messageHandler,
) (fluxcore.Unsubscriber, error) {
	unsuber := &unsubscriber{
		subscribers: make([]fluxcore.Unsubscriber, 0, 4),
	}

	subCommited, err := b.bus.Subscribe((&MessageCommittedEvent{}).Type(), func(message []byte, metadata fluxcore.Metadata) error {
		var msg MessageCommittedEvent
		if err := json.Unmarshal(message, &msg); err != nil {
			return err
		}
		return handler.Committed(&msg, metadata)
	})
	if err != nil {
		return nil, err
	}
	unsuber.subscribers = append(unsuber.subscribers, subCommited)

	subResync, err := b.bus.Subscribe((&MessageRequestResync{}).Type(), func(message []byte, metadata fluxcore.Metadata) error {
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

	subResyncEvents, err := b.bus.Subscribe((&MessageResyncEvents{}).Type(), func(message []byte, metadata fluxcore.Metadata) error {
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

	subHeartBeat, err := b.bus.Subscribe((&MessageHeartBeat{}).Type(), func(message []byte, metadata fluxcore.Metadata) error {
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
