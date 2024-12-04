package eventflux

import (
	"github.com/hallgren/eventsourcing/core"

	fluxcore "github.com/gehhilfe/eventflux/core"
)

type MessageBaseEvent struct {
	StoreId       fluxcore.StoreId
	StoreMetadata fluxcore.Metadata
}

func FromSubStore(subStore fluxcore.SubStore) MessageBaseEvent {
	// Create metadata map without type key
	metadata := make(fluxcore.Metadata)
	for k, v := range subStore.Metadata() {
		if k != "type" {
			metadata[k] = v
		}
	}
	return MessageBaseEvent{
		StoreId:       subStore.Id(),
		StoreMetadata: metadata,
	}
}

type MessageCommittedEvent struct {
	MessageBaseEvent
	core.Event
}

type MessageRequestResync struct {
	MessageBaseEvent
	From core.Version
}

type MessageResyncEvents struct {
	MessageBaseEvent
	Events []core.Event
}

type MessageHeartBeat struct {
	MessageBaseEvent
	LastVersion core.Version
}

func (m *MessageCommittedEvent) Type() string {
	return "committed-event.v1"
}

func (m *MessageRequestResync) Type() string {
	return "request-resync.v1"
}

func (m *MessageResyncEvents) Type() string {
	return "resync-events.v1"
}

func (m *MessageHeartBeat) Type() string {
	return "heart-beat.v1"
}
