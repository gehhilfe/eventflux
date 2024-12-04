package eventflux

import (
	"testing"

	"github.com/gehhilfe/eventflux/bus"
	fluxcore "github.com/gehhilfe/eventflux/core"
)

func TestTypesMessageBusWithInMemoryBus(t *testing.T) {
	shardA := bus.NewInMemoryMessageBus()

	tm := NewTypedMessageBus(shardA)

	countComitted := 0
	countResync := 0
	countHeartBeat := 0

	tm.Subscribe(typedMessageHandler{
		Committed: func(message *MessageCommittedEvent, metadata fluxcore.Metadata) error {
			countComitted++
			return nil
		},
		RequestResync: func(message *MessageRequestResync, metadata fluxcore.Metadata) error {
			countResync++
			return nil
		},
		HeartBeat: func(message *MessageHeartBeat, metadata fluxcore.Metadata) error {
			countHeartBeat++
			return nil
		},
	})

	tm.Publish(&MessageCommittedEvent{})
	tm.Publish(&MessageRequestResync{})
	tm.Publish(&MessageHeartBeat{})
	tm.Publish(&MessageHeartBeat{})
	tm.Publish(&MessageHeartBeat{})

	if countComitted != 1 {
		t.Errorf("expected 1, got %d", countComitted)
	}
	if countResync != 1 {
		t.Errorf("expected 1, got %d", countResync)
	}
	if countHeartBeat != 3 {
		t.Errorf("expected 3, got %d", countHeartBeat)
	}
}
