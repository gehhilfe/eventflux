package eventflux

import (
	"testing"

	"github.com/gehhilfe/eventflux/bus"
)

func TestTypesMessageBusWithInMemoryBus(t *testing.T) {
	shardA := bus.NewInMemoryMessageBus()

	tm := NewTypedMessageBus(shardA)

	countComitted := 0
	countResync := 0
	countHeartBeat := 0

	tm.Subscribe(typedMessageHandler{
		Commited: func(message *MessageCommitedEvent, metadata map[string]string) error {
			countComitted++
			return nil
		},
		RequestResync: func(message *MessageRequestResync, metadata map[string]string) error {
			countResync++
			return nil
		},
		HeartBeat: func(message *MessageHeartBeat, metadata map[string]string) error {
			countHeartBeat++
			return nil
		},
	})

	tm.Publish(&MessageCommitedEvent{})
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
