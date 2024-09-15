package eventflux

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/gehhilfe/eventflux/bus"
	fluxcore "github.com/gehhilfe/eventflux/core"
	"github.com/gehhilfe/eventflux/store/memory"
	"github.com/google/uuid"
	"github.com/hallgren/eventsourcing/core"
)

func TestEarlyResyncResponse(t *testing.T) {
	smA := memory.NewInMemoryStoreManager()

	mb := bus.NewInMemoryMessageBus()

	// Create store A
	a, _ := NewStores(
		smA,
		mb,
	)

	a.localStore.Append(core.Event{
		AggregateID:   "1",
		Version:       1,
		GlobalVersion: 1,
		AggregateType: "test",
		Timestamp:     time.Now(),
		Reason:        "TEST",
		Data:          []byte{},
		Metadata:      []byte{},
	})

	typed := NewTypedMessageBus(mb)

	var wg sync.WaitGroup
	typed.Subscribe(typedMessageHandler{
		Commited: func(message *MessageCommitedEvent, metadata map[string]string) error {
			return nil
		},
		RequestResync: func(message *MessageRequestResync, metadata map[string]string) error {
			defer wg.Done()
			fmt.Println("RequestResync")
			return nil
		},
		ResyncEvents: func(message *MessageResyncEvents, metadata map[string]string) error {
			defer wg.Done()
			if len(message.Events) != 1 {
				t.Fatalf("Expected 1 event, got %d", len(message.Events))
			}
			return nil
		},
		HeartBeat: func(message *MessageHeartBeat, metadata map[string]string) error {
			return nil
		},
	})
	wg.Add(2)

	typed.Publish(&MessageRequestResync{
		MessageBaseEvent: FromSubStore(a.localStore),
		From:             core.Version(0),
	})

	wg.Wait()
}

func TestResyncSending(t *testing.T) {
	smA := memory.NewInMemoryStoreManager()

	mb := bus.NewInMemoryMessageBus()

	// Create store A
	_, _ = NewStores(
		smA,
		mb,
	)

	typed := NewTypedMessageBus(mb)

	var wg sync.WaitGroup
	typed.Subscribe(typedMessageHandler{
		Commited: func(message *MessageCommitedEvent, metadata map[string]string) error {
			return nil
		},
		RequestResync: func(message *MessageRequestResync, metadata map[string]string) error {
			defer wg.Done()
			if message.From != 0 {
				t.Fatalf("Expected from 0, got %d", message.From)
			}
			return nil
		},
		ResyncEvents: func(message *MessageResyncEvents, metadata map[string]string) error {
			return nil
		},
		HeartBeat: func(message *MessageHeartBeat, metadata map[string]string) error {
			return nil
		},
	})
	wg.Add(1)

	typed.Publish(&MessageCommitedEvent{
		MessageBaseEvent: MessageBaseEvent{
			StoreId:       fluxcore.StoreId(uuid.New()),
			StoreMetadata: map[string]string{},
		},
		Event: core.Event{
			AggregateID:   "1",
			Version:       1,
			GlobalVersion: 10,
			AggregateType: "test",
			Timestamp:     time.Now(),
			Reason:        "TEST",
			Data:          []byte{},
			Metadata:      []byte{},
		},
	})

	wg.Wait()
}

func TestResyncSendingDueToHeartBeat(t *testing.T) {
	smA := memory.NewInMemoryStoreManager()

	mb := bus.NewInMemoryMessageBus()

	// Create store A
	_, _ = NewStores(
		smA,
		mb,
	)

	typed := NewTypedMessageBus(mb)

	var wg sync.WaitGroup
	typed.Subscribe(typedMessageHandler{
		Commited: func(message *MessageCommitedEvent, metadata map[string]string) error {
			return nil
		},
		RequestResync: func(message *MessageRequestResync, metadata map[string]string) error {
			defer wg.Done()
			if message.From != 0 {
				t.Fatalf("Expected from 0, got %d", message.From)
			}
			return nil
		},
		ResyncEvents: func(message *MessageResyncEvents, metadata map[string]string) error {
			return nil
		},
		HeartBeat: func(message *MessageHeartBeat, metadata map[string]string) error {
			return nil
		},
	})
	wg.Add(1)

	typed.Publish(&MessageHeartBeat{
		MessageBaseEvent: MessageBaseEvent{
			StoreId:       fluxcore.StoreId(uuid.New()),
			StoreMetadata: map[string]string{},
		},
		LastVersion: 100,
	})

	wg.Wait()
}

func TestResyncSending2(t *testing.T) {
	smA := memory.NewInMemoryStoreManager()

	mb := bus.NewInMemoryMessageBus()

	// Create store A
	_, _ = NewStores(
		smA,
		mb,
	)

	typed := NewTypedMessageBus(mb)

	var wg sync.WaitGroup
	typed.Subscribe(typedMessageHandler{
		Commited: func(message *MessageCommitedEvent, metadata map[string]string) error {
			return nil
		},
		RequestResync: func(message *MessageRequestResync, metadata map[string]string) error {
			defer wg.Done()
			if message.From != 3 {
				t.Fatalf("Expected from 3, got %d", message.From)
			}
			return nil
		},
		ResyncEvents: func(message *MessageResyncEvents, metadata map[string]string) error {
			return nil
		},
		HeartBeat: func(message *MessageHeartBeat, metadata map[string]string) error {
			return nil
		},
	})
	wg.Add(1)

	storeId := fluxcore.StoreId(uuid.New())

	typed.Publish(&MessageCommitedEvent{
		MessageBaseEvent: MessageBaseEvent{
			StoreId:       storeId,
			StoreMetadata: map[string]string{},
		},
		Event: core.Event{
			AggregateID:   "1",
			Version:       1,
			GlobalVersion: 1,
			AggregateType: "test",
			Timestamp:     time.Now(),
			Reason:        "TEST",
			Data:          []byte{},
			Metadata:      []byte{},
		},
	})

	typed.Publish(&MessageCommitedEvent{
		MessageBaseEvent: MessageBaseEvent{
			StoreId:       storeId,
			StoreMetadata: map[string]string{},
		},
		Event: core.Event{
			AggregateID:   "1",
			Version:       2,
			GlobalVersion: 2,
			AggregateType: "test",
			Timestamp:     time.Now(),
			Reason:        "TEST",
			Data:          []byte{},
			Metadata:      []byte{},
		},
	})

	typed.Publish(&MessageCommitedEvent{
		MessageBaseEvent: MessageBaseEvent{
			StoreId:       storeId,
			StoreMetadata: map[string]string{},
		},
		Event: core.Event{
			AggregateID:   "1",
			Version:       3,
			GlobalVersion: 3,
			AggregateType: "test",
			Timestamp:     time.Now(),
			Reason:        "TEST",
			Data:          []byte{},
			Metadata:      []byte{},
		},
	})

	typed.Publish(&MessageCommitedEvent{
		MessageBaseEvent: MessageBaseEvent{
			StoreId:       storeId,
			StoreMetadata: map[string]string{},
		},
		Event: core.Event{
			AggregateID:   "1",
			Version:       10,
			GlobalVersion: 10,
			AggregateType: "test",
			Timestamp:     time.Now(),
			Reason:        "TEST",
			Data:          []byte{},
			Metadata:      []byte{},
		},
	})

	wg.Wait()
}

func TestCommittedSending(t *testing.T) {
	smA := memory.NewInMemoryStoreManager()

	mb := bus.NewInMemoryMessageBus()

	// Create store A
	a, _ := NewStores(
		smA,
		mb,
	)

	typed := NewTypedMessageBus(mb)

	var wg sync.WaitGroup
	typed.Subscribe(typedMessageHandler{
		Commited: func(message *MessageCommitedEvent, metadata map[string]string) error {
			defer wg.Done()
			return nil
		},
		RequestResync: func(message *MessageRequestResync, metadata map[string]string) error {
			return nil
		},
		ResyncEvents: func(message *MessageResyncEvents, metadata map[string]string) error {
			return nil
		},
		HeartBeat: func(message *MessageHeartBeat, metadata map[string]string) error {
			return nil
		},
	})
	wg.Add(1)

	a.localStore.Save([]core.Event{
		{
			AggregateID:   "1",
			Version:       1,
			GlobalVersion: 1,
			AggregateType: "test",
			Timestamp:     time.Now(),
			Reason:        "TEST",
			Data:          []byte{},
			Metadata:      []byte{},
		},
	})

	wg.Wait()
}

func TestHBSending(t *testing.T) {
	smA := memory.NewInMemoryStoreManager()

	mb := bus.NewInMemoryMessageBus()

	// Create store A
	a, _ := NewStores(
		smA,
		mb,
	)

	typed := NewTypedMessageBus(mb)

	var wg sync.WaitGroup
	typed.Subscribe(typedMessageHandler{
		Commited: func(message *MessageCommitedEvent, metadata map[string]string) error {
			return nil
		},
		RequestResync: func(message *MessageRequestResync, metadata map[string]string) error {
			return nil
		},
		ResyncEvents: func(message *MessageResyncEvents, metadata map[string]string) error {
			return nil
		},
		HeartBeat: func(message *MessageHeartBeat, metadata map[string]string) error {
			defer wg.Done()
			if message.LastVersion != 1 {
				t.Fatalf("Expected last version 1, got %d", message.LastVersion)
			}
			return nil
		},
	})
	wg.Add(1)

	a.localStore.Save([]core.Event{
		{
			AggregateID:   "1",
			Version:       1,
			GlobalVersion: 1,
			AggregateType: "test",
			Timestamp:     time.Now(),
			Reason:        "TEST",
			Data:          []byte{},
			Metadata:      []byte{},
		},
	})

	wg.Wait()
}
