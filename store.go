package eventflux

import (
	"errors"
	"fmt"
	"iter"
	"log/slog"
	"strconv"
	"time"

	"github.com/hallgren/eventsourcing/core"

	"github.com/gehhilfe/eventflux/bus"
	fluxcore "github.com/gehhilfe/eventflux/core"
)

type Stores struct {
	manager fluxcore.StoreManager
	bus     *typedMessageBus

	logger *slog.Logger
}

type Option func(*Stores)

func NewStores(
	manager fluxcore.StoreManager,
	mb fluxcore.MessageBus,
	opts ...Option,
) (*Stores, error) {
	stores := &Stores{
		logger:  slog.Default(),
		manager: manager,
		bus:     NewTypedMessageBus(bus.NewBusLogger(mb)),
	}

	for _, opt := range opts {
		opt(stores)
	}

	stores.manager.OnCommit(func(s fluxcore.SubStore, events []fluxcore.Event) {
		if v, ok := s.Metadata()["type"]; ok && v == "local" {
			for _, e := range events {
				stores.bus.Publish(&MessageCommitedEvent{
					MessageBaseEvent: FromSubStore(s),
					Event:            e.Event,
				})
			}
		}
	})

	go func() {
		t := time.NewTicker(5 * time.Second)
		for range t.C {
			for s := range stores.manager.List(fluxcore.Metadata{"type": "local"}) {
				stores.bus.Publish(&MessageHeartBeat{
					MessageBaseEvent: FromSubStore(s),
					LastVersion:      s.LastVersion(),
				})
			}
		}
	}()

	stores.bus.Subscribe(typedMessageHandler{
		Commited: func(message *MessageCommitedEvent, metadata fluxcore.Metadata) error {
			return stores.commitedReceived(message)
		},
		RequestResync: func(message *MessageRequestResync, metadata fluxcore.Metadata) error {
			return stores.requestResyncReceived(message)
		},
		HeartBeat: func(message *MessageHeartBeat, metadata fluxcore.Metadata) error {
			return stores.heartBeatReceived(message)
		},
		ResyncEvents: func(message *MessageResyncEvents, metadata fluxcore.Metadata) error {
			return stores.resyncEventsReceived(message)
		},
	})

	return stores, nil
}

func (s *Stores) commitedReceived(m *MessageCommitedEvent) error {
	// Get the store
	store, err := s.manager.Get(m.StoreId)
	if errors.Is(err, fluxcore.ErrStoreNotFound) {
		metadata := fluxcore.Metadata{"type": "remote"}
		for k, v := range m.StoreMetadata {
			if k != "type" {
				metadata[k] = v
			}
		}
		store, err = s.manager.Create(m.StoreId, metadata)
		if err != nil {
			return fmt.Errorf("failed to create remote store: %w", err)
		}
	} else if err != nil {
		return fmt.Errorf("failed to get store: %w", err)
	}

	// Ignore if not a remote store
	if store.Metadata()["type"] != "remote" {
		return nil
	}

	err = store.Append(m.Event)
	ooo := &fluxcore.EventOutOfOrderError{}
	if errors.As(err, &ooo) {
		s.logger.Warn("event out of order", slog.Any("store", ooo.StoreId), slog.Any("expected", ooo.Expected), slog.Any("actual", ooo.Actual))
		// Request resync
		s.bus.Publish(&MessageRequestResync{
			MessageBaseEvent: FromSubStore(store),
			From:             ooo.Expected - 1, // Request from the last known version
		})
	}

	return nil
}

func chunk[E any](seq iter.Seq[E], size int) iter.Seq[[]E] {
	return func(yield func([]E) bool) {
		var chunk []E
		for e := range seq {
			chunk = append(chunk, e)
			if len(chunk) == size {
				if !yield(chunk) {
					return
				}
				chunk = nil
			}
		}
		if len(chunk) > 0 {
			yield(chunk)
		}
	}
}

func (s *Stores) requestResyncReceived(m *MessageRequestResync) error {
	store, err := s.manager.Get(m.StoreId)
	if errors.Is(err, fluxcore.ErrStoreNotFound) {
		return nil
	} else if err != nil {
		return fmt.Errorf("failed to get store: %w", err)
	}

	// Ignore if not a local store
	if store.Metadata()["type"] != "local" {
		return nil
	}

	// Get all events from the requested version
	iterator, err := store.All(m.From)
	if err != nil {
		return fmt.Errorf("failed to get all events: %w", err)
	}
	batchSize := 100

	for events := range chunk(iter.Seq[core.Event](iterator), batchSize) {
		s.bus.Publish(&MessageResyncEvents{
			MessageBaseEvent: FromSubStore(store),
			Events:           events,
		})
	}

	return nil
}

func (s *Stores) resyncEventsReceived(m *MessageResyncEvents) error {
	store, err := s.manager.Get(m.StoreId)
	if errors.Is(err, fluxcore.ErrStoreNotFound) {
		metadata := fluxcore.Metadata{"type": "remote"}
		for k, v := range m.StoreMetadata {
			if k != "type" {
				metadata[k] = v
			}
		}
		store, err = s.manager.Create(m.StoreId, metadata)
		if err != nil {
			return nil
		}
	} else if err != nil {
		return nil
	}

	if store.Metadata()["type"] != "remote" {
		return nil
	}

	for _, e := range m.Events {
		err := store.Append(e)
		if err != nil && !errors.Is(err, fluxcore.ErrEventExists) {
			return fmt.Errorf("failed to append event: %w", err)
		}
	}

	return nil
}

func (s *Stores) heartBeatReceived(m *MessageHeartBeat) error {
	if m.LastVersion == 0 {
		return nil
	}

	store, err := s.manager.Get(m.StoreId)
	if errors.Is(err, fluxcore.ErrStoreNotFound) {
		// create store
		metadata := fluxcore.Metadata{"type": "remote"}
		for k, v := range m.StoreMetadata {
			if k != "type" {
				metadata[k] = v
			}
		}
		store, err = s.manager.Create(m.StoreId, metadata)
		if err != nil {
			return nil
		}
		s.bus.Publish(&MessageRequestResync{
			MessageBaseEvent: m.MessageBaseEvent,
			From:             store.LastVersion(),
		})
		return nil
	} else if err != nil {
		return nil
	}

	if store.Metadata()["type"] != "remote" {
		return nil
	}

	// Update metedata
	metadata := make(fluxcore.Metadata, len(store.Metadata()))
	for k, v := range store.Metadata() {
		metadata[k] = v
	}
	metadata["last_heartbeat_at"] = time.Now().Format(time.RFC3339)
	metadata["last_heartbeat_version"] = strconv.FormatUint(uint64(m.LastVersion), 10)
	if err := store.UpdateMetadata(metadata); err != nil {
		return nil
	}

	if m.LastVersion > store.LastVersion() {
		s.bus.Publish(&MessageRequestResync{
			MessageBaseEvent: FromSubStore(store),
			From:             store.LastVersion(),
		})
	}

	return nil
}

func WithLogger(logger *slog.Logger) Option {
	return func(s *Stores) {
		s.logger = logger
	}
}
