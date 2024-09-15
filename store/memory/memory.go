package memory

import (
	"iter"

	"github.com/hallgren/eventsourcing/core"

	fluxcore "github.com/gehhilfe/eventflux/core"
)

type InMemoryStoreManager struct {
	stores      map[fluxcore.StoreId]*InMemorySubStore
	onCommitCbs []func(fluxcore.SubStore, []core.Event)
}

func NewInMemoryStoreManager() *InMemoryStoreManager {
	return &InMemoryStoreManager{
		stores:      make(map[fluxcore.StoreId]*InMemorySubStore),
		onCommitCbs: make([]func(fluxcore.SubStore, []core.Event), 0),
	}
}

func (m *InMemoryStoreManager) List(metadata map[string]string) iter.Seq[fluxcore.SubStore] {
	return func(yield func(fluxcore.SubStore) bool) {
		for _, store := range m.stores {
			for k, v := range metadata {
				if store.metadata[k] != v {
					continue
				}
			}
			if !yield(store) {
				return
			}
		}
	}
}

func (m *InMemoryStoreManager) Get(id fluxcore.StoreId) (fluxcore.SubStore, error) {
	store, ok := m.stores[id]
	if !ok {
		return nil, fluxcore.ErrStoreNotFound
	}
	return store, nil
}

func (m *InMemoryStoreManager) Create(id fluxcore.StoreId, metadata map[string]string) (fluxcore.SubStore, error) {
	if s, ok := m.stores[id]; ok {
		return s, nil
	}

	store := &InMemorySubStore{
		manager:    m,
		id:         id,
		metadata:   metadata,
		aggregates: make(map[string]*aggregateBucket),
	}

	m.stores[id] = store
	return store, nil
}

func (m *InMemoryStoreManager) OnCommit(cb func(fluxcore.SubStore, []core.Event)) fluxcore.Unsubscriber {
	m.onCommitCbs = append(m.onCommitCbs, cb)
	return fluxcore.UnsubscribeFunc(func() error {
		for i, c := range m.onCommitCbs {
			if &c == &cb {
				m.onCommitCbs = append(m.onCommitCbs[:i], m.onCommitCbs[i+1:]...)
				return nil
			}
		}
		return nil
	})
}

func (m *InMemoryStoreManager) commited(s fluxcore.SubStore, events []core.Event) error {
	for _, cb := range m.onCommitCbs {
		cb(s, events)
	}
	return nil
}

func (m *InMemoryStoreManager) All(starts map[fluxcore.StoreId]core.Version) (iter.Seq[fluxcore.StoreEvent], error) {
	return func(yield func(fluxcore.StoreEvent) bool) {
		for _, store := range m.stores {
			pos, ok := starts[store.id]
			if !ok {
				pos = 0
			}

			iter, _ := store.All(pos)
			for e := range iter {
				if !yield(fluxcore.StoreEvent{
					StoreId: store.id,
					Event:   e,
				}) {
					return
				}
			}
		}

	}, nil
}
