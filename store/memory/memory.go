package memory

import (
	"iter"
	"sync"

	"github.com/hallgren/eventsourcing/core"

	fluxcore "github.com/gehhilfe/eventflux/core"
)

type InMemoryStoreManager struct {
	stores          map[fluxcore.StoreId]*InMemorySubStore
	onCommitCbs     []func(fluxcore.SubStore, []fluxcore.Event)
	fluxStore       []fluxcore.Event
	nextFluxVersion core.Version

	transactionLock sync.Mutex
}

type transaction struct {
	manager   *InMemoryStoreManager
	before    core.Version
	didCommit bool
}

func (t *transaction) Commit() {
	t.didCommit = true
	t.manager.transactionLock.Unlock()
}

func (t *transaction) Rollback() {
	if t.didCommit {
		return
	}
	t.manager.nextFluxVersion = t.before
	t.manager.transactionLock.Unlock()
}

func (t *transaction) NextFluxVersion() core.Version {
	v := t.manager.nextFluxVersion
	t.manager.nextFluxVersion++
	return v
}

func NewInMemoryStoreManager() *InMemoryStoreManager {
	return &InMemoryStoreManager{
		stores:          make(map[fluxcore.StoreId]*InMemorySubStore),
		onCommitCbs:     make([]func(fluxcore.SubStore, []fluxcore.Event), 0),
		fluxStore:       make([]fluxcore.Event, 0),
		nextFluxVersion: 1,
	}
}

func (m *InMemoryStoreManager) Tx() *transaction {
	m.transactionLock.Lock()
	return &transaction{
		manager: m,
		before:  m.nextFluxVersion,
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

func (m *InMemoryStoreManager) OnCommit(cb func(fluxcore.SubStore, []fluxcore.Event)) fluxcore.Unsubscriber {
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

func (m *InMemoryStoreManager) commited(s fluxcore.SubStore, events []fluxcore.Event) error {
	m.fluxStore = append(m.fluxStore, events...)

	for _, cb := range m.onCommitCbs {
		cb(s, events)
	}
	return nil
}

func (m *InMemoryStoreManager) All(start core.Version) (iter.Seq[fluxcore.Event], error) {
	return func(yield func(fluxcore.Event) bool) {
		if start >= core.Version(len(m.fluxStore)) {
			return
		}
		for _, event := range m.fluxStore[start:] {
			if !yield(event) {
				return
			}
		}
	}, nil
}
