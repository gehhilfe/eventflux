package bolt

import (
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/hallgren/eventsourcing/core"
	"go.etcd.io/bbolt"

	fluxcore "github.com/gehhilfe/eventflux/core"
)

type BoltStoreManager struct {
	db          *bbolt.DB
	onCommitCbs []func(fluxcore.SubStore, []core.Event)
}

const (
	globalStoreBucketName = "global_store"
)

// NewBoltStoreManager creates a new EdgeStoreManager
func NewBoltStoreManager(path string) (*BoltStoreManager, error) {
	db, err := bbolt.Open(path, 0600, nil)
	if err != nil {
		return nil, err
	}

	err = db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(globalStoreBucketName))
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return &BoltStoreManager{
		db:          db,
		onCommitCbs: make([]func(fluxcore.SubStore, []core.Event), 0),
	}, nil
}

func (m *BoltStoreManager) List(metadata map[string]string) <-chan fluxcore.SubStore {
	ch := make(chan fluxcore.SubStore)
	go func() {
		m.db.View(func(tx *bbolt.Tx) error {
			b := tx.Bucket([]byte(globalStoreBucketName))
			if b == nil {
				return nil
			}
			return b.ForEach(func(k, v []byte) error {
				id, err := uuid.Parse(string(k))
				if err != nil {
					return err
				}
				loadedMetadata := make(map[string]string)
				err = json.Unmarshal(v, &loadedMetadata)
				if err != nil {
					return err
				}

				for k, v := range metadata {
					if loadedMetadata[k] != v {
						return nil
					}
				}

				ch <- &BoltSubStore{
					manager:  m,
					db:       m.db,
					id:       fluxcore.StoreId(id),
					metadata: loadedMetadata,
				}
				return nil
			})
		})
		close(ch)
	}()
	return ch
}

func (m *BoltStoreManager) Create(id fluxcore.StoreId, metadata map[string]string) (fluxcore.SubStore, error) {
	err := m.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(globalStoreBucketName))
		data, err := json.Marshal(metadata)
		if err != nil {
			return err
		}
		return b.Put([]byte(id.String()), data)
	})
	if err != nil {
		return nil, err
	}

	s := &BoltSubStore{
		manager:  m,
		db:       m.db,
		id:       id,
		metadata: metadata,
	}
	return s, s.initialize()
}

func (m *BoltStoreManager) Get(id fluxcore.StoreId) (fluxcore.SubStore, error) {
	tx, err := m.db.Begin(false)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	b := tx.Bucket([]byte(globalStoreBucketName))
	data := b.Get([]byte(id.String()))
	if data == nil {
		return nil, fluxcore.ErrStoreNotFound
	}
	metadata := make(map[string]string)
	err = json.Unmarshal(data, &metadata)
	if err != nil {
		return nil, fmt.Errorf("could not unmarshal metadata: %v", err)
	}
	return &BoltSubStore{
		manager:  m,
		db:       m.db,
		id:       id,
		metadata: metadata,
	}, nil
}

func (m *BoltStoreManager) OnCommit(cb func(fluxcore.SubStore, []core.Event)) fluxcore.Unsubscriber {
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

func (m *BoltStoreManager) commited(s fluxcore.SubStore, events []core.Event) error {
	for _, cb := range m.onCommitCbs {
		cb(s, events)
	}
	return nil
}
