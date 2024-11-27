package bolt

import (
	"encoding/json"
	"errors"
	"fmt"
	"iter"

	"github.com/google/uuid"
	"github.com/hallgren/eventsourcing/core"
	"go.etcd.io/bbolt"

	fluxcore "github.com/gehhilfe/eventflux/core"
)

type BoltStoreManager struct {
	db          *bbolt.DB
	onCommitCbs []func(fluxcore.SubStore, []fluxcore.Event)
}

const (
	globalStoreBucketName = "global_store"
	fluxBucketName        = "flux"
	dataBucketName        = "data"
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

		_, err = tx.CreateBucketIfNotExists([]byte(fluxBucketName))
		if err != nil {
			return err
		}

		_, err = tx.CreateBucketIfNotExists([]byte(dataBucketName))
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
		onCommitCbs: make([]func(fluxcore.SubStore, []fluxcore.Event), 0),
	}, nil
}

func (m *BoltStoreManager) List(metadata fluxcore.Metadata) iter.Seq[fluxcore.SubStore] {
	return func(yield func(fluxcore.SubStore) bool) {
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
				loadedMetadata := make(fluxcore.Metadata)
				err = json.Unmarshal(v, &loadedMetadata)
				if err != nil {
					return err
				}

				for k, v := range metadata {
					if loadedMetadata[k] != v {
						return nil
					}
				}

				subStore := &BoltSubStore{
					manager:  m,
					db:       m.db,
					id:       fluxcore.StoreId(id),
					metadata: loadedMetadata,
				}

				if !yield(subStore) {
					return nil
				}
				return nil
			})
		})
	}
}

func (m *BoltStoreManager) Create(id fluxcore.StoreId, metadata fluxcore.Metadata) (fluxcore.SubStore, error) {
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
	metadata := make(fluxcore.Metadata)
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

func (m *BoltStoreManager) All(start core.Version) (iter.Seq[fluxcore.Event], error) {
	tx, err := m.db.Begin(false)
	if err != nil {
		return nil, err
	}

	fluxBucket := tx.Bucket([]byte(fluxBucketName))
	if fluxBucket == nil {
		tx.Rollback()
		return func(yield func(fluxcore.Event) bool) {}, errors.New("flux bucket not found")
	}

	dataBucket := tx.Bucket([]byte(dataBucketName))
	if dataBucket == nil {
		tx.Rollback()
		return func(yield func(fluxcore.Event) bool) {}, errors.New("data bucket not found")
	}

	globalStoreBucket := tx.Bucket([]byte(globalStoreBucketName))
	if globalStoreBucket == nil {
		tx.Rollback()
		return func(yield func(fluxcore.Event) bool) {}, errors.New("global store bucket not found")
	}

	return func(yield func(fluxcore.Event) bool) {
		defer tx.Rollback()
		cursor := fluxBucket.Cursor()

		lookup := map[string]fluxcore.Metadata{}

		for k, digest := cursor.Seek(itob(uint64(start + 1))); k != nil; k, digest = cursor.Next() {
			data := dataBucket.Get(digest)
			if data == nil {
				return
			}

			var boltEvent boltEvent
			err := json.Unmarshal(data, &boltEvent)
			if err != nil {
				return
			}

			storeId := boltEvent.OwnedByStoreId
			if _, ok := lookup[storeId]; !ok {
				data := globalStoreBucket.Get([]byte(storeId))
				if data == nil {
					return
				}
				metadata := make(fluxcore.Metadata)
				err = json.Unmarshal(data, &metadata)
				if err != nil {
					return
				}
				lookup[storeId] = metadata
			}

			fluxEvent := fluxcore.Event{
				StoreId:       fluxcore.StoreId(uuid.MustParse(storeId)),
				StoreMetadata: lookup[storeId],
				FluxVersion:   core.Version(boltEvent.FluxVersion),
				Event: core.Event{
					AggregateID:   boltEvent.AggregateID,
					Version:       core.Version(boltEvent.Version),
					GlobalVersion: core.Version(boltEvent.GlobalVersion),
					AggregateType: boltEvent.AggregateType,
					Timestamp:     boltEvent.Timestamp,
					Reason:        boltEvent.Reason,
					Data:          boltEvent.Data,
					Metadata:      boltEvent.Metadata,
				},
			}

			if !yield(fluxEvent) {
				return
			}
		}
	}, nil
}

func (m *BoltStoreManager) OnCommit(cb func(fluxcore.SubStore, []fluxcore.Event)) fluxcore.Unsubscriber {
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

func (m *BoltStoreManager) commited(s fluxcore.SubStore, events []fluxcore.Event) error {
	for _, cb := range m.onCommitCbs {
		cb(s, events)
	}
	return nil
}
