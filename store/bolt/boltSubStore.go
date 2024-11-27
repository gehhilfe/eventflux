package bolt

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"time"

	"github.com/hallgren/eventsourcing/core"
	"go.etcd.io/bbolt"

	fluxcore "github.com/gehhilfe/eventflux/core"
)

type boltEvent struct {
	AggregateID    string
	Version        uint64
	GlobalVersion  uint64
	FluxVersion    uint64
	Reason         string
	AggregateType  string
	OwnedByStoreId string
	Timestamp      time.Time
	Data           []byte
	Metadata       []byte // map[string]interface{}
}

type BoltSubStore struct {
	manager  *BoltStoreManager
	db       *bbolt.DB
	id       fluxcore.StoreId
	metadata fluxcore.Metadata
}

func (s *BoltSubStore) Id() fluxcore.StoreId {
	return s.id
}

func (s *BoltSubStore) Metadata() fluxcore.Metadata {
	return s.metadata
}

func (s *BoltSubStore) globalbucket() []byte {
	return []byte(fmt.Sprint("global_", s.id.String()))
}

func (s *BoltSubStore) fluxbucket() []byte {
	return []byte(fluxBucketName)
}

func (s *BoltSubStore) initialize() error {
	// Create global table
	return s.db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(s.globalbucket())
		return err
	})
}

// Save an aggregate (its events)
func (s *BoltSubStore) Save(events []core.Event) error {
	// Return if there is no events to save
	if len(events) == 0 {
		return nil
	}

	// get bucket name from first event
	aggregateType := events[0].AggregateType
	aggregateID := events[0].AggregateID
	bucketRef := s.bucketRef(aggregateType, aggregateID)

	tx, err := s.db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	evBucket := tx.Bucket(bucketRef)
	if evBucket == nil {
		// Ensure that we have a bucket named events_aggregateType_aggregateID for the given aggregate
		evBucket, err = tx.CreateBucketIfNotExists(bucketRef)
		if err != nil {
			return errors.New("could not create aggregate events bucket")
		}
	}

	currentVersion := uint64(0)
	cursor := evBucket.Cursor()
	k, obj := cursor.Last()
	if k != nil {
		lastEvent := boltEvent{}
		err := json.Unmarshal(obj, &lastEvent)
		if err != nil {
			return fmt.Errorf("could not serialize event: %v", err)
		}
		currentVersion = lastEvent.Version

		if lastEvent.OwnedByStoreId != s.id.String() {
			return core.ErrConcurrency
		}
	}

	// Make sure no other has saved event to the same aggregate concurrently
	if core.Version(currentVersion)+1 != events[0].Version {
		return core.ErrConcurrency
	}

	fluxBucket := tx.Bucket(s.fluxbucket())
	if fluxBucket == nil {
		return errors.New("flux bucket not found")
	}

	globalBucket := tx.Bucket(s.globalbucket())
	if globalBucket == nil {
		return errors.New("global bucket not found")
	}

	dataBucket := tx.Bucket([]byte(dataBucketName))
	if dataBucket == nil {
		return errors.New("data bucket not found")
	}

	var globalSequence uint64
	var fluxBucketSequence uint64
	fluxEvents := make([]fluxcore.Event, 0, len(events))
	for i, event := range events {
		sequence, err := evBucket.NextSequence()
		if err != nil {
			return fmt.Errorf("could not get sequence for %#v", string(bucketRef))
		}

		// We need to establish a global event order that spans over all buckets. This is so that we can be
		// able to play the event (or send) them in the order that they was entered into this database.
		// The global sequence bucket contains an ordered line of pointer to all events on the form bucket_name:seq_num
		globalSequence, err = globalBucket.NextSequence()
		if err != nil {
			return errors.New("could not get next sequence for global bucket")
		}

		// We also need to keep track of the order of the events in the flux bucket, which also includes events from other stores
		fluxBucketSequence, err = fluxBucket.NextSequence()
		if err != nil {
			return errors.New("could not get next sequence for flux bucket")
		}

		// build the internal bolt event
		bEvent := boltEvent{
			AggregateID:    event.AggregateID,
			AggregateType:  event.AggregateType,
			Version:        uint64(event.Version),
			GlobalVersion:  globalSequence,
			FluxVersion:    fluxBucketSequence,
			Reason:         event.Reason,
			Timestamp:      event.Timestamp,
			Metadata:       event.Metadata,
			Data:           event.Data,
			OwnedByStoreId: s.id.String(),
		}

		value, err := json.Marshal(bEvent)
		if err != nil {
			return fmt.Errorf("could not serialize event: %v", err)
		}

		digest := sha256.Sum256(value)

		err = evBucket.Put(itob(sequence), digest[:])
		if err != nil {
			return fmt.Errorf("could not save event %#v in bucket", event)
		}
		err = globalBucket.Put(itob(globalSequence), digest[:])
		if err != nil {
			return fmt.Errorf("could not save global sequence pointer for %#v", string(bucketRef))
		}
		err = fluxBucket.Put(itob(fluxBucketSequence), digest[:])
		if err != nil {
			return fmt.Errorf("could not save flux sequence pointer for %#v", string(bucketRef))
		}
		err = dataBucket.Put(digest[:], value)
		if err != nil {
			return fmt.Errorf("could not save data for %#v", string(bucketRef))
		}

		// override the event in the slice exposing the GlobalVersion to the caller
		events[i].GlobalVersion = core.Version(globalSequence)
		fluxEvents = append(fluxEvents, fluxcore.Event{
			Event:         events[i],
			StoreId:       s.id,
			StoreMetadata: s.metadata,
			FluxVersion:   core.Version(fluxBucketSequence),
		})
	}
	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("could not commit transaction: %v", err)
	}
	s.manager.commited(s, fluxEvents)
	return nil
}

// Get aggregate events
func (s *BoltSubStore) Get(ctx context.Context, id string, aggregateType string, afterVersion core.Version) (core.Iterator, error) {
	tx, err := s.db.Begin(false)
	if err != nil {
		return nil, err
	}
	bucket := tx.Bucket(s.bucketRef(aggregateType, id))
	if bucket == nil {
		tx.Rollback()
		// no aggregate event stream
		return core.ZeroIterator(), nil
	}
	dataBucket := tx.Bucket([]byte(dataBucketName))
	if dataBucket == nil {
		tx.Rollback()
		return nil, errors.New("data bucket not found")
	}
	cursor := bucket.Cursor()

	return func(yield func(core.Event, error) bool) {
		defer tx.Rollback()
		for _, digest := cursor.Seek(position(afterVersion)); digest != nil; _, digest = cursor.Next() {
			data := dataBucket.Get(digest)
			bEvent := boltEvent{}
			err := json.Unmarshal(data, &bEvent)
			if err != nil {
				yield(core.Event{}, fmt.Errorf("could not deserialize event: %v", err))
				return
			}

			event := core.Event{
				AggregateID:   bEvent.AggregateID,
				AggregateType: bEvent.AggregateType,
				Version:       core.Version(bEvent.Version),
				GlobalVersion: core.Version(bEvent.GlobalVersion),
				Timestamp:     bEvent.Timestamp,
				Metadata:      bEvent.Metadata,
				Data:          bEvent.Data,
				Reason:        bEvent.Reason,
			}

			if !yield(event, nil) {
				// Close the read transaction
				return
			}
		}
	}, nil
}

// All iterate over event in GlobalEvents order
func (s *BoltSubStore) All(start core.Version) (iter.Seq[core.Event], error) {
	tx, err := s.db.Begin(false)
	if err != nil {
		return nil, err
	}

	globalBucket := tx.Bucket(s.globalbucket())
	if globalBucket == nil {
		return func(yield func(core.Event) bool) {}, nil
	}

	dataBucket := tx.Bucket([]byte(dataBucketName))
	if dataBucket == nil {
		tx.Rollback()
		return nil, errors.New("data bucket not found")
	}

	cursor := globalBucket.Cursor()
	return func(yield func(core.Event) bool) {
		defer tx.Rollback()
		for k, digest := cursor.Seek(position(start)); k != nil; k, digest = cursor.Next() {
			data := dataBucket.Get(digest)
			bEvent := boltEvent{}
			err := json.Unmarshal(data, &bEvent)
			if err != nil {
				return
			}
			event := core.Event{
				AggregateID:   bEvent.AggregateID,
				AggregateType: bEvent.AggregateType,
				Version:       core.Version(bEvent.Version),
				GlobalVersion: core.Version(bEvent.GlobalVersion),
				Reason:        bEvent.Reason,
				Timestamp:     bEvent.Timestamp,
				Data:          bEvent.Data,
				Metadata:      bEvent.Metadata,
			}
			if !yield(event) {
				return
			}
		}
	}, nil
}

func (s *BoltSubStore) LastVersion() core.Version {
	tx, err := s.db.Begin(false)
	if err != nil {
		return 0
	}
	defer tx.Rollback()

	globalBucket := tx.Bucket(s.globalbucket())
	seq := globalBucket.Sequence()
	return core.Version(seq)
}

func (s *BoltSubStore) UpdateMetadata(metadata map[string]string) error {
	tx, err := s.db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	bucket := tx.Bucket([]byte(globalStoreBucketName))
	if bucket == nil {
		return errors.New("could not find global bucket")
	}

	metadataJson, err := json.Marshal(metadata)
	if err != nil {
		return errors.New("could not serialize metadata")
	}

	err = bucket.Put(s.id[:], metadataJson)
	if err != nil {
		return errors.New("could not save metadata")
	}

	return tx.Commit()
}

func (s *BoltSubStore) Append(event core.Event) error {
	tx, err := s.db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	globalBucket := tx.Bucket(s.globalbucket())
	if globalBucket == nil {
		return errors.New("could not find global bucket")
	}

	dataBucket := tx.Bucket([]byte(dataBucketName))
	if dataBucket == nil {
		return errors.New("could not find data bucket")
	}

	fluxBucket := tx.Bucket(s.fluxbucket())
	if fluxBucket == nil {
		return errors.New("flux bucket not found")
	}

	curGlobal := core.Version(globalBucket.Sequence())

	// Check if the event is the next in the global sequence
	if event.GlobalVersion < curGlobal+1 {
		return fluxcore.ErrEventExists
	} else if event.GlobalVersion > curGlobal+1 {
		return &fluxcore.EventOutOfOrderError{
			StoreId:  s.id,
			Expected: curGlobal + 1,
			Actual:   event.GlobalVersion,
		}
	}

	bucket := tx.Bucket(s.bucketRef(event.AggregateType, event.AggregateID))
	if bucket == nil && event.Version == 1 {
		bucket, err = tx.CreateBucket(s.bucketRef(event.AggregateType, event.AggregateID))
		if err != nil {
			return errors.New("could not create aggregate bucket")
		}
	} else if bucket == nil {
		return errors.New("could not find aggregate bucket")
	}

	cur := core.Version(bucket.Sequence())
	if event.Version < cur+1 {
		return fluxcore.ErrEventExists
	} else if event.Version > cur+1 {
		return &fluxcore.EventOutOfOrderError{
			StoreId:  s.id,
			Expected: curGlobal + 1,
			Actual:   event.GlobalVersion,
		}
	}

	cursor := bucket.Cursor()
	k, obj := cursor.Last()
	if k != nil {
		lastEvent := boltEvent{}
		err := json.Unmarshal(obj, &lastEvent)
		if err != nil {
			return fmt.Errorf("could not serialize event: %v", err)
		}

		if lastEvent.OwnedByStoreId != s.id.String() {
			return core.ErrConcurrency
		}
	}

	bucketSequence, err := bucket.NextSequence()
	if err != nil {
		return errors.New("could not get sequence")
	}

	globalSequence, err := globalBucket.NextSequence()
	if err != nil {
		return errors.New("could not get global sequence")
	}

	fluxBucketSequence, err := fluxBucket.NextSequence()
	if err != nil {
		return errors.New("could not get flux sequence")
	}

	boltEvent := boltEvent{
		AggregateID:    event.AggregateID,
		Version:        bucketSequence,
		GlobalVersion:  globalSequence,
		FluxVersion:    fluxBucketSequence,
		Reason:         event.Reason,
		AggregateType:  event.AggregateType,
		OwnedByStoreId: s.id.String(),
		Timestamp:      event.Timestamp,
		Data:           event.Data,
		Metadata:       event.Metadata,
	}

	value, err := json.Marshal(boltEvent)
	if err != nil {
		return errors.New("could not serialize event")
	}

	digest := sha256.Sum256(value)

	err = bucket.Put(itob(bucketSequence), digest[:])
	if err != nil {
		return errors.New("could not save event")
	}

	err = globalBucket.Put(itob(globalSequence), digest[:])
	if err != nil {
		return errors.New("could not save global sequence")
	}

	err = fluxBucket.Put(itob(fluxBucketSequence), digest[:])
	if err != nil {
		return errors.New("could not save flux sequence")
	}

	err = dataBucket.Put(digest[:], value)
	if err != nil {
		return errors.New("could not save data")
	}

	return tx.Commit()
}

// bucketRef return the reference where to store and fetch events
func (s *BoltSubStore) bucketRef(aggregateType, aggregateID string) []byte {
	return []byte(aggregateType + "_" + aggregateID)
}

// calculate the correct posiotion and convert to bbolt key type
func position(p core.Version) []byte {
	return itob(uint64(p + 1))
}

// itob returns an 8-byte big endian representation of v.
func itob(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}
