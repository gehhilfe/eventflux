package memory

import (
	"context"
	"errors"
	"iter"
	"sync"
	"sync/atomic"

	"github.com/hallgren/eventsourcing/core"

	fluxcore "github.com/gehhilfe/eventflux/core"
)

type aggregateBucket struct {
	aggregateId   string
	aggregateType string
	events        []core.Event
}

type InMemorySubStore struct {
	lock sync.Mutex

	manager *InMemoryStoreManager

	id       fluxcore.StoreId
	metadata map[string]string

	aggregates map[string]*aggregateBucket

	globalVersion atomic.Uint64
	globalEvents  []core.Event
}

func (s *InMemorySubStore) Id() fluxcore.StoreId {
	return s.id
}

func (s *InMemorySubStore) Metadata() map[string]string {
	return s.metadata
}

func (s *InMemorySubStore) Save(events []core.Event) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	if len(events) == 0 {
		return nil
	}

	aggregateType := events[0].AggregateType
	aggregateID := events[0].AggregateID

	bucket, ok := s.aggregates[aggregateID]
	if !ok {
		s.aggregates[aggregateID] = &aggregateBucket{
			aggregateId:   aggregateID,
			aggregateType: aggregateType,
			events:        make([]core.Event, 0),
		}
		bucket = s.aggregates[aggregateID]
	}

	curGlobalVersion := core.Version(s.globalVersion.Load())
	curBucketVersion := core.Version(len(bucket.events))

	for i, event := range events {
		globalVersion := curGlobalVersion + core.Version(i+1)
		bucketVersion := curBucketVersion + core.Version(i+1)

		if event.Version != bucketVersion {
			return core.ErrConcurrency
		}

		event.GlobalVersion = globalVersion
		event.Version = bucketVersion

		bucket.events = append(bucket.events, event)
		s.globalEvents = append(s.globalEvents, event)

		s.globalVersion.Store(uint64(globalVersion))

		events[i].GlobalVersion = globalVersion

	}

	s.manager.commited(s, events)
	return nil
}

func (s *InMemorySubStore) Get(ctx context.Context, id string, aggregateType string, afterVersion core.Version) (core.Iterator, error) {
	bucket, ok := s.aggregates[id]
	if !ok {
		return nil, errors.New("no aggregate event stream")
	}

	return func(yield func(core.Event, error) bool) {
		for _, event := range bucket.events {
			if event.Version > afterVersion {
				if !yield(event, nil) {
					return
				}
			}
		}
	}, nil
}

func (s *InMemorySubStore) All(start core.Version) (iter.Seq[core.Event], error) {
	return func(yield func(core.Event) bool) {
		for _, event := range s.globalEvents[start:] {
			if !yield(event) {
				return
			}
		}
	}, nil
}

func (s *InMemorySubStore) Append(event core.Event) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	nextGlobalVersion := core.Version(s.globalVersion.Load() + 1)

	if event.GlobalVersion < nextGlobalVersion {
		return fluxcore.ErrEventExists
	} else if event.GlobalVersion > nextGlobalVersion {
		return &fluxcore.EventOutOfOrderError{
			StoreId:  s.id,
			Expected: nextGlobalVersion,
			Actual:   event.GlobalVersion,
		}
	}

	aggregateID := event.AggregateID
	bucket, ok := s.aggregates[aggregateID]
	if !ok {
		s.aggregates[aggregateID] = &aggregateBucket{
			aggregateId:   aggregateID,
			aggregateType: event.AggregateType,
			events:        make([]core.Event, 0),
		}
		bucket = s.aggregates[aggregateID]
	}

	curBucketVersion := core.Version(len(bucket.events))
	if event.Version < curBucketVersion+1 {
		return fluxcore.ErrEventExists
	} else if event.Version > curBucketVersion+1 {
		return &fluxcore.EventOutOfOrderError{
			StoreId:  s.id,
			Expected: nextGlobalVersion,
			Actual:   event.GlobalVersion,
		}
	}

	bucket.events = append(bucket.events, event)
	s.globalEvents = append(s.globalEvents, event)
	s.globalVersion.Store(uint64(event.GlobalVersion))
	return nil
}

func (s *InMemorySubStore) LastVersion() core.Version {
	return core.Version(s.globalVersion.Load())
}

func (s *InMemorySubStore) UpdateMetadata(metadata map[string]string) error {
	s.metadata = metadata
	return nil
}
