package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"iter"

	"github.com/hallgren/eventsourcing/core"

	fluxcore "github.com/gehhilfe/eventflux/core"
)

type subStore struct {
	db       *sql.DB
	manager  *StoreManager
	pgId     uint64
	storeId  fluxcore.StoreId
	metadata fluxcore.Metadata
}

func (s *subStore) Id() fluxcore.StoreId {
	return s.storeId
}

func (s *subStore) All(start core.Version) (iter.Seq[core.Event], error) {
	res, err := s.db.Query(`
		SELECT aggregate_id, version, global_version, aggregate_type, created_at, reason, data, metadata
		FROM events
		WHERE store_id = $1 AND global_version > $2
		ORDER BY global_version ASC;
	`, s.storeId.String(), start)
	if err != nil {
		return nil, err
	}

	return func(yield func(core.Event) bool) {
		defer res.Close()

		for res.Next() {
			var e core.Event
			err := res.Scan(&e.AggregateID, &e.Version, &e.GlobalVersion, &e.AggregateType, &e.Timestamp, &e.Reason, &e.Data, &e.Metadata)
			if err != nil {
				return
			}
			if !yield(e) {
				return
			}
		}
	}, nil
}

func (s *subStore) Append(e core.Event) error {
	nextGlobalVersion := s.LastVersion() + 1
	if e.GlobalVersion < nextGlobalVersion {
		return fluxcore.ErrEventExists
	} else if e.GlobalVersion > nextGlobalVersion {
		return &fluxcore.EventOutOfOrderError{
			StoreId:  s.storeId,
			Expected: nextGlobalVersion,
			Actual:   e.GlobalVersion,
		}
	}

	res, err := s.db.Query(`
		INSERT INTO events (store_id, aggregate_id, aggregate_type, version, reason, data, metadata, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		RETURNING global_version;
	`, s.storeId.String(), e.AggregateID, e.AggregateType, e.Version, e.Reason, e.Data, e.Metadata, e.Timestamp)
	if err != nil {
		return err
	}
	defer res.Close()

	return nil
}

func (s *subStore) Get(ctx context.Context, aggregateId string, aggregateType string, version core.Version) (core.Iterator, error) {
	res, err := s.db.Query(`
		SELECT aggregate_id, version, global_version, aggregate_type, created_at, reason, data, metadata
		FROM events
		WHERE store_id = $1 AND aggregate_id = $2 AND aggregate_type = $3 AND version > $4
		ORDER BY version ASC;
	`, s.storeId.String(), aggregateId, aggregateType, version)
	if err != nil {
		return nil, err
	}

	return func(yield func(core.Event, error) bool) {
		defer res.Close()

		for res.Next() {
			var e core.Event
			err := res.Scan(
				&e.AggregateID,
				&e.Version,
				&e.GlobalVersion,
				&e.AggregateType,
				&e.Timestamp,
				&e.Reason,
				&e.Data,
				&e.Metadata,
			)
			if !yield(e, err) {
				return
			}
		}
	}, nil

}

func (s *subStore) LastVersion() core.Version {
	var version core.Version
	s.db.QueryRow(`
		SELECT last_version
		FROM stores
		WHERE store_id = $1;
	`, s.storeId.String()).Scan(&version)
	return version
}

func (s *subStore) Metadata() map[string]string {
	return s.metadata
}

func (s *subStore) Save(events []core.Event) error {
	if s.metadata["type"] != "local" {
		return fmt.Errorf("cannot save events to a remote store")
	}

	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	fluxEvents := make([]fluxcore.Event, len(events))
	for i, e := range events {
		err := func() error {
			res, err := tx.Query(`
				INSERT INTO events (store_id, aggregate_id, aggregate_type, version, reason, data, metadata, created_at)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
				RETURNING id, global_version;
			`, s.storeId.String(), e.AggregateID, e.AggregateType, e.Version, e.Reason, e.Data, e.Metadata, e.Timestamp)
			if err != nil {
				return err
			}
			defer res.Close()

			var globalVersion core.Version
			var fluxVersion core.Version
			if res.Next() {
				err = res.Scan(&fluxVersion, &globalVersion)
				if err != nil {
					return err
				}
			}

			events[i].GlobalVersion = globalVersion
			fluxEvents[i] = fluxcore.Event{
				StoreId:       s.storeId,
				StoreMetadata: s.metadata,
				FluxVersion:   fluxVersion,
				Event:         events[i],
			}
			return nil
		}()
		if err != nil {
			return err
		}
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	s.manager.commited(s, fluxEvents)
	return nil
}

func (s *subStore) UpdateMetadata(metadata map[string]string) error {
	metadataJson, err := json.Marshal(metadata)
	if err != nil {
		return err
	}

	_, err = s.db.Exec(`
		UPDATE stores
		SET metadata = $1
		WHERE store_id = $2;
	`, metadataJson, s.storeId.String())
	if err != nil {
		return err
	}

	s.metadata = metadata
	return nil
}
