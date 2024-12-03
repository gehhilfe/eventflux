package postgres

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"iter"

	"github.com/google/uuid"
	"github.com/hallgren/eventsourcing/core"
	_ "github.com/lib/pq"

	"github.com/huandu/go-sqlbuilder"

	fluxcore "github.com/gehhilfe/eventflux/core"
)

type StoreManager struct {
	db          *sql.DB
	onCommitCbs []func(fluxcore.SubStore, []fluxcore.Event)
}

func (sm *StoreManager) DB() *sql.DB {
	return sm.db
}

func NewStoreManager(
	uri string,
) (*StoreManager, error) {
	db, err := sql.Open("postgres", uri)
	if err != nil {
		return nil, fmt.Errorf("failed to open database connection: %w", err)
	}

	m := &StoreManager{
		db:          db,
		onCommitCbs: make([]func(fluxcore.SubStore, []fluxcore.Event), 0),
	}

	tx, err := db.Begin()
	if err != nil {
		return nil, fmt.Errorf("failed to start transaction: %w", err)
	}
	defer tx.Rollback()

	// Initialize the tables if they don't exist
	// Create stores table
	_, err = tx.Exec(`
		CREATE TABLE IF NOT EXISTS stores (
			id SERIAL PRIMARY KEY,
			store_id TEXT NOT NULL,
			metadata JSONB NOT NULL,
			last_version INT NOT NULL DEFAULT 0,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		);
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to create stores table: %w", err)
	}

	// Create index on store_id
	_, err = tx.Exec(`
		CREATE UNIQUE INDEX IF NOT EXISTS store_id_idx ON stores (store_id);
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to create index on store_id: %w", err)
	}

	// Create events table
	_, err = tx.Exec(`
		CREATE TABLE IF NOT EXISTS events (
			id SERIAL PRIMARY KEY,
			global_version INT NOT NULL,
			store_id TEXT NOT NULL,
			aggregate_id TEXT NOT NULL,
			aggregate_type TEXT NOT NULL,
			version INT NOT NULL,
			reason TEXT NOT NULL,
			data JSONB NOT NULL,
			metadata JSONB NOT NULL,
			created_at TIMESTAMPTZ NOT NULL
		);
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to create events table: %w", err)
	}

	// Create index to improve query for where store_id and sort by global_version
	_, err = tx.Exec(`
		CREATE INDEX IF NOT EXISTS store_id_global_version_idx ON events (store_id, global_version);
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to create index on store_id and global_version: %w", err)
	}

	// Create index to improve query for where aggregate_id and sort by version
	_, err = tx.Exec(`
		CREATE INDEX IF NOT EXISTS aggregate_id_version_idx ON events (aggregate_id, version);
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to create index on aggregate_id and version: %w", err)
	}

	// Create unique index to prevent duplicate events for each store_id and global_version
	_, err = tx.Exec(`
		CREATE UNIQUE INDEX IF NOT EXISTS store_id_global_version_unique ON events (store_id, global_version);
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to create unique index on store_id and global_version: %w", err)
	}

	// Create unique index to prevent duplicate events for each aggregate_id and version
	_, err = tx.Exec(`
		CREATE UNIQUE INDEX IF NOT EXISTS aggregate_id_version_unique ON events (aggregate_id, version);
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to create unique index on aggregate_id and version: %w", err)
	}

	// Prevent all delete and update on events table with before trigger
	_, err = tx.Exec(`
	CREATE OR REPLACE FUNCTION prevent_delete_update_on_events()
	RETURNS TRIGGER AS $$
	BEGIN
		RAISE EXCEPTION 'Delete and update are not allowed on events table';
	END;
	$$ LANGUAGE plpgsql;`)
	if err != nil {
		return nil, fmt.Errorf("failed to create or replace function prevent_delete_update_on_events: %w", err)
	}

	// Create trigger to prevent delete and update on events table
	_, err = tx.Exec(`
DO $$
BEGIN
	-- Check if the trigger exists
	IF NOT EXISTS (
		SELECT 1
		FROM pg_trigger
		WHERE tgname = 'prevent_delete_update_on_events'
	) THEN
		-- Create the trigger if it does not exist
		CREATE TRIGGER prevent_delete_update_on_events
		BEFORE DELETE OR UPDATE ON events
		FOR EACH STATEMENT
		EXECUTE FUNCTION prevent_delete_update_on_events();
	END IF;
END $$;
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to create or replace trigger prevent_delete_update_on_events: %w", err)
	}

	// Create function to increment the global version
	_, err = tx.Exec(`
	CREATE OR REPLACE FUNCTION update_global_version()
	RETURNS TRIGGER AS $$
	BEGIN
		-- Update the global version for the store_id
		UPDATE stores
		SET last_version = last_version + 1
		WHERE store_id = NEW.store_id
		RETURNING last_version INTO NEW.global_version;

		-- If no record exists for the store_id, insert a new one and set global_version to 0
		IF NOT FOUND THEN
			INSERT INTO stores (store_id, last_version)
			VALUES (NEW.store_id, 0)
			RETURNING last_version INTO NEW.global_version;
		END IF;

		RETURN NEW;
	END;
	$$ LANGUAGE plpgsql;`)
	if err != nil {
		return nil, fmt.Errorf("failed to create or replace function update_global_version: %w", err)
	}

	// Create trigger to increment the global version
	_, err = tx.Exec(`
DO $$
BEGIN
	-- Check if the trigger exists
	IF NOT EXISTS (
		SELECT 1 
		FROM pg_trigger 
		WHERE tgname = 'set_global_version'
	) THEN
		-- Create the trigger if it does not exist
		CREATE TRIGGER set_global_version
		BEFORE INSERT ON events
		FOR EACH ROW
		EXECUTE FUNCTION update_global_version();
	END IF;
END $$;
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to create or replace trigger set_global_version: %w", err)
	}

	// Setup complete
	if tx.Commit() != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return m, nil
}

func (m *StoreManager) List(metadata fluxcore.Metadata) iter.Seq[fluxcore.SubStore] {
	metadataStr, _ := json.Marshal(metadata)

	return func(yield func(fluxcore.SubStore) bool) {
		res, err := m.db.Query(`
			SELECT id, store_id, metadata FROM stores WHERE metadata @> $1;
		`, metadataStr)
		if err != nil {
			return
		}
		defer res.Close()

		for res.Next() {
			var pgId uint64
			var storeIdStr string
			var metadataStr string
			err = res.Scan(&pgId, &storeIdStr, &metadataStr)
			if err != nil {
				return
			}
			var metadata fluxcore.Metadata
			err = json.Unmarshal([]byte(metadataStr), &metadata)
			if err != nil {
				return
			}
			storeId := fluxcore.StoreId(uuid.MustParse(storeIdStr))

			if !yield(&subStore{
				manager:  m,
				db:       m.db,
				pgId:     pgId,
				storeId:  storeId,
				metadata: metadata,
			}) {
				return
			}
		}
	}
}

func (m *StoreManager) Get(id fluxcore.StoreId) (fluxcore.SubStore, error) {
	// Lookup store
	res, err := m.db.Query(`
		SELECT id, store_id, metadata FROM stores WHERE store_id = $1;
	`, id.String())
	if err != nil {
		return nil, err
	}
	defer res.Close()

	var pgId uint64
	var storeId fluxcore.StoreId
	var metadata fluxcore.Metadata

	if res.Next() {
		err = res.Scan(&pgId, &storeId, &metadata)
		if err != nil {
			return nil, err
		}

		return &subStore{
			manager:  m,
			db:       m.db,
			pgId:     pgId,
			storeId:  storeId,
			metadata: metadata,
		}, nil
	} else {
		return nil, fluxcore.ErrStoreNotFound
	}
}

func (m *StoreManager) Create(id fluxcore.StoreId, metadata fluxcore.Metadata) (fluxcore.SubStore, error) {
	metadataJson, err := json.Marshal(metadata)
	if err != nil {
		return nil, err
	}

	// Create store
	res, err := m.db.Query(`
		INSERT INTO stores (store_id, metadata)
		VALUES ($1, $2)
		RETURNING id;
	`, id.String(), metadataJson)
	if err != nil {
		return nil, err
	}
	defer res.Close()

	var pgId uint64
	if res.Next() {
		err = res.Scan(&pgId)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, fluxcore.ErrStoreNotFound
	}

	return &subStore{
		manager:  m,
		db:       m.db,
		pgId:     pgId,
		storeId:  id,
		metadata: metadata,
	}, nil
}

func (m *StoreManager) OnCommit(cb func(fluxcore.SubStore, []fluxcore.Event)) fluxcore.Unsubscriber {
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

func condJsonMatch(sb *sqlbuilder.SelectBuilder, field string, value interface{}) error {
	d, err := json.Marshal(value)
	if err != nil {
		return err
	}

	sb.Where(field + " @> " + sb.Args.Add(d))
	return nil
}

func (m *StoreManager) All(start core.Version, filter fluxcore.Filter) (iter.Seq[fluxcore.Event], error) {
	sb := sqlbuilder.PostgreSQL.NewSelectBuilder()
	sb.Select("events.id", "events.store_id", "events.aggregate_id", "events.version", "events.global_version", "events.aggregate_type", "events.created_at", "events.reason", "events.data", "events.metadata", "stores.metadata")
	sb.From("events")
	sb.JoinWithOption(sqlbuilder.InnerJoin, "stores", "events.store_id = stores.store_id")
	sb.Where(sb.GreaterThan("events.id", start))

	if filter.AggregateID != nil {
		sb.Where(sb.Equal("events.aggregate_id", *filter.AggregateID))
	}
	if filter.AggregateType != nil {
		sb.Where(sb.Equal("events.aggregate_type", *filter.AggregateType))
	}
	if filter.Metadata != nil {
		condJsonMatch(sb, "events.metadata", filter.Metadata)
	}
	if filter.StoreMetadata != nil {
		condJsonMatch(sb, "stores.metadata", filter.StoreMetadata)
	}
	sb.OrderBy("events.id")
	sb.Asc()

	sql, args := sb.Build()
	res, err := m.db.Query(sql, args...)
	if err != nil {
		return nil, err
	}
	return func(yield func(fluxcore.Event) bool) {
		defer res.Close()

		for res.Next() {
			var e fluxcore.Event
			err := res.Scan(&e.FluxVersion, &e.StoreId, &e.AggregateID, &e.Version, &e.GlobalVersion, &e.AggregateType, &e.Timestamp, &e.Reason, &e.Data, &e.Metadata, &e.StoreMetadata)
			if err != nil {
				return
			}
			if !yield(e) {
				return
			}
		}
	}, nil
}

func (m *StoreManager) commited(s fluxcore.SubStore, events []fluxcore.Event) error {
	for _, cb := range m.onCommitCbs {
		cb(s, events)
	}
	return nil
}
