package projection

import (
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/gehhilfe/eventflux/cmd/example/model"
	fluxcore "github.com/gehhilfe/eventflux/core"
	"github.com/hallgren/eventsourcing"
	"github.com/hallgren/eventsourcing/core"
)

const (
	notificationViewName = "notification_view_v2"
)

// NotificationView is project that creates a query table that lists all notifications.
type NotificationView struct {
	db           *sql.DB
	state        NotificationViewState
	register     *eventsourcing.Register
	storeManager fluxcore.StoreManager
}

type NotificationViewState struct {
	LastVersion core.Version `json:"last_version"`
}

func (s *NotificationViewState) Scan(value interface{}) error {
	bytes, ok := value.([]byte)
	if !ok {
		return errors.New("type assertion to []byte failed")
	}
	return json.Unmarshal(bytes, s)
}

func NewNotificationView(
	db *sql.DB,
	storeManager fluxcore.StoreManager,
	register *eventsourcing.Register,
) *NotificationView {
	return &NotificationView{
		db:           db,
		register:     register,
		storeManager: storeManager,
	}
}

func (v *NotificationView) Project() {
	// First create the table to track the state of this projection in the database.
	_, err := v.db.Exec(`
		CREATE TABLE IF NOT EXISTS projection_state (
			name TEXT PRIMARY KEY NOT NULL,
			state JSONB NOT NULL
		);
	`)
	if err != nil {
		panic(err)
	}

	// Check if the projection state exists or init new state.
	err = v.db.QueryRow(`
		SELECT state FROM projection_state WHERE name = $1;
	`, notificationViewName).Scan(&v.state)
	if err == sql.ErrNoRows {
		// If no state exists, create a new state.
		v.state = NotificationViewState{}
	}

	// Create the notification view table.
	_, err = v.db.Exec(`
		CREATE TABLE IF NOT EXISTS notification_view_v2 (
			id TEXT PRIMARY KEY NOT NULL,
			created_at TIMESTAMP NOT NULL,
			title TEXT NOT NULL,
			body TEXT NOT NULL,
			servity TEXT NOT NULL
		);
	`)
	if err != nil {
		panic(err)
	}

	// Start projecting the events from the current state
	iterator := fluxcore.NewStoreIterator(v.storeManager, v.state.LastVersion)
	defer iterator.Close()

	for iterator.WaitForNext() {
		event := iterator.Value()

		if event.AggregateType != "NotificationAggregate" {
			continue
		}

		err := func() error {
			tx, err := v.db.Begin()
			if err != nil {
				return err
			}
			defer tx.Rollback()

			f, ok := v.register.EventRegistered(event.Event)
			if !ok {
				return errors.New("event not registered")
			}

			aggregateEvent := f()
			err = json.Unmarshal(event.Data, aggregateEvent)
			if err != nil {
				return err
			}

			switch e := aggregateEvent.(type) {
			case *model.NotificationCreated:
				_, err = tx.Exec(`
					INSERT INTO notification_view_v2 (id, created_at, title, body, servity)
					VALUES ($1, $2, $3, $4, $5)
					ON CONFLICT (id) DO NOTHING;
				`, event.AggregateID, event.Timestamp, e.Title, e.Body, e.Servity)
				if err != nil {
					return err
				}
			}

			// Update the state of the projection
			v.state.LastVersion = event.FluxVersion

			// Update the state in the database
			stateJson, err := json.Marshal(v.state)
			if err != nil {
				return err
			}

			_, err = tx.Exec(`
				INSERT INTO projection_state (name, state)
				VALUES ($1, $2)
				ON CONFLICT (name) DO UPDATE SET state = $2;
			`, notificationViewName, stateJson)
			if err != nil {
				return err
			}

			return tx.Commit()
		}()
		if err != nil {
			panic(err)
		}
	}
}
