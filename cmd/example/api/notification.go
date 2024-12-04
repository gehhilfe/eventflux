package api

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/gehhilfe/eventflux/cmd/example/model"
	"github.com/gehhilfe/eventflux/cmd/example/projection"
	fluxcore "github.com/gehhilfe/eventflux/core"
	"github.com/hallgren/eventsourcing"
	"github.com/hallgren/eventsourcing/core"
)

type createNotificationDto struct {
	Title   string                    `json:"title"`
	Body    string                    `json:"body"`
	Servity model.NotificationServity `json:"servity"`
}

func (d *createNotificationDto) IsValid() error {
	if d.Title == "" {
		return &invalidFieldError{"title"}
	}
	if d.Body == "" {
		return &invalidFieldError{"body"}
	}
	return nil
}

func CreateNotificationHandler(repo *eventsourcing.EventRepository) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		dto := createNotificationDto{}
		err := json.NewDecoder(r.Body).Decode(&dto)
		if err != nil {
			http.Error(w, "Bad request", http.StatusBadRequest)
			return
		}

		if err := dto.IsValid(); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		note, err := model.CreateNotification(dto.Title, dto.Body, dto.Servity, map[string][]string{})
		if err != nil {
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}

		repo.Save(note)

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(note)
	}
}

func ListRecentNotificationsHandler(no *projection.NotificationOverview) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		recentNotifications := no.RecentNotifications()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(recentNotifications)
	}
}

func MarkNotificationAsReadHandler(repo *eventsourcing.EventRepository) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		id := r.PathValue("id")
		if id == "" {
			http.Error(w, "Bad request", http.StatusBadRequest)
			return
		}

		note := model.NotificationAggregate{}
		err := repo.Get(id, &note)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		note.TrackChange(&note, &model.NotificationRead{})
		err = repo.Save(&note)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusNoContent)
	}
}

func StreamNotificationsHandler(sm fluxcore.StoreManager) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var startVersion core.Version
		// Get the last version from the query parameter
		if v := r.URL.Query().Get("start"); v != "" {
			_, err := fmt.Sscan(v, &startVersion)
			if err != nil {
				http.Error(w, "Bad request", http.StatusBadRequest)
				return
			}
		}

		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Accept, Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization")
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")

		encoder := json.NewEncoder(w)

		for event, err := range fluxcore.Iterate(r.Context(), sm, startVersion, fluxcore.Filter{}) {
			if err != nil {
				fmt.Fprintf(w, "event: %s\n\n", err)
				w.(http.Flusher).Flush()
				return
			}

			// Check if the connection is still open
			select {
			case <-r.Context().Done():
				return
			default:
			}

			fmt.Fprint(w, "data: ")
			encoder.Encode(event)
			fmt.Fprint(w, "\n") // Only one newline is needed, because the encoder already adds one
			w.(http.Flusher).Flush()
		}
	}
}
