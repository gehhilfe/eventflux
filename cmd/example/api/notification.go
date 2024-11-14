package api

import (
	"encoding/json"
	"net/http"

	"github.com/gehhilfe/eventflux/cmd/example/model"
	"github.com/hallgren/eventsourcing"
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

type invalidFieldError struct {
	field string
}

func (e *invalidFieldError) Error() string {
	return "Invalid field: " + e.field
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
