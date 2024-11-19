package api

import (
	"encoding/json"
	"net/http"

	"github.com/gehhilfe/eventflux/cmd/example/model"
	"github.com/gehhilfe/eventflux/cmd/example/projection"
	"github.com/hallgren/eventsourcing"
)

// POST /api/category
type createCategoryDto struct {
	Title string `json:"title"`
	Tag   string `json:"tag"`
}

func (d *createCategoryDto) IsValid() error {
	if d.Title == "" {
		return &invalidFieldError{"title"}
	}
	if d.Tag == "" {
		return &invalidFieldError{"tag"}
	}
	return nil
}

func CreateCategoryHandler(repo *eventsourcing.EventRepository) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		dto := createCategoryDto{}
		err := json.NewDecoder(r.Body).Decode(&dto)
		if err != nil {
			http.Error(w, "Bad request", http.StatusBadRequest)
			return
		}

		if err := dto.IsValid(); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		cat, err := model.CreateCategory(dto.Title, dto.Tag)
		if err != nil {
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}

		repo.Save(cat)

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(cat)
	}
}

func ListCategoriesHandler(projector *projection.CategoryLookupProjection) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(projector.Data())
	}
}
