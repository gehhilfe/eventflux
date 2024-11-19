package model

import "github.com/hallgren/eventsourcing"

type Category struct {
	Title string
	Tag   string
}

type CategoryAggregate struct {
	eventsourcing.AggregateRoot
	Category
}

type CategoryCreated struct {
	Title string
}

type CategoryRenamed struct {
	Title string
}

func (c *CategoryAggregate) Register(r eventsourcing.RegisterFunc) {
	r(
		&CategoryCreated{},
		&CategoryRenamed{},
	)
}

func (c *Category) Transition(event eventsourcing.Event) {
	switch e := event.Data().(type) {
	case *CategoryCreated:
		c.Title = e.Title
	case *CategoryRenamed:
		c.Title = e.Title
	}
}

func CreateCategory(
	title string,
	tag string,
) (*CategoryAggregate, error) {
	category := CategoryAggregate{}
	category.TrackChange(&category, &CategoryCreated{
		Title: title,
		Tag:   tag,
	})
	return &category, nil
}
