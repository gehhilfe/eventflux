package core

import (
	"errors"
	"fmt"
	"iter"

	"github.com/google/uuid"
	"github.com/hallgren/eventsourcing/core"
)

type StoreId uuid.UUID

func (id StoreId) MarshalText() ([]byte, error) {
	return uuid.UUID(id).MarshalText()
}

func (id *StoreId) UnmarshalText(data []byte) error {
	var u uuid.UUID
	if err := u.UnmarshalText(data); err != nil {
		return err
	}
	*id = StoreId(u)
	return nil
}

func (id StoreId) String() string {
	return uuid.UUID(id).String()
}

var (
	ErrStoreNotFound = errors.New("store not found")
	ErrEventExists   = errors.New("event already exists")
)

type EventOutOfOrderError struct {
	StoreId  StoreId
	Expected core.Version
	Actual   core.Version
}

func (e *EventOutOfOrderError) Error() string {
	return fmt.Sprintf("event out of order: store=%s, expected=%d, actual=%d", e.StoreId, e.Expected, e.Actual)
}

type StoreManager interface {
	List(metadata map[string]string) iter.Seq[SubStore]
	Create(id StoreId, metadata map[string]string) (SubStore, error)
	Get(id StoreId) (SubStore, error)
	OnCommit(handler func(SubStore, []core.Event)) Unsubscriber
	All(starts map[StoreId]core.Version) (iter.Seq[StoreEvent], error)
}

type SubStore interface {
	core.EventStore
	Id() StoreId
	Metadata() map[string]string
	// start is the non inclusive version to start from
	All(start core.Version) (iter.Seq[core.Event], error)
	Append(event core.Event) error

	LastVersion() core.Version

	UpdateMetadata(metadata map[string]string) error
}

type StoreEvent struct {
	StoreId       StoreId
	StoreMetadata map[string]string
	core.Event
}

type StoreIterator interface {
	Close()
	WaitForNext() bool
	Value() StoreEvent
}
