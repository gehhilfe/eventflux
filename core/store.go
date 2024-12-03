package core

import (
	"encoding/json"
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

func (id *StoreId) Scan(src any) error {
	var u uuid.UUID
	if err := u.Scan(src); err != nil {
		return err
	}
	*id = StoreId(u)
	return nil
}

func (id StoreId) String() string {
	return uuid.UUID(id).String()
}

type Metadata map[string]string

func (m *Metadata) Scan(src any) error {
	data, ok := src.([]uint8)
	if !ok {
		return errors.New("invalid data type for Metadata")
	}
	return json.Unmarshal(data, m)
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

type Filter struct {
	AggregateType *string
	AggregateID   *string
	Metadata      *Metadata
	StoreMetadata *Metadata
}

type StoreManager interface {
	List(metadata Metadata) iter.Seq[SubStore]
	Create(id StoreId, metadata Metadata) (SubStore, error)
	Get(id StoreId) (SubStore, error)
	OnCommit(handler func(SubStore, []Event)) Unsubscriber
	All(fluxVersion core.Version, filter Filter) (iter.Seq[Event], error)
}

type SubStore interface {
	core.EventStore
	Id() StoreId
	Metadata() Metadata
	// start is the non inclusive version to start from
	All(start core.Version) (iter.Seq[core.Event], error)
	Append(event core.Event) error

	LastVersion() core.Version

	UpdateMetadata(metadata Metadata) error
}

type StoreIterator interface {
	Close()
	WaitForNext() bool
	Value() Event
}
