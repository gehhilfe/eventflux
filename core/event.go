package core

import "github.com/hallgren/eventsourcing/core"

type Event struct {
	StoreId       StoreId
	StoreMetadata map[string]string
	FluxVersion   core.Version
	core.Event
}
