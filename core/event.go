package core

import "github.com/hallgren/eventsourcing/core"

type Event struct {
	StoreId       StoreId
	StoreMetadata Metadata
	FluxVersion   core.Version
	core.Event
}
