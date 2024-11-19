package projection

import (
	"encoding/json"
	"sync"

	"github.com/gehhilfe/eventflux/cmd/example/model"
	fluxcore "github.com/gehhilfe/eventflux/core"
	"github.com/hallgren/eventsourcing"
)

type CategoryLookupMap map[string]string

type CategoryLookupProjection struct {
	rwLock                 sync.RWMutex
	storeManager           fluxcore.StoreManager
	register               *eventsourcing.Register
	lookup                 CategoryLookupMap
	lookupAggregateIdToTag map[string]string
}

func NewCategoryLookupProject(
	storeManager fluxcore.StoreManager,
	register *eventsourcing.Register,
) *CategoryLookupProjection {
	return &CategoryLookupProjection{
		rwLock:                 sync.RWMutex{},
		storeManager:           storeManager,
		register:               register,
		lookup:                 make(CategoryLookupMap),
		lookupAggregateIdToTag: make(map[string]string),
	}
}

func (c *CategoryLookupProjection) Project() {
	iterator := fluxcore.NewStoreIterator(c.storeManager, 0)
	defer iterator.Close()

	for iterator.WaitForNext() {
		event := iterator.Value()

		if event.AggregateType != "CategoryAggregate" {
			continue
		}

		func() {
			c.rwLock.Lock()
			defer c.rwLock.Unlock()

			f, ok := c.register.EventRegistered(event.Event)
			if !ok {
				panic("event not registered")
			}

			// Get the event data
			aggregateEvent := f()
			json.Unmarshal(event.Data, aggregateEvent)

			// Handle the event
			switch e := aggregateEvent.(type) {
			case *model.CategoryCreated:
				c.lookup[e.Tag] = e.Title
				c.lookupAggregateIdToTag[event.AggregateID] = e.Tag
			case *model.CategoryRenamed:
				c.lookup[c.lookupAggregateIdToTag[event.AggregateID]] = e.Title
			}
		}()
	}
}

// Data returns a copy of the lookup map
func (c *CategoryLookupProjection) Data() CategoryLookupMap {
	c.rwLock.RLock()
	defer c.rwLock.RUnlock()

	lookup := make(CategoryLookupMap)
	for k, v := range c.lookup {
		lookup[k] = v
	}
	return lookup
}
