package projection

import (
	"encoding/json"
	"slices"
	"sync"
	"time"

	fluxcore "github.com/gehhilfe/eventflux/core"
	"github.com/hallgren/eventsourcing"

	"github.com/gehhilfe/eventflux/cmd/example/model"
)

type NotificationOverviewEntry struct {
	Id        string
	Title     string
	Servity   model.NotificationServity
	CreatedAt time.Time
}

// This projection is a simple example of a projection that listens to all events and projects a list of the 10 most recent notifications.
type NotificationOverview struct {
	rwLock              sync.RWMutex
	storeManager        fluxcore.StoreManager
	register            *eventsourcing.Register
	recentNotifications []NotificationOverviewEntry
}

func NewNotificationOverview(
	storeManager fluxcore.StoreManager,
	register *eventsourcing.Register,
) *NotificationOverview {
	return &NotificationOverview{
		rwLock:              sync.RWMutex{},
		storeManager:        storeManager,
		register:            register,
		recentNotifications: make([]NotificationOverviewEntry, 0, 10),
	}
}

func (n *NotificationOverview) Project() {
	iterator := fluxcore.NewStoreIterator(n.storeManager, 0)
	defer iterator.Close()

	for iterator.WaitForNext() {
		event := iterator.Value()

		if event.AggregateType != "NotificationAggregate" {
			continue
		}

		func() {
			n.rwLock.Lock()
			defer n.rwLock.Unlock()

			f, ok := n.register.EventRegistered(event.Event)
			if !ok {
				panic("event not registered")
			}

			aggregateEvent := f()
			json.Unmarshal(event.Data, aggregateEvent)

			switch e := aggregateEvent.(type) {
			case *model.NotificationCreated:
				n.recentNotifications = append(n.recentNotifications, NotificationOverviewEntry{
					Id:        event.AggregateID,
					Title:     e.Title,
					Servity:   e.Servity,
					CreatedAt: event.Timestamp,
				})
			}

			// Sort the notifications by creation date, newest first
			slices.SortFunc(n.recentNotifications, func(a, b NotificationOverviewEntry) int {
				return a.CreatedAt.Compare(b.CreatedAt) * -1
			})
		}()
	}
}

func (n *NotificationOverview) RecentNotifications() []NotificationOverviewEntry {
	n.rwLock.RLock()
	defer n.rwLock.RUnlock()

	clone := make([]NotificationOverviewEntry, len(n.recentNotifications))
	copy(clone, n.recentNotifications)
	return clone
}
