package core

import (
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hallgren/eventsourcing/core"
)

type storeIterator struct {
	m         StoreManager
	positions sync.Map

	pacer       *time.Ticker
	closeSignal chan struct{}
	ch          chan StoreEvent
	value       StoreEvent
}

// Close implements StoreIterator.
func (i *storeIterator) Close() {
	close(i.closeSignal)
}

// Value implements StoreIterator.
func (i *storeIterator) Value() StoreEvent {
	return i.value
}

// WaitForNext implements StoreIterator.
func (i *storeIterator) WaitForNext() bool {
	value, has := <-i.ch
	i.value = value
	return has
}

func NewStoreIterator(
	m StoreManager,
	starts map[StoreId]core.Version,
) *storeIterator {

	positions := make(map[StoreId]core.Version)
	for id, v := range starts {
		positions[id] = v
	}

	iterator := &storeIterator{
		m:           m,
		positions:   sync.Map{},
		closeSignal: make(chan struct{}),
		ch:          make(chan StoreEvent),
	}

	for id, v := range positions {
		iterator.positions.Store(id, v)
	}

	go iterator.iterate()

	return iterator
}

func (i *storeIterator) iterate() {
	defer close(i.ch) // Close the channel when the goroutine ends

	continueSignal := make(chan struct{}, 1)

	i.pacer = time.NewTicker(5 * time.Second)

	sub := i.m.OnCommit(func(s SubStore, events []core.Event) {
		continueSignal <- struct{}{}
	})
	defer sub.Unsubscribe()

	for {
		var foundAny atomic.Bool
		var wg sync.WaitGroup

		// Drain the continue signal channel
	drain:
		for {
			select {
			case <-continueSignal:
			default:
				break drain
			}
		}

		for store := range i.m.List(map[string]string{}) {
			id := store.Id()
			start := core.Version(0)
			if v, ok := i.positions.Load(id); ok {
				start = v.(core.Version)
			}

			wg.Add(1)
			// Start a goroutine for each store
			go func() {
				defer wg.Done()

				iter, err := store.All(start)
				if err != nil {
					return
				}

				for event := range iter {
					i.ch <- StoreEvent{
						StoreId:       id,
						StoreMetadata: store.Metadata(),
						Event:         event,
					}
					foundAny.Store(true)

					i.positions.Store(id, event.GlobalVersion)
				}
				slog.Info("store iterator done", slog.Any("store", id))
			}()
		}

		// Wait for all goroutines to finish
		wg.Wait()

		// If events were found, continue to the next iteration
		if foundAny.Load() {
			continue
		}

		// If no events were found, wait for a signal to continue
		select {
		case <-i.closeSignal:
			return
		case <-i.pacer.C:
		case <-continueSignal:
		}
	}
}
