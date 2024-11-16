package core

import (
	"time"

	"github.com/hallgren/eventsourcing/core"
)

type storeIterator struct {
	m        StoreManager
	position core.Version

	pacer       *time.Ticker
	closeSignal chan struct{}
	ch          chan Event
	value       Event
}

// Close implements StoreIterator.
func (i *storeIterator) Close() {
	close(i.closeSignal)
}

// Value implements StoreIterator.
func (i *storeIterator) Value() Event {
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
	start core.Version,
) *storeIterator {

	iterator := &storeIterator{
		m:           m,
		position:    start,
		closeSignal: make(chan struct{}),
		ch:          make(chan Event),
	}

	go iterator.iterate()

	return iterator
}

func (i *storeIterator) iterate() {
	defer close(i.ch) // Close the channel when the goroutine ends

	continueSignal := make(chan struct{}, 1)

	i.pacer = time.NewTicker(5 * time.Second)

	sub := i.m.OnCommit(func(s SubStore, events []Event) {
		continueSignal <- struct{}{}
	})
	defer sub.Unsubscribe()

	for {
		var foundAny bool

		// Drain the continue signal channel
	drain:
		for {
			select {
			case <-continueSignal:
			default:
				break drain
			}
		}

		iter, err := i.m.All(i.position)
		if err != nil {
			return
		}

		for e := range iter {
			i.ch <- e
			i.position = e.FluxVersion
			foundAny = true
		}

		// If events were found, continue to the next iteration
		if foundAny {
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
