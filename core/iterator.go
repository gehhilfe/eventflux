package core

import (
	"context"
	"iter"
	"log/slog"
	"time"

	"github.com/hallgren/eventsourcing/core"
	slogctx "github.com/veqryn/slog-context"
)

func Iterate(ctx context.Context, m StoreManager, fluxVersion core.Version, filter Filter) iter.Seq2[*Event, error] {
	logger := slogctx.FromCtx(ctx)
	pacer := time.NewTicker(5 * time.Second)
	continueSignal := make(chan struct{}, 1)
	sub := m.OnCommit(func(s SubStore, events []Event) {
		continueSignal <- struct{}{}
	})
	return func(yield func(*Event, error) bool) {
		defer sub.Unsubscribe()

		for {
			var foundAny bool

			select {
			case <-ctx.Done():
				return
			default:
			}

			// Drain the continue signal channel
		drain:
			for {
				select {
				case <-continueSignal:
					continue
				case <-pacer.C:
					continue
				default:
					break drain
				}
			}

			iter, err := m.All(fluxVersion, filter)
			if err != nil {
				logger.Error("Failed to get all events", slog.Any("error", err))
				yield(nil, err)
				return
			}

			for e := range iter {
				if !yield(&e, nil) {
					return
				}
				fluxVersion = e.FluxVersion
				foundAny = true
			}

			if foundAny {
				logger.Debug("storeIterator: found events, will not sleep")
				continue
			}

			logger.Debug("storeIterator: sleeping for new events")
			select {
			case <-pacer.C:
				logger.Debug("storeIterator: waking up")
			case <-continueSignal:
				logger.Debug("storeIterator: waking up")
			}
		}
	}
}
