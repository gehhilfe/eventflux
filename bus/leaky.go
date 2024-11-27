package bus

import (
	"math/rand"

	"github.com/gehhilfe/eventflux/core"
)

type LeakyBus struct {
	dropPercentage int // 0-100
	bus            core.MessageBus
}

func NewLeakyBus(
	bus core.MessageBus,
	dropPercentage int,
) *LeakyBus {
	return &LeakyBus{
		bus:            bus,
		dropPercentage: min(max(dropPercentage, 0), 100),
	}
}

func (b *LeakyBus) Publish(subject string, message []byte) error {
	if rand.Intn(100) < b.dropPercentage {
		return nil // Drop the message
	}
	return b.bus.Publish(subject, message)
}

func (b *LeakyBus) Subscribe(subject string, handler func(message []byte, metadata core.Metadata) error) (core.Unsubscriber, error) {
	return b.bus.Subscribe(subject, handler)
}
