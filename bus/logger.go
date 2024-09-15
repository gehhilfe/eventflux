package bus

import (
	"log/slog"

	"github.com/gehhilfe/eventflux/core"
)

type BusLogger struct {
	bus core.MessageBus
}

func NewBusLogger(
	bus core.MessageBus,
) *BusLogger {
	return &BusLogger{
		bus: bus,
	}
}

func (b *BusLogger) Publish(subject string, message []byte) error {
	slog.Info("Publishing message", slog.String("subject", subject), slog.String("message", string(message)))
	return b.bus.Publish(subject, message)
}

func (b *BusLogger) Subscribe(subject string, handler func(message []byte, metadata map[string]string) error) (core.Unsubscriber, error) {
	return b.bus.Subscribe(subject, func(message []byte, metadata map[string]string) error {
		slog.Info("Received message", slog.String("subject", subject), slog.String("message", string(message)), slog.Any("metadata", metadata))
		return handler(message, metadata)
	})
}
