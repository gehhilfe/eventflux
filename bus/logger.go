package bus

import (
	"log/slog"

	"github.com/gehhilfe/eventflux/core"
)

type BusLogger struct {
	bus    core.MessageBus
	logger *slog.Logger
}

func NewBusLogger(
	logger *slog.Logger,
	bus core.MessageBus,
) *BusLogger {
	return &BusLogger{
		bus:    bus,
		logger: logger,
	}
}

func (b *BusLogger) Publish(subject string, message []byte) error {
	b.logger.Info("Publishing message", slog.String("subject", subject), slog.String("message", string(message)))
	return b.bus.Publish(subject, message)
}

func (b *BusLogger) Subscribe(subject string, handler func(message []byte, metadata core.Metadata) error) (core.Unsubscriber, error) {
	return b.bus.Subscribe(subject, func(message []byte, metadata core.Metadata) error {
		b.logger.Info("Received message", slog.String("subject", subject), slog.String("message", string(message)), slog.Any("metadata", metadata))
		return handler(message, metadata)
	})
}
