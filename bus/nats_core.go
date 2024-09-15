package bus

import (
	"fmt"

	"github.com/nats-io/nats.go"

	"github.com/gehhilfe/eventflux/core"
)

type CoreNatsMessageBus struct {
	nc            *nats.Conn
	subjectPrefix string
}

func NewCoreNatsMessageBus(
	nc *nats.Conn,
	subjectPrefix string,
) *CoreNatsMessageBus {
	return &CoreNatsMessageBus{
		nc:            nc,
		subjectPrefix: subjectPrefix,
	}
}

func (b *CoreNatsMessageBus) sub(subject string) string {
	if b.subjectPrefix == "" {
		return subject
	}
	return fmt.Sprint(b.subjectPrefix, ".", subject)
}

func (b *CoreNatsMessageBus) Publish(subject string, message []byte) error {
	return b.nc.Publish(b.sub(subject), message)
}

func (b *CoreNatsMessageBus) Subscribe(subject string, handler func(message []byte, metadata map[string]string) error) (core.Unsubscriber, error) {
	return b.nc.Subscribe(b.sub(subject), func(m *nats.Msg) {
		handler(m.Data, map[string]string{})
	})
}
