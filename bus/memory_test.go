package bus

import (
	"sync"
	"testing"

	"github.com/gehhilfe/eventflux/core"
)

func TestInMemoryMessageBus_Publish(t *testing.T) {
	bus := NewInMemoryMessageBus()

	var wg sync.WaitGroup
	wg.Add(1)

	bus.Subscribe("test", func(message []byte, metadata core.Metadata) error {
		if string(message) != "test message" {
			t.Errorf("expected 'test message', got %v", message)
		}
		wg.Done()
		return nil
	})

	err := bus.Publish("test", []byte("test message"))
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	wg.Wait()
}
