package bus

import (
	"sync"
	"testing"

	"github.com/gehhilfe/eventflux/core"
	test "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
)

func TestNatsCoreBus(t *testing.T) {
	// start nats server
	server := test.RunDefaultServer()
	defer server.Shutdown()

	nc, err := nats.Connect(server.ClientURL())
	if err != nil {
		t.Fatal(err)
	}

	a := NewCoreNatsMessageBus(nc, "test")

	var wg sync.WaitGroup
	var receivedMessage []byte

	a.Subscribe("test", func(message []byte, metadata core.Metadata) error {
		defer wg.Done()
		receivedMessage = message
		return nil
	})

	wg.Add(1)
	err = a.Publish("test", []byte("test message"))
	if err != nil {
		t.Fatal(err)
	}

	wg.Wait()
	if string(receivedMessage) != "test message" {
		t.Errorf("expected 'test message', got %v", receivedMessage)
	}
}
