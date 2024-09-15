package bus

import (
	"testing"
)

func TestLeakyBus_SubscribeInMemory(t *testing.T) {
	bus := NewInMemoryMessageBus()
	leakyBus := NewLeakyBus(bus, 50) // Drop 50% of the messages

	ctr := 0
	_, err := leakyBus.Subscribe("subject", func(message []byte, metadata map[string]string) error {
		ctr++
		return nil
	})

	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10000; i++ {
		err := leakyBus.Publish("subject", []byte("message"))
		if err != nil {
			t.Fatal(err)
		}
	}

	allowedErrorPercentage := float64(5)
	receivedPercentage := float64(ctr) / 10000 * 100

	if receivedPercentage < 50-allowedErrorPercentage || receivedPercentage > 50+allowedErrorPercentage {
		t.Fatalf("Expected to receive 50%% of the messages, but received %f%%", receivedPercentage)
	}
}
