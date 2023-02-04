package kafkalib

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/yurii-vyrovyi/kafkalib/dummylogger"

	"github.com/stretchr/testify/require"
)

func TestConsumer_CheckConnection(t *testing.T) {

	config := ConsumerConfig{
		Topics:  []string{"test-topic-with-schema-1"},
		Brokers: "localhost:9092",
		GroupID: "test-local-group-13",

		CheckConnectionTimeout: 20 * time.Second,
	}

	consumer, err := NewConsumer(config, &dummylogger.Logger{}, nil)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// go func() {
	// 	time.Sleep(15 * time.Second)
	// 	cancel()
	// }()

	err = consumer.CheckConnection(ctx)
	fmt.Println(err)
}
