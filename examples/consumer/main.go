package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"gitlab.test.igdcs.com/finops/nextgen/sandbox/kafka-poc/kafkalib"
	"gitlab.test.igdcs.com/finops/nextgen/sandbox/kafka-poc/kafkalib/dummylogger"

	logger "github.com/sirupsen/logrus"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())

	setupGracefulShutdown(cancel)

	if err := run(ctx); err != nil {
		fmt.Println("ERR:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	log := dummylogger.Logger{}

	consumerConfig := kafkalib.ConsumerConfig{
		Topics: []string{
			"test-topic-no-schema",
			"test-topic-no-schema-2",
		},
		Brokers:      "localhost:9092",
		GroupID:      "example-consumer-1",
		CloseTimeout: 30 * time.Second,
	}

	consumer, err := kafkalib.NewConsumer(consumerConfig, &log, nil)
	if err != nil {
		return fmt.Errorf("creating consumer: %w", err)
	}

	if err := consumer.Run(ctx, handler); err != nil {
		return fmt.Errorf("consumer run: %w", err)
	}

	return nil
}

func handler(_ context.Context, msg *kafkalib.Message) error {

	// fmt.Printf("[t: %s] [o: %d] [k: %s]: %s\n", msg.Topic, msg.Offset, string(msg.Key), string(msg.Value))
	// fmt.Printf("[t: %s] [o: %d]\n", msg.Topic, msg.Offset)

	fmt.Println(".")

	return nil
}

func setupGracefulShutdown(stop func()) {
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-signalChan
		logger.Info("got term signal")
		stop()
	}()
}
