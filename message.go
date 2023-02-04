package kafkalib

import "time"

// Message is the Go representation of a Kafka message.
type Message struct {
	Topic     string
	Partition int
	Offset    int
	Value     []byte
	Key       []byte
	Timestamp time.Time
}
