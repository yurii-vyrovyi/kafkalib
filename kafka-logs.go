package kafkalib

import (
	"context"

	confluentkafka "github.com/confluentinc/confluent-kafka-go/kafka"
	gojson "github.com/goccy/go-json"
)

const (
	LogLabelConsumer = "KAFKA_C"
	LogLabelProducer = "KAFKA_P"
)

func handleLogs(ctx context.Context, chanLogs chan confluentkafka.LogEvent, label string, logger Logger) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return

			case log, ok := <-chanLogs:
				if !log.Timestamp.IsZero() {
					buf, err := gojson.Marshal(log)

					switch {
					case err != nil:
						logger.Debugf("%s: %v", label, err)

						continue

					case buf != nil:
						logger.Debugf(`{label: "%s", log: "%s"}`, label, buf)

					default:
						logger.Debugf(`{label: "%s", log: [NO LOG]}`, label)
					}
				}

				if !ok {
					logger.Debugf("%s: logs channel is closed", label)

					return
				}
			}
		}
	}()
}
