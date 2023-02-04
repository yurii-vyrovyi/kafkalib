package kafkalib

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"time"

	"gitlab.test.igdcs.com/finops/nextgen/sandbox/kafka-poc/kafkalib/schemaregistry"

	confluentkafka "github.com/confluentinc/confluent-kafka-go/kafka"
	gojson "github.com/goccy/go-json"
	"github.com/santhosh-tekuri/jsonschema/v5"
)

type (
	ProducerMetrics interface {
		IncKafkaProducedMessages()
	}

	SchemaRegistry interface {
		GetLatestSchema(ctx context.Context, topic string) (*schemaregistry.Schema, error)
	}
)

type (
	Producer struct {
		producer     *confluentkafka.Producer
		logger       Logger
		metrics      ProducerMetrics
		flushTimeout time.Duration
		pingTimeout  time.Duration
		closeOnce    sync.Once
		chanReady    chan interface{}
		chanClosed   chan interface{}
		enableLogs   bool

		validateMessageSchema bool
		schemaRegistryTimeout time.Duration
		schemaRegistry        SchemaRegistry
		schemaCache           *CacheWithTTL[SchemaCacheItem]
		muxCache              sync.Mutex
	}

	CallBackFunc func(error)

	TopicMetadata struct {
		PartitionsNum int
	}

	SchemaCacheItem struct {
		Schema    string
		Validator *jsonschema.Schema
	}
)

// BuildProducer creates and runs producer. Under the hood producer runs a loop that pumps Kafka events.
//
// Producer MUST be closed before a shutdown to make it gracefully - to flush all messages  and to close connection
// with a broker. Call Close() method or cancel a context to close producer.
func BuildProducer(
	ctx context.Context,
	config ProducerConfig,
	schemaRegistry SchemaRegistry,
	logger Logger,
	metrics ProducerMetrics,
) (*Producer, error) {
	cProducer, err := confluentkafka.NewProducer(config.ConfigMapWithDefaults())
	if err != nil {
		return nil, fmt.Errorf("creating Kafka producer: %w", err)
	}

	if config.ValidateMessageSchema && schemaRegistry == nil {
		return nil, errors.New("")
	}

	config = config.WithDefaults()

	producer := Producer{
		producer:     cProducer,
		logger:       NewTaggedLogger(logger),
		metrics:      metrics,
		flushTimeout: config.FlushTimeout,
		pingTimeout:  config.PingTimeout,
		chanReady:    make(chan interface{}),
		chanClosed:   make(chan interface{}),
		enableLogs:   config.EnableLogs,

		validateMessageSchema: config.ValidateMessageSchema,
		schemaRegistryTimeout: config.SchemaRegistryTimeout,
		schemaRegistry:        schemaRegistry,
		schemaCache:           NewSchemaCache[SchemaCacheItem](config.SchemaTTL),
	}

	producer.run(ctx)

	return &producer, nil
}

// run runs confluent Kafka event loop in a routine.
// This is a core of a producer - it handles Kafka errors and invokes a callbacks.
func (p *Producer) run(ctx context.Context) {
	defer close(p.chanReady)

	if p.enableLogs {
		handleLogs(ctx, p.producer.Logs(), "K-PROD", p.logger)
	}

	// Handling context canceling.
	// On ctx.Done we MUST call producer.Close() to allow the producer to shut down gracefully.
	// Event loop MUST keep working, the producer will close events channel itself when all messages in queue will be flushed.
	go func() {
		select {
		case <-ctx.Done():
			if err := p.Close(); err != nil {
				p.logger.Errorf("closing producer: %w", err)
			}

		case <-p.chanClosed:
		}
	}()

	// pumping event queue
	go func() {
		evChan := p.producer.Events()
		for event := range evChan {
			switch ev := event.(type) {
			case *confluentkafka.Message:

				cb, ok := ev.Opaque.(func(error))
				if !ok || cb == nil {
					continue
				}

				if ev.TopicPartition.Error != nil {
					cb(fmt.Errorf("delivery error: %w", ev.TopicPartition.Error))
				} else {
					cb(nil)
				}

			case *confluentkafka.Error:
				if kafkaErr, ok := event.(*confluentkafka.Error); !ok {
					p.logger.Errorf("unknown kafka error: %+v", event)
				} else {
					p.logger.Errorf("kafka error event:", kafkaErr)
				}
			}
		}
	}()
}

// GetTopicMetadata returns a metadata for a topic.
// If topic is nil GetTopicMetadata returns a metadata for all topics.
func (p *Producer) GetTopicMetadata(topic *string) (map[string]TopicMetadata, error) {
	kafkaMetadata, err := p.producer.GetMetadata(topic, true, int(p.pingTimeout/time.Millisecond))
	if err != nil {
		return nil, fmt.Errorf("gtting metadata: %w", err)
	}

	metadata := make(map[string]TopicMetadata, len(kafkaMetadata.Topics))
	for t, v := range kafkaMetadata.Topics {
		metadata[t] = TopicMetadata{
			PartitionsNum: len(v.Partitions),
		}
	}

	return metadata, nil
}

// Ping tries to check if the broker can be connected.
func (p *Producer) Ping() error {
	_, err := p.producer.GetMetadata(nil, true, int(p.pingTimeout/time.Millisecond))
	if err != nil {
		return fmt.Errorf("getting metadata: %w", err)
	}

	return nil
}

// Close shuts down producer.
func (p *Producer) Close() error {
	var retErr error

	p.closeOnce.Do(func() {
		defer close(p.chanClosed)
		if err := p.Flush(); err != nil {
			p.logger.Errorf("flush:", err)
			retErr = err
		}

		p.producer.Close()
		p.logger.Debugf("flushed and closed")
	})

	return retErr
}

// waitUtilReady allows to wait until producer is up and connected.
//
// If you try to send a message before producer is connected you will get an error.
// This may lead to an infinite restarts if a service tries to send a message immediately after a start.
// Calling waitUtilReady() allows to avoid the issue.
func (p *Producer) waitUtilReady(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return errors.New("terminated")
	case <-p.chanReady:
		return nil
	}
}

// Flush flushes until it completes successfully or timeout.
func (p *Producer) Flush() error {
	waitMs := int(p.flushTimeout.Milliseconds())
	for prev, rem := p.producer.Flush(0), 0; prev > 0; prev = rem {
		rem = p.producer.Flush(waitMs)
		if prev == rem {
			return fmt.Errorf("no progress during timeout, dropped remaining %d records", rem)
		}
	}

	return nil
}

// getCachedSchema returns a schema for a particular topic.
// It tries to get cached schema. It there's no item in cache it calls schema registry for it.
// In case schema registry is nil or there's no schema for the topic ErrNoSchema error is returned.
func (p *Producer) getCachedSchema(ctx context.Context, topic string) (*SchemaCacheItem, error) {
	if p.schemaRegistry == nil {
		return nil, ErrNoSchema
	}

	p.muxCache.Lock()
	defer p.muxCache.Unlock()

	item, ok := p.schemaCache.Get(topic)
	if ok {
		return item, nil
	}

	// no cached schema - requesting schema from schema registry

	ctx, cancel := context.WithTimeout(ctx, p.schemaRegistryTimeout)
	defer cancel()

	schemaData, err := p.schemaRegistry.GetLatestSchema(ctx, topic)

	// if schema is not set for topic we treat message as valid
	if errors.Is(err, schemaregistry.ErrNotFound) {
		p.schemaCache.Set(topic, nil)

		return nil, ErrNoSchema
	}

	if err != nil {
		return nil, fmt.Errorf("getting latest schema for [%s]: %w", topic, err)
	}

	// creating and caching validator for the schema
	validator, err := jsonschema.CompileString("schema.json", schemaData.Schema)
	if err != nil {
		return nil, fmt.Errorf("compiling schema: %w", err)
	}

	cacheItem := SchemaCacheItem{
		Schema:    schemaData.Schema,
		Validator: validator,
	}

	p.schemaCache.Set(topic, &cacheItem)

	return &cacheItem, nil
}

// validateJsonMessage validates json object against a topic's schema.
// The return value is nil if message is valid, schema registry is nil or validation flag is false.
//
// If schema registry has no schema for the topic the message is treated as valid.
func (p *Producer) validateJSONMessage(ctx context.Context, jsonPayload, topic string) error {
	if !p.validateMessageSchema || p.schemaRegistry == nil {
		return nil
	}

	schemaData, err := p.getCachedSchema(ctx, topic)
	if err != nil {
		return fmt.Errorf("getting schema: %w", err)
	}

	// if there's no schema we treat message as valid
	if errors.Is(err, ErrNoSchema) {
		return nil
	}

	// if there's no schema we treat message as valid
	if schemaData == nil {
		return errors.New("schema is nil")
	}

	var mapObject map[string]interface{}
	err = gojson.Unmarshal([]byte(jsonPayload), &mapObject)
	if err != nil {
		return fmt.Errorf("unmarshalling payload: %w", err)
	}

	if err := schemaData.Validator.Validate(mapObject); err != nil {
		return fmt.Errorf("invalid obejct: %w", err)
	}

	return nil
}

// produceMessage produces a Kafka message.
// Producer sends a message asynchronously. When a message is sent to broker a callback is invoked.
//
// If partition is nil, the message will be assigned to any partition (according to broker's partition assignment strategy).
// Callback cb(error) is a callback function. If message was sent successfully an error is nil. In other case it explains the issue.
func (p *Producer) produceMessage(
	ctx context.Context,
	topic string,
	key string,
	payload []byte,
	partition *int32,
	cb func(error),
) error {
	if err := p.waitUtilReady(ctx); err != nil {
		return err
	}

	kafkaPartition := confluentkafka.PartitionAny
	if partition != nil {
		kafkaPartition = *partition
	}

	msg := confluentkafka.Message{
		TopicPartition: confluentkafka.TopicPartition{
			Topic:     &topic,
			Partition: kafkaPartition,
		},
		Value:  payload,
		Key:    []byte(key),
		Opaque: cb,
	}

	if err := p.producer.Produce(&msg, nil); err != nil {
		return fmt.Errorf("producing message: %w", err)
	}

	if p.metrics != nil {
		p.metrics.IncKafkaProducedMessages()
	}

	return nil
}

// AppendSchemaID encodes adds schemaID to Kafka message.
func AppendSchemaID(value []byte, schemaID int) ([]byte, error) {
	buf := bytes.NewBuffer([]byte{magicByte})
	if err := binary.Write(buf, binary.BigEndian, uint32(schemaID)); err != nil {
		return nil, err //nolint:wrapcheck // nothing to add
	}

	buf.Write(value)

	return buf.Bytes(), nil
}

func (p *Producer) ProduceMessageWithSchemaID(
	ctx context.Context,
	topic string,
	key string,
	payload []byte,
	partition *int32,
	schemaID int,
	cb func(error),
) error {
	payloadWithSchemaID, err := AppendSchemaID(payload, schemaID)
	if err != nil {
		return fmt.Errorf("adding schemaID to a payload: %w", err)
	}

	return p.produceMessage(ctx, topic, key, payloadWithSchemaID, partition, cb)
}

// ProduceJSON produces a JSON Kafka message.
// Producer sends a message asynchronously. When a message is sent to broker a callback is invoked.
//
// Before sending a message ProduceJSON() validates message against a topic schema.
// If message is invalid an error is returned. An error message wraps ErrInvalidMessage.
// It can be checked with errors.Is().
//
// If partition is nil, the message will be assigned to any partition (according to broker's partition assignment strategy).
// Callback cb(error) is a callback function. If message was sent successfully an error is nil. In other case it explains the issue.
func (p *Producer) ProduceJSON(
	ctx context.Context,
	topic string,
	key string,
	payload interface{},
	partition *int32,
	schemaID *int,
	cb func(error),
) error {
	jsonPayload, err := gojson.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshaling payload: %w", err)
	}

	if err := p.validateJSONMessage(ctx, string(jsonPayload), topic); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidMessage, err)
	}

	if schemaID != nil {
		return p.ProduceMessageWithSchemaID(ctx, topic, key, jsonPayload, partition, *schemaID, cb)
	}

	return p.produceMessage(ctx, topic, key, jsonPayload, partition, cb)
}

// Should be implemented later

// func (p *Producer) ProduceAvro(
// 	ctx context.Context,
// 	topic string,
// 	key string,
// 	payload interface{},
// 	partition *int32,
// 	cb func(error),
// ) error {}
