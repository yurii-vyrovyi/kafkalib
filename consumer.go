package kafkalib

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	confluentkafka "github.com/confluentinc/confluent-kafka-go/kafka"

	"github.com/hashicorp/go-multierror"
)

type (
	Metrics interface {
		IncKafkaReceivedMessages()
	}
)

type (
	// MessageHandler handles Kafka messages.
	MessageHandler func(context.Context, *Message) error

	TopicOffsetsMap map[int32]confluentkafka.TopicPartition

	// Consumer is a high level Kafka consumer.
	Consumer struct {
		// kafka consumer
		consumer *confluentkafka.Consumer

		// Metrics
		metrics Metrics

		// config
		config ConsumerConfig

		// logger
		logger Logger

		// // message handling callback function
		msgHandler MessageHandler

		// stores offsets
		offsets map[string]TopicOffsetsMap

		// guards offsets
		muxOffsets sync.RWMutex

		// consumer's Close() should be run only once
		onceClose sync.Once
		errClose  error

		// terminates commit loops
		chanTermCommit chan struct{}

		// propagates commit loop errors
		chanCommitErrors chan error
	}
)

const (
	checkConnectionSleepPeriod = 500 * time.Millisecond
)

func NewConsumer(config ConsumerConfig, logger Logger, metrics Metrics) (*Consumer, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}

	configMap := config.ConfigMapWithDefaults()

	ckConsumer, err := confluentkafka.NewConsumer(&configMap)
	if err != nil {
		return nil, fmt.Errorf("creating consumer: %w", err)
	}

	return &Consumer{
		metrics:          metrics,
		logger:           NewTaggedLogger(logger),
		consumer:         ckConsumer,
		config:           config.WithDefaults(),
		offsets:          make(map[string]TopicOffsetsMap),
		chanTermCommit:   make(chan struct{}),
		chanCommitErrors: make(chan error),
	}, nil
}

func (c *Consumer) Run(ctx context.Context, messageHandler func(ctx context.Context, msg *Message) error) error {

	if messageHandler == nil {
		return errors.New("message handler is nil")
	}

	c.msgHandler = messageHandler

	if err := c.CheckConnection(ctx); err != nil {
		return fmt.Errorf("connecting broker: %w", err)
	}

	if c.config.EnableLogs {
		handleLogs(ctx, c.consumer.Logs(), LogLabelConsumer, c.logger)
	}

	c.logger.Info("consumer connected kafka")

	errSubscribe := c.consumer.SubscribeTopics(c.config.Topics, c.rebalanceCB)
	if errSubscribe != nil {
		return fmt.Errorf("cannot subscribe to topics: %w", errSubscribe)
	}

	c.logger.Info("consumer subscribed to topics")

	chEventsLoopErr := c.runEventsLoop(ctx)

	var retErr error

	select {
	case <-ctx.Done():
		// termination

	case err := <-chEventsLoopErr:
		// events loop failed
		if err != nil {
			retErr = err
		}

	case err := <-c.chanCommitErrors:
		// commit failed
		if err != nil {
			retErr = err
		}
	}

	c.logger.Info("consumer is going to close")

	if err := c.Close(); err != nil {
		retErr = multierror.Append(retErr, fmt.Errorf("kafka consumer close: %w", err))
	}
	close(c.chanTermCommit)
	close(c.chanCommitErrors)

	c.logger.Info("run is closed")

	return retErr
}

// Close shuts down Kafka consumer.
func (c *Consumer) Close() error {
	c.logger.Debug("closing consumer")

	// In some rare cases consumer Close gets stuck.
	// To prevent the service to get stuck as well, Close() is getting over after a timeout.
	closeErrChan := make(chan error)
	go func() {
		defer close(closeErrChan)

		c.onceClose.Do(func() {
			c.errClose = c.consumer.Close()
		})

		closeErrChan <- c.errClose
	}()

	var closeErr error
	select {

	case <-time.After(c.config.CloseTimeout):
		closeErr = errors.New("consumer close timeout")

	case err := <-closeErrChan:
		closeErr = err
	}

	c.logger.Infof("consumer is closed [err: %v]", closeErr)

	return closeErr
}

// ---------------- Aliveness checks funcs ----------------

// CheckConnection checks connection with a brokers.
// It makes several attempts until ping will be successful or a timeout will be reached.
func (c *Consumer) CheckConnection(ctx context.Context) error {
	now := time.Now()

	idxAttempt := 0

	for {
		// In case ctx.Done comes first and ping returns AFTER that,
		// sending error to channel will freeze because following select will quit on ctx.Done.
		// Buffered channel with capacity=2 unblocks `chanErr <- err` and a routine can end up.
		chanErr := make(chan error, 2)

		// running ping in parallel to terminate on ctx.Done
		go func() {
			defer close(chanErr)

			err := c.Ping()
			if err == nil {
				return
			}

			chanErr <- err
		}()

		// waiting for ping results or ctx.Done
		select {
		case <-ctx.Done():
			return errors.New("ctx.Done")

		case err, ok := <-chanErr:
			if !ok || err == nil {
				return nil
			}

			c.logger.Infof("check connection [attempt: #%d] failed: %v", idxAttempt, err)
		}

		// Some error happened. We'll try more a bit later
		time.Sleep(checkConnectionSleepPeriod)

		// Maybe that's enough?
		if time.Since(now) >= c.config.CheckConnectionTimeout {
			return errors.New("timeout")
		}

		idxAttempt++
	}
}

// Ping checks connection with a broker
func (c *Consumer) Ping() error {
	c.logger.Debug("ping")

	for _, topic := range c.config.Topics {
		topic := topic

		md, err := c.consumer.GetMetadata(&topic, false, int(c.config.PingTimeout/time.Millisecond))
		if err != nil {
			return fmt.Errorf("getting metadta [topic: %s]: %w", topic, err)
		}

		for _, topicMd := range md.Topics {
			if topicMd.Error.IsFatal() {
				return fmt.Errorf("topic [%s] metadata has fatal error: %s", topicMd.Topic, topicMd.Error.String())
			}

			if len(topicMd.Partitions) == 0 {
				return fmt.Errorf("topic [%s] metadata: no partitions", topicMd.Topic)
			}
		}
	}

	return nil
}

// ---------------- Offsets and rebalance funcs ----------------

// updateOffsets updates a map of offsets. These offsets will be committed when commit() is called.
func (c *Consumer) updateOffsets(msg *confluentkafka.Message) {
	if c.offsets == nil {
		return
	}

	if msg == nil || msg.TopicPartition.Topic == nil || len(*msg.TopicPartition.Topic) == 0 {
		return
	}

	c.muxOffsets.Lock()
	defer c.muxOffsets.Unlock()

	topic := *msg.TopicPartition.Topic
	partition := msg.TopicPartition.Partition

	topicInfo := c.offsets[topic]
	if topicInfo == nil {
		topicInfo = make(TopicOffsetsMap)
	}

	partitionInfo := topicInfo[partition]

	if msg.TopicPartition.Offset > partitionInfo.Offset {
		topicInfo[partition] = msg.TopicPartition
		c.offsets[topic] = topicInfo
	}
}

// commit sends offsets to a broker.
func (c *Consumer) commit() error {
	c.muxOffsets.RLock()
	defer c.muxOffsets.RUnlock()

	for _, topicInfo := range c.offsets {
		if len(topicInfo) == 0 {
			continue
		}

		// Committed offset value should always be the next after the last message that was consumed
		commitOffsets := make([]confluentkafka.TopicPartition, 0, len(topicInfo))

		for _, tp := range topicInfo {
			if tp.Offset == confluentkafka.OffsetInvalid {
				continue
			}

			tp.Offset += 1
			commitOffsets = append(commitOffsets, tp)
		}

		if len(commitOffsets) == 0 {
			return nil
		}

		// Logging committed partitions
		for _, partitionInfo := range commitOffsets {
			var topic string
			if partitionInfo.Topic != nil {
				topic = *partitionInfo.Topic
			}

			c.logger.Debugf("committing offsets [topic: %v] [partition: %v] [offset: %v]",
				topic, partitionInfo.Partition, partitionInfo.Offset)
		}

		_, err := c.consumer.CommitOffsets(commitOffsets)
		if err != nil {
			return fmt.Errorf("committing offsets: %w", err)
		}
	}

	return nil
}

func (c *Consumer) rebalanceCB(consumer *confluentkafka.Consumer, ev confluentkafka.Event) error {
	switch evTyped := ev.(type) {
	case confluentkafka.AssignedPartitions:

		// Initializing offsets map
		for _, p := range evTyped.Partitions {
			var topic string
			if p.Topic != nil {
				topic = *p.Topic
			}
			c.logger.Debugf("assigning [topic: %v] [partition: %v]", topic, p.Partition)

			if len(topic) == 0 || p.Offset == confluentkafka.OffsetInvalid {
				continue
			}

			topicInfo := c.offsets[topic]
			if topicInfo == nil {
				topicInfo = make(TopicOffsetsMap)
			}

			topicInfo[p.Partition] = p
			c.offsets[topic] = topicInfo
		}

		// running commit scheduled job
		c.runCommitPeriodicJob()

		// Assign partition
		if err := consumer.Assign(evTyped.Partitions); err != nil {
			c.logger.Errorf("failed to assign partition: %v", err)
			return fmt.Errorf("assigning on rebalance: %w", err)
		}

		c.logger.Debug("rebalance [assigned] is done")

	case confluentkafka.RevokedPartitions:
		for _, p := range evTyped.Partitions {
			var topic string
			if p.Topic != nil {
				topic = *p.Topic
			}
			c.logger.Debugf("revoking [topic: %v] [partition: %v]", topic, p.Partition)
		}

		c.logger.Debug("committing last offsets before unassigning")
		c.stopCommitPeriodicJob()

		// Commit the current offset synchronously before revoked partitions
		if err := c.commit(); err != nil {
			return fmt.Errorf("committing offsets: %w", err)
		}

		// Unassign partition
		if err := consumer.Unassign(); err != nil {
			c.logger.Errorf("failed to revoke partition: %v", err)
			return fmt.Errorf("revoking on rebalance: %w", err)
		}

		// removing offsets for revoked partitions
		for _, p := range evTyped.Partitions {
			if p.Topic == nil {
				continue
			}

			topic := *p.Topic
			topicPartitions := c.offsets[topic]
			delete(topicPartitions, p.Partition)
			c.offsets[topic] = topicPartitions
		}

		c.logger.Debug("rebalance [revoked] is done")
	}

	return nil
}

func (c *Consumer) runCommitPeriodicJob() {
	c.logger.Debug("running scheduled commit job")

	commitFaults := 0
	var retErr error

	go func() {
		defer c.logger.Debug("commit scheduled job exited")

		for {
			select {

			case <-c.chanTermCommit:
				c.logger.Debug("commit loop got chanTermCommit")
				return

			case <-time.After(c.config.CommitPeriod):
				if err := c.commit(); err != nil {
					commitFaults++

					retErr = multierror.Append(retErr, err)

					if commitFaults >= c.config.CommitFaultsTolerance {

						// Signaling that commit fails and waiting for channel to be closed.
						// Closing channel here may cause a deadlock while revoking partitions.
						c.chanCommitErrors <- fmt.Errorf("failed to commit offsets [%d times]: %w", c.config.CommitFaultsTolerance, retErr)

						return
					}

				} else {
					commitFaults = 0
					retErr = nil
				}

			}
		}
	}()

}

// stopCommitPeriodicJob stops offsets commits scheduler
func (c *Consumer) stopCommitPeriodicJob() {
	c.logger.Debug("stopping commit scheduled job")

	c.chanTermCommit <- struct{}{}
}

// ---------------- Messages processing funcs ----------------

func (c *Consumer) runEventsLoop(ctx context.Context) chan error {
	resultChan := make(chan error)

	go func() {
		defer close(resultChan)

		eventsChannel := c.consumer.Events()
		for event := range eventsChannel {
			if event == nil {
				continue
			}

			switch evTyped := event.(type) {

			case *confluentkafka.Message:
				if evTyped == nil {
					continue
				}

				if err := c.handleMessage(ctx, evTyped); err != nil {
					resultChan <- err
					return
				}

			case confluentkafka.PartitionEOF:
				// c.logger.Debug("reached end of queue")

			case confluentkafka.Error:
				resultChan <- evTyped
				return

			default:
				c.logger.Debug(fmt.Sprintf("ignoring event: %+v", evTyped))
			}
		}
	}()

	c.logger.Debug("events loop runs")

	return resultChan
}

func (c *Consumer) handleMessage(ctx context.Context, kafkaMessage *confluentkafka.Message) error {
	if kafkaMessage == nil {
		return nil
	}

	msg := newMessage(*kafkaMessage)

	// handling message
	if err := c.msgHandler(ctx, &msg); err != nil {
		return fmt.Errorf("handling kafka message: %w", err)
	}

	// Handling offsets
	c.updateOffsets(kafkaMessage)

	return nil
}

// newMessage transforms confluentkafka.Message to Message
func newMessage(kafkaMessage confluentkafka.Message) Message {
	var topic string
	if kafkaMessage.TopicPartition.Topic != nil {
		topic = *kafkaMessage.TopicPartition.Topic
	}

	return Message{
		Topic:     topic,
		Partition: int(kafkaMessage.TopicPartition.Partition),
		Offset:    int(kafkaMessage.TopicPartition.Offset),
		Value:     kafkaMessage.Value,
		Key:       kafkaMessage.Key,
		Timestamp: kafkaMessage.Timestamp,
	}
}
