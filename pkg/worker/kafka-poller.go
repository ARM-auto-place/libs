package worker

import (
	"context"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/rs/zerolog"
)

type kafkaPoller[Req any] struct {
	logger        *zerolog.Logger
	consumer      *kafka.Consumer
	topic         string
	connected     bool
	handleMessage func(ctx context.Context, m *kafka.Message) (*Req, error)
}

func (p *kafkaPoller[Req]) Connect(ctx context.Context) error {
	err := p.consumer.Subscribe(p.topic, func(c *kafka.Consumer, e kafka.Event) error {
		p.logger.Info().Interface("kafka event", e).Msg("rebalance event")
		return nil
	})
	if err != nil {
		return fmt.Errorf("could not subscribe topic: %w", err)
	}

	p.connected = true
	return nil
}

func (p *kafkaPoller[Req]) Poll(ctx context.Context, timeout time.Duration) (*Message[Req], error) {
	if !p.connected {
		return nil, fmt.Errorf("poller not connected to kafka")
	}

	ev := p.consumer.Poll(int(timeout.Milliseconds()))
	if ev == nil {
		p.logger.Debug().Msg("empty event from kafka")
		return nil, nil
	}

	switch e := ev.(type) {
	case *kafka.Message:
		p.logger.Debug().Interface("kafka message", e).Msg("got message from kafka")
		req, err := p.handleMessage(ctx, e)
		if err != nil {
			return nil, err
		}
		return &Message[Req]{
			Payload:  *req,
			Metadata: e,
		}, nil
	case kafka.Error:
		return nil, fmt.Errorf("kafka message error %v: %w", e.Code(), e)
	default:
		p.logger.Debug().Msgf("Ignored event: %v", e)
		return nil, nil
	}
}

func (p *kafkaPoller[Req]) Commit(ctx context.Context, msg *Message[Req]) error {
	kafkaMsg, ok := msg.Metadata.(*kafka.Message)
	if !ok {
		return fmt.Errorf("invalid message metadata type")
	}

	if _, err := p.consumer.CommitMessage(kafkaMsg); err != nil {
		return fmt.Errorf("could not commit message: %w", err)
	}
	return nil
}

// GetLag returns the current consumer lag (messages behind)
func (p *kafkaPoller[Req]) GetLag(ctx context.Context) (float64, error) {
	if !p.connected {
		return 0, fmt.Errorf("poller not connected")
	}

	// Get metadata for the topic
	metadata, err := p.consumer.GetMetadata(nil, true, 5000)
	if err != nil {
		return 0, fmt.Errorf("could not get metadata: %w", err)
	}

	topicMeta, ok := metadata.Topics[p.topic]
	if !ok {
		return 0, fmt.Errorf("topic %s not found in metadata", p.topic)
	}

	var totalLag float64
	for _, partition := range topicMeta.Partitions {
		// Get watermark offsets (low, high)
		low, high, err := p.consumer.QueryWatermarkOffsets(p.topic, partition.ID, 5000)
		if err != nil {
			continue
		}

		// Get committed offset
		committed, err := p.consumer.Committed([]kafka.TopicPartition{
			{Topic: &p.topic, Partition: partition.ID},
		}, 5000)
		if err != nil {
			continue
		}

		for _, tp := range committed {
			if tp.Partition == partition.ID {
				var committedOffset int64
				if tp.Offset == kafka.OffsetInvalid {
					committedOffset = low
				} else {
					committedOffset = int64(tp.Offset)
				}
				lag := high - committedOffset
				if lag > 0 {
					totalLag += float64(lag)
				}
			}
		}
	}

	return totalLag, nil
}

func NewKafkaPoller[Req any](
	logger *zerolog.Logger,
	topic string,
	kafkaCfg *kafka.ConfigMap,
	handleMessage func(ctx context.Context, m *kafka.Message) (*Req, error),
) (Poller[Req], error) {
	consumer, err := kafka.NewConsumer(kafkaCfg)
	if err != nil {
		return nil, fmt.Errorf("could not create consumer: %w", err)
	}

	go runLogs(consumer, logger)

	return &kafkaPoller[Req]{
		logger:        logger,
		topic:         topic,
		consumer:      consumer,
		handleMessage: handleMessage,
	}, nil
}

func runLogs(consumer *kafka.Consumer, logger *zerolog.Logger) {
	for e := range consumer.Logs() {
		logger.Info().
			Str("tag", e.Tag).
			Str("consumer name", e.Name).
			Int("level", e.Level).
			Msg(e.Message)
	}
}
