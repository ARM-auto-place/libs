package poll_worker

import (
	"context"

	"github.com/rs/zerolog"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ARM-auto-place/libs/pkg/config"
)

type Producer interface {
	Produce(
		ctx context.Context,
		topic string,
		key []byte,
		value []byte,
	) error
	ProduceAsync(
		ctx context.Context,
		topic string,
		key []byte,
		value []byte,
	) error
	Shutdown()
}

type kafkaProducer struct {
	client *kgo.Client
	logger *zerolog.Logger
}

func (p *kafkaProducer) Produce(
	ctx context.Context,
	topic string,
	key []byte,
	value []byte,
) error {
	rec := &kgo.Record{
		Topic: topic,
		Key:   key,
		Value: value,
	}

	return p.client.ProduceSync(ctx, rec).FirstErr()
}

func (p *kafkaProducer) ProduceAsync(
	ctx context.Context,
	topic string,
	key []byte,
	value []byte,
) error {
	rec := &kgo.Record{
		Topic: topic,
		Key:   key,
		Value: value,
	}

	p.client.Produce(ctx, rec, func(_ *kgo.Record, err error) {
		if err != nil && p.logger != nil {
			p.logger.Error().Err(err).Str("topic", topic).Msg("async produce failed")
		}
	})

	return nil
}

func (p *kafkaProducer) Shutdown() {
	p.client.Close()
}

func NewKafkaProducer(logger *zerolog.Logger, kafkaConfig config.KafkaConfig) (Producer, error) {
	opts := config.MakeProducerKafkaConfigMap(logger, kafkaConfig)

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}

	return &kafkaProducer{
		client: client,
		logger: logger,
	}, nil
}

type MockProducer struct {
	ProduceFunc func(
		ctx context.Context,
		topic string,
		key []byte,
		value []byte,
	) error
	ProduceAsyncFunc func(
		ctx context.Context,
		topic string,
		key []byte,
		value []byte,
	) error
}

func (m *MockProducer) Produce(
	ctx context.Context,
	topic string,
	key []byte,
	value []byte,
) error {
	if m.ProduceFunc == nil {
		return nil
	}
	return m.ProduceFunc(ctx, topic, key, value)
}

func (m *MockProducer) ProduceAsync(
	ctx context.Context,
	topic string,
	key []byte,
	value []byte,
) error {
	if m.ProduceAsyncFunc == nil {
		return nil
	}
	return m.ProduceAsyncFunc(ctx, topic, key, value)
}

func (m *MockProducer) Shutdown() {}
