package poll_worker

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/rs/zerolog"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ARM-auto-place/libs/pkg/config"
)

type kafkaPoller[Req any] struct {
	pollCtx        context.Context
	pollCtxTimeout time.Duration
	logger         *zerolog.Logger
	consumer       *kgo.Client
	handleMessage  func(ctx context.Context, m *kgo.Record) (*Req, error)

	groupID string
	topic   string
}

func (p *kafkaPoller[Req]) Poll(ctx context.Context) ([]Message[Req], error) {
	pollCtx, cancel := context.WithTimeout(p.pollCtx, p.pollCtxTimeout)
	defer cancel()
	//pollCtx need to stop fetching immediately
	fetches := p.consumer.PollFetches(pollCtx)
	if err := pollCtx.Err(); err != nil {
		return nil, nil
	}

	if errs := fetches.Errors(); len(errs) > 0 {
		return nil, errs[0].Err
	}

	var pollErr error
	var result []Message[Req]
	fetches.EachRecord(func(record *kgo.Record) {
		req, err := p.handleMessage(ctx, record)
		if err != nil {
			pollErr = err
			return
		}
		result = append(result, Message[Req]{
			Payload:  *req,
			Metadata: record,
		})
		p.logger.Debug().Interface("kafka message", req).Msg("got message from kafka")
	})
	if pollErr != nil {
		return nil, pollErr
	}

	return result, nil
}

func (p *kafkaPoller[Req]) Commit(ctx context.Context, msg Message[Req]) error {
	return p.consumer.CommitRecords(
		ctx,
		msg.Metadata,
	)
}

// GetLag returns the current consumer lag (messages behind)
func (p *kafkaPoller[Req]) GetLag(ctx context.Context) (float64, error) {
	admin := kadm.NewClient(p.consumer)

	lags, err := admin.Lag(ctx, p.groupID)
	if err != nil {
		return 0, err
	}

	var total int64

	lags.Each(func(groupLag kadm.DescribedGroupLag) {
		totalByTopic := groupLag.Lag.TotalByTopic()
		totalLag, ok := totalByTopic[p.topic]
		if !ok {
			p.logger.Warn().Str("topic", p.topic).Msg("[Get Lag]: topic not found where")
		}
		total = totalLag.Lag
	})

	return float64(total), nil
}

func (p *kafkaPoller[Req]) Shutdown() {
	p.consumer.Close()
}

// Параметры NewKafkaPoller[Req any]:
//
//   - pollCtx — родительский контекст для чтения батча из Kafka;
//   - pollCtxTimeout — таймаут на PollFetches, ограничивает время ожидания сообщений;
//   - logger — логгер;
//   - kafkaCfg — конфигурация Kafka (config.KafkaConfig): ConsumerConfig
//     (настройки консьюмера: адреса, группа, топик, SASL, offset),
//     ProducerConfig (настройки продюсера), HostName (имя хоста),
//     ClientName (имя сервиса);
//   - handleMessage — функция, превращающая сырое *kgo.Record в полезную
//     нагрузку Req. Возвращает ошибку, если сообщение нельзя обработать (тогда
//     чтение прерывается).
func NewKafkaPoller[Req any](
	pollCtx context.Context,
	pollCtxTimeout time.Duration,
	logger *zerolog.Logger,
	kafkaCfg config.KafkaConfig,
	handleMessage func(ctx context.Context, m *kgo.Record) (*Req, error),
) (Poller[Req], error) {
	opts := config.MakeConsumerKafkaConfigMap(logger, kafkaCfg)

	consumer, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("could not create consumer: %w", err)
	}

	return &kafkaPoller[Req]{
		pollCtx:        pollCtx,
		pollCtxTimeout: pollCtxTimeout,
		logger:         logger,
		consumer:       consumer,
		handleMessage:  handleMessage,
	}, nil
}

func GetBaseHandleMessageFunc[Req any](validate *validator.Validate) func(ctx context.Context, m *kgo.Record) (*Req, error) {
	return func(ctx context.Context, m *kgo.Record) (*Req, error) {
		if m == nil {
			return nil, nil
		}

		var result Req
		if err := json.Unmarshal(m.Value, &result); err != nil {
			return nil, err
		}

		if validate != nil {
			if err := validate.Struct(&result); err != nil {
				return nil, err
			}
		}

		return &result, nil
	}
}
