// File poll-worker.go contains the LEGACY confluent-kafka single-message poll
// worker. It is kept only for backward compatibility and should not be used in
// new code — prefer reader-worker.go (TickerWorker) or the poll_worker
// sub-package (franz-go batch polling).
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/ARM-auto-place/libs/pkg/metrics"
	"github.com/cenkalti/backoff/v4"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type pollWorkerImpl[Req any, Res any] struct {
	logger      *zerolog.Logger
	tickTimeout time.Duration
	pollTimeout time.Duration
	metrics     metrics.IWorkerMetrics
	name        string
	tickCount   uint // for periodic lag updates
	MaxRetries  uint64

	kafkaProducer      *kafka.Producer
	kafkaProducerTopic string
}

var (
	ErrCritical = errors.New("critical error")
	ErrWarning  = errors.New("warning error")
	ErrSkipable = errors.New("skipable error")
)

func (w *pollWorkerImpl[Req, Res]) tick(ctx context.Context, poller Poller[Req], processor Processor[Req, Res], writer Writer[Res]) error {
	w.metrics.SetHealth(w.name, 1)
	tickCtx, cancel := context.WithTimeout(ctx, w.tickTimeout)
	defer cancel()

	pollStart := time.Now()
	defer func() {
		w.metrics.ObserveProcessDuration(w.name, time.Since(pollStart))
	}()
	msg, err := poller.Poll(tickCtx, w.pollTimeout)
	if msg != nil || err != nil {
		w.metrics.ObserveStepDuration(w.name, metrics.StepPoll, time.Since(pollStart))
	}
	if err != nil {
		w.logger.Error().Err(err).Msg("could not poll")
		w.metrics.IncError(w.name, "poll_error")
		return err
	}
	if msg == nil {
		return nil
	}

	var retryCount int
	var retryStart time.Time

	processStart := time.Now()
	err = backoff.Retry(func() error {
		retryCount++
		if retryCount == 1 {
			retryStart = time.Now()
		} else {
			w.metrics.IncRetryCount(w.name)
			w.metrics.ObserveRetryDelay(w.name, time.Since(retryStart))
			retryStart = time.Now()
		}

		result, err := processor.Process(tickCtx, msg.Payload, nil)
		if err != nil {
			if errors.Is(err, ErrCritical) {
				w.logger.Error().Err(err).Interface("req", msg.Payload).Msg("could not process")
				w.metrics.IncError(w.name, "process_critical_error")
				return backoff.Permanent(err)
			}
			if errors.Is(err, ErrSkipable) {
				w.metrics.IncError(w.name, "process_skipable_error")
				return backoff.Permanent(err)
			}
			return err
		}
		if result == nil {
			w.logger.Info().Interface("req", msg.Payload).Msg("process result is nil")
			return backoff.Permanent(nil)
		}

		w.logger.Info().Err(err).Interface("result", result).Msg("process result")

		writeStart := time.Now()
		err = writer.Write(tickCtx, *result, nil)
		if err != nil {
			w.logger.Error().Err(err).Interface("result", result).Msg("could not write result")
			w.metrics.IncError(w.name, "write_error")
			return err
		}
		w.metrics.ObserveStepDuration(w.name, metrics.StepWrite, time.Since(writeStart))
		return nil
	}, backoff.WithMaxRetries(backoff.NewExponentialBackOff(), w.MaxRetries))

	if err != nil && !errors.Is(err, ErrSkipable) {
		w.logger.Error().Err(err).Interface("req", msg.Payload).Msg("could not commit")
		w.metrics.IncError(w.name, "process_failed")
		//send to dl topics
		if w.kafkaProducer != nil {
			err = w.SentToDL(msg)
			if err != nil {
				w.logger.Error().Err(err).Interface("req", msg.Payload).Msg("could not send to dl")
			}
			w.metrics.IncError(w.name, "send_dl")
		}
		return err
	}

	w.metrics.IncProcessedCount(w.name)
	w.metrics.ObserveStepDuration(w.name, metrics.StepProcess, time.Since(processStart))

	commitStart := time.Now()
	err = poller.Commit(tickCtx, msg)
	w.metrics.ObserveStepDuration(w.name, metrics.StepCommit, time.Since(commitStart))
	if err != nil {
		w.logger.Error().Err(err).Interface("req", msg.Payload).Msg("could not commit")
		w.metrics.IncError(w.name, "commit_error")
		return err
	}

	log.Debug().Interface("req", msg.Payload).Msg("request done")

	w.tickCount++
	// Update lag every 100 ticks
	if w.tickCount%100 == 0 {
		lag, err := poller.GetLag(ctx)
		if err != nil {
			w.logger.Debug().Err(err).Msg("could not get kafka lag")
		} else {
			w.metrics.SetKafkaLag(w.name, lag)
		}
	}

	return nil
}

func (w *pollWorkerImpl[Req, Res]) SentToDL(msg *Message[Req]) error {
	var key []byte
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("could not marshal: %w", err)
	}
	if msg.Metadata != nil {
		metadata, ok := msg.Metadata.(*kafka.Message)
		if !ok {
			w.logger.Info().Msg("invalid message metadata type")
		} else {
			key = metadata.Key
		}
	}
	err = w.kafkaProducer.Produce(
		&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &w.kafkaProducerTopic, Partition: kafka.PartitionAny},
			Value:          data,
			Key:            key,
		}, nil,
	)
	return err
}

func (w *pollWorkerImpl[Req, Res]) Start(ctx context.Context, poller Poller[Req], processor Processor[Req, Res], writer Writer[Res]) error {
	for {
		select {
		case <-ctx.Done():
			w.logger.Info().Msg("kafka consumer stopped")
			return nil
		default:
			if err := w.tick(ctx, poller, processor, writer); err != nil {
				if errors.Is(err, ErrCritical) {
					w.metrics.SetHealth(w.name, 0)
					return fmt.Errorf("got critical error: %w", err)
				}
				log.Warn().Err(err).Msg("tick ends with error")
			}
		}
	}
}

func (w *pollWorkerImpl[Req, Res]) Stop(ctx context.Context) error {
	return nil
}

func NewPollWorker[Req any, Res any](
	logger *zerolog.Logger,
	tickTimeout time.Duration,
	pollTimeout time.Duration,
	maxTickRetries uint64,
	m *metrics.WorkerMetrics,
	name string,
	producer *kafka.Producer,
	kafkaProducerTopic string,
) PollWorker[Req, Res] {
	return &pollWorkerImpl[Req, Res]{
		logger:             logger,
		tickTimeout:        tickTimeout,
		pollTimeout:        pollTimeout,
		MaxRetries:         maxTickRetries,
		metrics:            metrics.WorkerMetricsOrNoop(m),
		name:               name,
		kafkaProducer:      producer,
		kafkaProducerTopic: kafkaProducerTopic,
	}
}
