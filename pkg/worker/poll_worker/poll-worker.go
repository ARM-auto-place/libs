package poll_worker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog"

	"github.com/ARM-auto-place/libs/pkg/metrics"
	"github.com/ARM-auto-place/libs/pkg/worker"
)

type pollWorker[Req any, Res any] struct {
	logger      *zerolog.Logger
	tickTimeout time.Duration
	metrics     metrics.IWorkerMetrics
	name        string
	tickCount   uint // for periodic lag updates
	MaxRetries  uint64

	kafkaProducer      Producer
	kafkaProducerTopic string
	getTransaction     func(ctx context.Context, isMaster bool) (pgx.Tx, error)

	dedupStore   DedupStore
	dedupKeyFunc func(ctx context.Context, msg Req) (string, error)
}

func (w *pollWorker[Req, Res]) Start(ctx context.Context, stop chan struct{}, poller Poller[Req], processor worker.Processor[Req, Res], writer worker.Writer[Res]) error {
	for {
		select {
		case <-stop:
			w.logger.Info().Msg("kafka consumer stopped")
			return nil
		default:
			if err := w.batchTick(ctx, stop, poller, processor, writer); err != nil {
				if errors.Is(err, worker.ErrCritical) {
					w.metrics.SetHealth(w.name, 0)
					return fmt.Errorf("got critical error: %w", err)
				}
				w.logger.Warn().Err(err).Msg("tick ends with error")
			}
		}
	}
}

func (w *pollWorker[Req, Res]) batchTick(ctx context.Context, stop chan struct{}, poller Poller[Req], processor worker.Processor[Req, Res], writer worker.Writer[Res]) error {
	w.metrics.SetHealth(w.name, 1)

	pollStart := time.Now()
	msgs, err := poller.Poll(ctx)
	if msgs != nil || err != nil {
		w.metrics.ObserveStepDuration(w.name, metrics.StepPoll, time.Since(pollStart))
	}
	if err != nil {
		w.logger.Error().Err(err).Msg("could not poll")
		w.metrics.IncError(w.name, "poll_error")
		return err
	}
	if msgs == nil {
		w.logger.Info().Msg("empty message")

		return nil
	}

	w.logger.Debug().Interface("counts", len(msgs)).Msg("got messages")

	for _, msg := range msgs {
		select {
		case <-stop:
			return nil
		default:
		}

		err = w.tick(ctx, processor, writer, msg)
		if err != nil {
			return err
		}

		commitStart := time.Now()
		err = poller.Commit(ctx, msg)
		w.metrics.ObserveStepDuration(w.name, metrics.StepCommit, time.Since(commitStart))
		if err != nil {
			w.logger.Error().Err(err).Interface("req", msg.Payload).Msg("could not commit")
			w.metrics.IncError(w.name, "commit_error")
			return err
		}

		w.logger.Debug().Interface("req", msg.Payload).Msg("request done")

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
	}

	return nil
}

func (w *pollWorker[Req, Res]) tick(ctx context.Context, processor worker.Processor[Req, Res], writer worker.Writer[Res], msg Message[Req]) error {
	tickStart := time.Now()
	defer func() {
		w.metrics.ObserveProcessDuration(w.name, time.Since(tickStart))
	}()

	var tickCtx context.Context
	var cancel context.CancelFunc
	if w.tickTimeout == time.Duration(0) {
		//for infinity retry topics
		tickCtx, cancel = context.WithCancel(ctx)
	} else {
		tickCtx, cancel = context.WithTimeout(ctx, w.tickTimeout)
	}
	defer cancel()

	w.logger.Debug().Interface("req", msg.Payload).Msg("start processing message")

	var dedupKey string
	if w.dedupStore != nil {
		if w.dedupKeyFunc == nil {
			return fmt.Errorf("dedup enabled but dedupKeyFunc is nil")
		}
		key, err := w.dedupKeyFunc(tickCtx, msg.Payload)
		if err != nil {
			return err
		}
		if key == "" {
			return fmt.Errorf("dedup key is empty (key func returned empty string)")
		}
		dedupKey = fmt.Sprintf("%s:%s", dedupKeyPrefix, key)
		ok, err := w.dedupStore.SetValueNX(tickCtx, dedupKey, time.Now().UnixNano(), dedupMessageTTL)
		if err != nil {
			return err
		}
		if !ok {
			w.logger.Info().Str("dedupKey", dedupKey).Msg("duplicate message, skip")
			return nil
		}
	}

	var result *Res

	var retryCount int
	var retryStart time.Time

	notify := func(err error, retryIn time.Duration) {
		w.logger.Warn().Int32("partition", msg.Metadata.Partition).Int64("offset", msg.Metadata.Offset).Err(err).Interface("data", msg.Payload).Msgf("could process message. retry in: %s", retryIn)
	}

	processStart := time.Now()
	var err error
	err = backoff.RetryNotify(func() error {
		retryCount++
		if retryCount == 1 {
			retryStart = time.Now()
		} else {
			w.metrics.IncRetryCount(w.name)
			w.metrics.ObserveRetryDelay(w.name, time.Since(retryStart))
			retryStart = time.Now()
		}

		var tx *pgx.Tx
		if w.getTransaction != nil {
			t, terr := w.getTransaction(ctx, true)
			if terr != nil {
				return terr
			}
			tx = &t
		}
		defer func() {
			if tx != nil {
				_ = (*tx).Rollback(context.WithoutCancel(ctx))
			}
		}()

		processed, processErr := processor.Process(tickCtx, msg.Payload, tx)
		if processErr != nil {
			if errors.Is(processErr, worker.ErrCritical) {
				w.logger.Error().Err(processErr).Interface("req", msg.Payload).Msg("could not process")
				w.metrics.IncError(w.name, "process_critical_error")
				return backoff.Permanent(processErr)
			}
			if errors.Is(processErr, worker.ErrSkipable) {
				w.metrics.IncError(w.name, "process_skipable_error")
				return backoff.Permanent(processErr)
			}
			return processErr
		}
		result = processed
		if result == nil {
			// ничего не было обработано/записано — держать ключ бессмысленно
			w.deleteDedupKey(tickCtx, dedupKey)
			w.logger.Debug().Interface("req", msg.Payload).Msg("process result is nil")
			if tx != nil {
				if cerr := (*tx).Commit(ctx); cerr != nil {
					return cerr
				}
			}
			return nil
		}

		w.logger.Debug().Interface("result", result).Msg("process result")

		writeStart := time.Now()
		writeErr := writer.Write(tickCtx, *result, tx)
		if writeErr != nil {
			w.logger.Error().Err(writeErr).Interface("result", result).Msg("could not write result")
			w.metrics.IncError(w.name, "write_error")
			return writeErr
		}
		w.metrics.ObserveStepDuration(w.name, metrics.StepWrite, time.Since(writeStart))

		if tx != nil {
			if cerr := (*tx).Commit(ctx); cerr != nil {
				return cerr
			}
		}

		return nil
	},
		backoff.WithMaxRetries(backoff.NewExponentialBackOff(), w.MaxRetries),
		notify,
	)

	if err != nil && !errors.Is(err, worker.ErrSkipable) {
		w.metrics.IncError(w.name, "process_failed")
		w.deleteDedupKey(tickCtx, dedupKey)
		if result != nil {
			if uerr := writer.Update(ctx, *result); uerr != nil {
				w.logger.Error().Interface("result", result).Err(uerr).Msg("could not update a processing result")
			}
		}
		if w.kafkaProducer != nil {
			sendErr := w.SendToRetryTopic(tickCtx, msg)
			w.metrics.IncError(w.name, "send_dl")
			if sendErr != nil {
				w.logger.Error().Err(sendErr).Interface("req", msg.Payload).Msg("could not send to dl")

				return err
			}

			return nil
		}
		return err
	}

	w.metrics.IncProcessedCount(w.name)
	w.metrics.ObserveStepDuration(w.name, metrics.StepProcess, time.Since(processStart))

	w.logger.Debug().Err(err).Interface("result", result).Msg("tick done")

	return nil
}

func (w *pollWorker[Req, Res]) SendToRetryTopic(ctx context.Context, msg Message[Req]) error {
	if msg.Metadata == nil {
		return fmt.Errorf("message metadata is nil")
	}
	return w.kafkaProducer.Produce(ctx, w.kafkaProducerTopic, msg.Metadata.Key, msg.Metadata.Value)
}

func (w *pollWorker[Req, Res]) deleteDedupKey(ctx context.Context, dedupKey string) {
	if w.dedupStore == nil || dedupKey == "" {
		return
	}
	if err := w.dedupStore.DeleteValue(ctx, dedupKey); err != nil {
		w.logger.Error().Err(err).Str("dedupKey", dedupKey).Msg("could not delete dedup key")
	}
}

func (w *pollWorker[Req, Res]) Stop(poller Poller[Req]) {
	if w.kafkaProducer != nil {
		w.kafkaProducer.Shutdown()
	}

	poller.Shutdown()
}

// Параметры NewPollWorker[Req any, Res any]:
//
//   - logger — логгер воркера;
//   - tickTimeout — таймаут на обработку одного сообщения. Если 0 — обработка
//     без ограничения по времени (для топиков с бесконечными ретраями);
//   - maxTickRetries — максимальное число попыток обработки одного сообщения
//     (через exponential backoff);
//   - m — метрики воркера. Если nil — no-op;
//   - name — имя воркера (label name в метриках);
//   - opts — дополнительные опции (WithDedup для дедупликации сообщений Kafka,
//     WithGetTransaction для транзакции pgx, WithRetry для retry-топика).
func NewPollWorker[Req any, Res any](
	logger *zerolog.Logger,
	tickTimeout time.Duration,
	maxTickRetries uint64,
	m *metrics.WorkerMetrics,
	name string,
	opts ...PollWorkerOption[Req, Res],
) PollWorker[Req, Res] {
	w := &pollWorker[Req, Res]{
		logger:      logger,
		tickTimeout: tickTimeout,
		MaxRetries:  maxTickRetries,
		metrics:     metrics.WorkerMetricsOrNoop(m),
		name:        name,
	}
	for _, opt := range opts {
		opt(w)
	}
	return w
}
