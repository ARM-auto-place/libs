package worker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ARM-auto-place/libs/pkg/metrics"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type pollWorkreImplDL[Req any, Res any] struct {
	logger      *zerolog.Logger
	tickTimeout time.Duration
	pollTimeout time.Duration
	metrics     metrics.IWorkerMetrics
	name        string
}

func (w *pollWorkreImplDL[Req, Res]) tick(ctx context.Context, poller Poller[Req], processor Processor[Req, Res], writer Writer[Res]) error {
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

	processStart := time.Now()
	result, err := processor.Process(tickCtx, msg.Payload, nil)
	if err != nil {
		if errors.Is(err, ErrCritical) {
			w.logger.Error().Err(err).Interface("req", msg.Payload).Msg("could not process")
			w.metrics.IncError(w.name, "process_critical_error")
		}
		if errors.Is(err, ErrSkipable) {
			w.metrics.IncError(w.name, "process_skipable_error")
		}
		return err
	}
	if result == nil {
		w.logger.Info().Interface("req", msg.Payload).Msg("process result is nil")
		return nil
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

	return nil
}

func (w *pollWorkreImplDL[Req, Res]) StartLag(ctx context.Context, poller Poller[Req]) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			w.logger.Info().Msg("lag consumer dl stopped")
		case <-ticker.C:
			lag, err := poller.GetLag(ctx)
			if err != nil {
				w.logger.Debug().Err(err).Msg("could not get kafka lag")
			} else {
				w.metrics.SetKafkaLag(w.name, lag)
			}
		}
	}
}

func (w *pollWorkreImplDL[Req, Res]) Start(ctx context.Context, poller Poller[Req], processor Processor[Req, Res], writer Writer[Res]) error {
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

func (w *pollWorkreImplDL[Req, Res]) Stop(ctx context.Context) error {
	return nil
}

func NewPollWorkerDL[Req any, Res any](
	logger *zerolog.Logger,
	tickTimeout time.Duration,
	pollTimeout time.Duration,
	m *metrics.WorkerMetrics,
	name string,
) PollWorker[Req, Res] {
	return &pollWorkreImplDL[Req, Res]{
		logger:      logger,
		tickTimeout: tickTimeout,
		pollTimeout: pollTimeout,
		metrics:     metrics.WorkerMetricsOrNoop(m),
		name:        name,
	}
}
