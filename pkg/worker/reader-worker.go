package worker

import (
	"context"
	"errors"
	"fmt"

	"reflect"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog"
	"golang.org/x/sync/errgroup"

	"github.com/ARM-auto-place/libs/pkg/metrics"
)

const loopTimeout = 1 * time.Second

var ErrEmptyItems = errors.New("items is empty")

type readerWorker[Req any, Res any] struct {
	logger    *zerolog.Logger
	batchSize uint64
	wg        *sync.WaitGroup
	ticker    *time.Ticker
	metrics   metrics.IWorkerMetrics
	name      string

	startImmediately bool
	workerCount      int

	contextTimeout     time.Duration
	itemProcessTimeout time.Duration
	getTransaction     func(ctx context.Context, isMaster bool) (pgx.Tx, error)
}

func (w *readerWorker[Req, Res]) Start(
	ctx context.Context,
	reader Reader[Req],
	processor Processor[Req, Res],
	writer Writer[Res],
) error {
	for i := 0; i < w.workerCount; i++ {
		w.wg.Go(func() {
			w.startWorker(ctx, i+1, reader, processor, writer)
		})
	}

	w.logger.Debug().Int("num_workers", w.workerCount).Msg("all workers started")
	return nil
}

func (w *readerWorker[Req, Res]) startWorker(
	ctx context.Context,
	workerID int,
	reader Reader[Req],
	processor Processor[Req, Res],
	writer Writer[Res],
) {
	w.logger.Debug().Int("worker_id", workerID).Msg("worker started")

	if w.startImmediately {
		w.tickLoop(ctx, workerID, reader, processor, writer)
	}

	for {
		select {
		case <-w.ticker.C:
			w.tickLoop(ctx, workerID, reader, processor, writer)
		case <-ctx.Done():
			return
		}
	}
}

func (w *readerWorker[Req, Res]) tickLoop(
	ctx context.Context,
	workerID int,
	reader Reader[Req],
	processor Processor[Req, Res],
	writer Writer[Res],
) {
	w.logger.Debug().Msg("start processing items")
	cycleStart := time.Now()
	count := 0

	for {
		if err := w.Tick(ctx, reader, processor, writer); err != nil {
			if errors.Is(err, ErrEmptyItems) {
				w.logger.Debug().Msgf("process is completed, item counts = %d, time = %s", count, time.Since(cycleStart))
				return
			}
			w.metrics.SetHealth(w.name, 0)
			w.logger.Error().Err(err).Int("worker_id", workerID).Msg("could not do the tick")
		}
		time.Sleep(loopTimeout)
	}
}

func (w *readerWorker[Req, Res]) Tick(
	headCtx context.Context,
	reader Reader[Req],
	processor Processor[Req, Res],
	writer Writer[Res],
) error {
	ctx, cancel := context.WithTimeout(headCtx, w.contextTimeout)
	defer cancel()

	w.metrics.SetHealth(w.name, 1)

	var tx *pgx.Tx
	var lastResult *Res
	if w.getTransaction != nil {
		t, err := w.getTransaction(ctx, true)
		if err != nil {
			return err
		}
		tx = &t

		defer func() {
			(*tx).Rollback(ctx)
			// Update need if we want to do smthng after rollback
			if lastResult != nil {
				err = writer.Update(ctx, *lastResult)
				if err != nil {
					w.logger.Error().Interface("result", lastResult).Err(err).Msg("could not update a processing result")
				}
			}
		}()
	}

	readStart := time.Now()
	defer func() {
		w.metrics.ObserveProcessDuration(w.name, time.Since(readStart))
	}()
	items, err := reader.Read(ctx, w.batchSize, tx)
	if err != nil {
		w.metrics.IncError(w.name, "read_error")
		return fmt.Errorf("could not read items: %w", err)
	}
	w.metrics.ObserveStepDuration(w.name, metrics.StepRead, time.Since(readStart))

	if len(items) == 0 {
		return ErrEmptyItems
	}

	w.logger.Info().Msgf("processing %d items", len(items))

	if tx != nil {
		for _, item := range items {
			w.logger.Info().Interface("item", MsgItem(item)).Msg("processing item")

			res, err := w.processItem(ctx, item, tx, processor, writer)
			if err != nil {
				return err
			}
			if res != nil {
				lastResult = res
			}
		}

		if err := (*tx).Commit(ctx); err != nil {
			return fmt.Errorf("could not commit transaction: %w", err)
		}

		w.logger.Debug().Msg("all items are processed")
		return nil
	}

	// Without a transaction the batch can be processed concurrently.
	eg, eCtx := errgroup.WithContext(ctx)
	for _, item := range items {
		w.logger.Info().Interface("item", MsgItem(item)).Msg("processing item")

		item := item
		eg.Go(func() error {
			_, err := w.processItem(eCtx, item, nil, processor, writer)
			return err
		})
	}

	w.logger.Debug().Msg("waiting for all items to be processed")
	if err := eg.Wait(); err != nil {
		return fmt.Errorf("could process items: %w", err)
	}

	w.logger.Debug().Msg("all items are processed")

	return nil
}

func (w *readerWorker[Req, Res]) processItem(
	ctx context.Context,
	item Req,
	tx *pgx.Tx,
	processor Processor[Req, Res],
	writer Writer[Res],
) (*Res, error) {
	w.metrics.IncActiveWorkers(w.name)
	defer w.metrics.DecActiveWorkers(w.name)

	processCtx, cancel := context.WithTimeout(ctx, w.itemProcessTimeout)
	defer cancel()

	processStart := time.Now()
	result, err := processor.Process(processCtx, item, tx)
	if err != nil {
		if errors.Is(err, ErrWarning) {
			w.logger.Warn().Interface("item", MsgItem(item)).Err(err).Msg("could not process item")
			w.metrics.IncError(w.name, "process_warning")
		} else {
			w.logger.Error().Interface("item", MsgItem(item)).Err(err).Msg("could not process item")
			w.metrics.IncError(w.name, "process_error")
			w.metrics.ObserveStepDuration(w.name, metrics.StepProcess, time.Since(processStart))
			return nil, fmt.Errorf("could process item %v: %w", MsgItem(item), err)
		}
	}
	w.metrics.ObserveStepDuration(w.name, metrics.StepProcess, time.Since(processStart))

	if result != nil {
		writeStart := time.Now()
		if err := writer.Write(processCtx, *result, tx); err != nil {
			w.logger.Warn().Interface("item", MsgItem(item)).Interface("result", result).Err(err).Msg("could not write a processing result")
			w.metrics.IncError(w.name, "write_error")
			w.metrics.ObserveStepDuration(w.name, metrics.StepWrite, time.Since(writeStart))
			return result, fmt.Errorf("could not write result for item %v: %w", MsgItem(item), err)
		}
		w.metrics.ObserveStepDuration(w.name, metrics.StepWrite, time.Since(writeStart))
	}

	w.metrics.IncProcessedCount(w.name)

	return result, nil
}

func (w *readerWorker[Req, Res]) Stop(ctx context.Context) error {
	w.ticker.Stop()
	w.wg.Wait()
	return nil
}

// Параметры NewReaderWorker[Req any, Res any]:
//
//   - m — метрики воркера (metrics.WorkerMetrics). Если nil — используется
//     no-op реализация, метрики не собираются;
//   - params — настройки воркера (ReaderWorkerParams): Logger (логгер воркера),
//     Ticker (тикер, по которому запускается обработка батчей),
//     BatchSize (максимум элементов, читаемых за один тик),
//     WorkerType (имя воркера — name-label метрики),
//     WorkerCount (количество параллельных воркеров, по умолчанию 1),
//     RunImmediately (запускать ли первую обработку сразу),
//     ContextTimeout (общий таймаут на один тик),
//     ItemProcessTimeout (таймаут на обработку одного элемента);
//   - getTransaction — фабрика транзакции БД. Если nil — воркер работает без
//     транзакции и обрабатывает батч конкурентно. Если передан — каждый батч
//     выполняется в одной транзакции, элементы обрабатываются последовательно.
func NewReaderWorker[Req any, Res any](
	m *metrics.WorkerMetrics,
	params ReaderWorkerParams,
	getTransaction func(ctx context.Context, isMaster bool) (pgx.Tx, error),
) TickerWorker[Req, Res] {
	if params.ItemProcessTimeout == 0 {
		params.ItemProcessTimeout = params.ContextTimeout
	}
	if params.WorkerCount == 0 {
		params.WorkerCount = 1
	}

	return &readerWorker[Req, Res]{
		logger:             params.Logger,
		ticker:             params.Ticker,
		batchSize:          params.BatchSize,
		wg:                 &sync.WaitGroup{},
		metrics:            metrics.WorkerMetricsOrNoop(m),
		name:               params.WorkerType,
		contextTimeout:     params.ContextTimeout,
		itemProcessTimeout: params.ItemProcessTimeout,
		startImmediately:   params.RunImmediately,
		getTransaction:     getTransaction,
		workerCount:        params.WorkerCount,
	}
}

func MsgItem(item any) any {
	itemType := reflect.ValueOf(item)

	if itemType.Kind() == reflect.Slice || itemType.Kind() == reflect.Array {
		itemLen := itemType.Len()

		if itemLen < 2 {
			return item
		}

		return map[string]any{
			"len": itemLen,
		}
	}

	return item
}
