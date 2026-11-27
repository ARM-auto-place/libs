package worker

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
)

type testModels struct {
	id   uint64
	Name string
}

type TestWorkerService interface {
	Reader[testModels]
	Writer[testModels]
	Processor[testModels, testModels]
}

type testProcessor struct{}

func NewProcessor() TestWorkerService {
	return &testProcessor{}
}

func (s *testProcessor) Process(ctx context.Context, testInfo testModels, tx *pgx.Tx) (*testModels, error) {
	time.Sleep(100 * time.Millisecond)

	return &testInfo, nil
}

func (s *testProcessor) Read(ctx context.Context, count uint64, tx *pgx.Tx) ([]testModels, error) {
	items := make([]testModels, count)
	for i := 0; i < int(count); i++ {
		items[i] = testModels{
			id:   uint64(i),
			Name: fmt.Sprintf("name%d", i),
		}
	}

	return items, nil
}

func (s *testProcessor) Write(ctx context.Context, testInfo testModels, tx *pgx.Tx) error {
	return nil
}
func (s *testProcessor) Update(ctx context.Context, testInfo testModels) error {
	return nil
}

type TestWorkerSliceService interface {
	Reader[[]testModels]
	Writer[[]testModels]
	Processor[[]testModels, []testModels]
}

type testSliceProcessor struct{}

func NewSliceProcessor() TestWorkerSliceService {
	return &testSliceProcessor{}
}

func (s *testSliceProcessor) Process(ctx context.Context, testInfo []testModels, tx *pgx.Tx) (*[]testModels, error) {
	time.Sleep(100 * time.Millisecond)

	return &testInfo, nil
}

func (s *testSliceProcessor) Read(ctx context.Context, count uint64, tx *pgx.Tx) ([][]testModels, error) {
	items := make([]testModels, count)
	for i := 0; i < int(count); i++ {
		items[i] = testModels{
			id:   uint64(i),
			Name: fmt.Sprintf("name%d", i),
		}
	}

	res := [][]testModels{items}
	return res, nil
}

func (s *testSliceProcessor) Write(ctx context.Context, testInfo []testModels, tx *pgx.Tx) error {
	return nil
}
func (s *testSliceProcessor) Update(ctx context.Context, testInfo []testModels) error {
	return nil
}

// тесты для проверки руками
func Test_ReaderWorker(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	t.Run("true", func(t *testing.T) {
		ctx := context.Background()

		params := ReaderWorkerParams{
			Ticker:             time.NewTicker(100 * time.Millisecond),
			BatchSize:          1,
			WorkerType:         "some",
			RunImmediately:     false,
			ContextTimeout:     3 * time.Second,
			ItemProcessTimeout: 1 * time.Second,
		}

		processor := NewProcessor()
		service := NewReaderWorker[testModels, testModels](
			nil,
			params,
			nil,
		)

		wg := &sync.WaitGroup{}

		wg.Add(1)
		go func() {
			defer wg.Done()
			newCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
			defer cancel()
			log.Info().Msg("Start worker")
			err := service.Start(newCtx, processor, processor, processor)
			assert.NoError(t, err)
		}()

		wg.Wait()
	})

	t.Run("true with immediately", func(t *testing.T) {
		ctx := context.Background()

		params := ReaderWorkerParams{
			Ticker:             time.NewTicker(100 * time.Millisecond),
			BatchSize:          1,
			WorkerType:         "some",
			RunImmediately:     true,
			ContextTimeout:     3 * time.Second,
			ItemProcessTimeout: 1 * time.Second,
		}

		processor := NewProcessor()
		service := NewReaderWorker[testModels, testModels](
			nil,
			params,
			nil,
		)

		wg := &sync.WaitGroup{}

		wg.Add(1)
		go func() {
			defer wg.Done()
			newCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
			defer cancel()
			log.Info().Msg("Start worker")
			err := service.Start(newCtx, processor, processor, processor)
			assert.NoError(t, err)
		}()

		wg.Wait()
	})

	t.Run("true items=slice", func(t *testing.T) {
		ctx := context.Background()

		params := ReaderWorkerParams{
			Ticker:             time.NewTicker(100 * time.Millisecond),
			BatchSize:          400000,
			WorkerType:         "some",
			RunImmediately:     true,
			ContextTimeout:     3 * time.Second,
			ItemProcessTimeout: 1 * time.Second,
		}

		processor := NewSliceProcessor()
		service := NewReaderWorker[[]testModels, []testModels](
			nil,
			params,
			nil,
		)

		wg := &sync.WaitGroup{}

		wg.Add(1)
		go func() {
			defer wg.Done()
			newCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
			defer cancel()
			log.Info().Msg("Start worker")
			err := service.Start(newCtx, processor, processor, processor)
			assert.NoError(t, err)
		}()

		wg.Wait()
	})

	t.Run("true items=slice with 1", func(t *testing.T) {
		ctx := context.Background()

		params := ReaderWorkerParams{
			Ticker:             time.NewTicker(100 * time.Millisecond),
			BatchSize:          1,
			WorkerType:         "some",
			RunImmediately:     true,
			ContextTimeout:     3 * time.Second,
			ItemProcessTimeout: 1 * time.Second,
		}

		processor := NewSliceProcessor()
		service := NewReaderWorker[[]testModels, []testModels](
			nil,
			params,
			nil,
		)

		wg := &sync.WaitGroup{}

		wg.Add(1)
		go func() {
			defer wg.Done()
			newCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
			defer cancel()
			log.Info().Msg("Start worker")
			err := service.Start(newCtx, processor, processor, processor)
			assert.NoError(t, err)
		}()

		wg.Wait()
	})
}
