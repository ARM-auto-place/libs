package worker

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog"
)

// Message represents a generic message that can be polled and committed
type Message[T any] struct {
	Payload T
	// Add any other generic message metadata if needed
	Metadata any
}

type Reader[T any] interface {
	Read(ctx context.Context, count uint64, tx *pgx.Tx) ([]T, error)
}

type Processor[T any, R any] interface {
	Process(ctx context.Context, t T, tx *pgx.Tx) (*R, error)
}

type Writer[R any] interface {
	Write(ctx context.Context, result R, tx *pgx.Tx) error
	Update(ctx context.Context, result R) error
}

type TickerWorker[T any, R any] interface {
	Start(ctx context.Context, reader Reader[T], processor Processor[T, R], writer Writer[R]) error
	Stop(ctx context.Context) error
}

type Poller[T any] interface {
	Connect(ctx context.Context) error
	// Poll returns a Message containing the payload and metadata needed for committing
	Poll(ctx context.Context, wait time.Duration) (*Message[T], error)
	Commit(ctx context.Context, msg *Message[T]) error
	// GetLag returns the current consumer lag
	GetLag(ctx context.Context) (float64, error)
}

type PollWorker[Req any, Res any] interface {
	Start(ctx context.Context, poller Poller[Req], processor Processor[Req, Res], writer Writer[Res]) error
	Stop(ctx context.Context) error
}

type ReaderWorkerParams struct {
	Logger      *zerolog.Logger
	Ticker      *time.Ticker
	BatchSize   uint64
	WorkerType  string
	WorkerCount int

	RunImmediately     bool
	ContextTimeout     time.Duration
	ItemProcessTimeout time.Duration
}
