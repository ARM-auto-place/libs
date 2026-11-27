package poll_worker

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ARM-auto-place/libs/pkg/worker"
)

// Message represents a generic message that can be polled and committed
type Message[T any] struct {
	Payload T
	// Add any other generic message metadata if needed
	Metadata *kgo.Record
}

type Poller[T any] interface {
	// Poll returns a Message containing the payload and metadata needed for committing
	Poll(ctx context.Context) ([]Message[T], error)
	Commit(ctx context.Context, msg Message[T]) error
	// GetLag returns the current consumer lag
	GetLag(ctx context.Context) (float64, error)
	Shutdown()
}

type PollWorker[Req any, Res any] interface {
	Start(ctx context.Context, stop chan struct{}, poller Poller[Req], processor worker.Processor[Req, Res], writer worker.Writer[Res]) error
	Stop(poller Poller[Req])
}
