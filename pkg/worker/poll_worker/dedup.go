package poll_worker

import (
	"context"
	"time"
)

const (
	dedupMessageTTL = 24 * time.Hour
	dedupKeyPrefix  = "processed-message"
)

// DedupStore provides the atomic operations needed for deduplicating Kafka
// messages: an atomic check-and-set (so only one consumer marks a message as
// processed) and delete (to allow reprocessing after a failure).
type DedupStore interface {
	SetValueNX(ctx context.Context, key string, value any, expiration time.Duration) (bool, error)
	DeleteValue(ctx context.Context, key string) error
}
