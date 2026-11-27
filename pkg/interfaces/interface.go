package interfaces

import (
	"context"
	"time"
)

type Cache[T any] interface {
	Get(ctx context.Context, key string) (*T, error)
	Set(ctx context.Context, key string, value T, ttl time.Duration) error
	Wait()
	Remove(key string)
	Clear()
	Misses() uint64
	Hits() uint64
	Ratio() float64
	KeysEvicted() uint64
	CostAdded() uint64
	CostEvicted() uint64
	KeysAdded() uint64
}
