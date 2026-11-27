package cache

import (
	"context"
	"fmt"
	"time"

	"github.com/ARM-auto-place/libs/pkg/config"
	"github.com/dgraph-io/ristretto/v2"
)

type RistrettoCache[T any] struct {
	ristretto *ristretto.Cache[string, T]
}

func (c *RistrettoCache[T]) Get(ctx context.Context, key string) (*T, error) {
	value, ok := c.ristretto.Get(key)
	if !ok {
		return nil, fmt.Errorf("value not found in cache")
	}
	return &value, nil
}

func (c *RistrettoCache[T]) Set(ctx context.Context, key string, value T, ttl time.Duration) error {
	ok := c.ristretto.SetWithTTL(key, value, 0, ttl)
	if !ok {
		return fmt.Errorf("failed to set value in cache")
	}
	return nil
}

func (c *RistrettoCache[T]) Wait() {
	c.ristretto.Wait()
}
func (c *RistrettoCache[T]) Remove(key string) {
	c.ristretto.Del(key)

}

func (c *RistrettoCache[T]) Clear() {
	c.ristretto.Clear()
}

func (c *RistrettoCache[T]) Misses() uint64 {
	return c.ristretto.Metrics.Misses()
}
func (c *RistrettoCache[T]) Hits() uint64 {
	return c.ristretto.Metrics.Hits()
}
func (c *RistrettoCache[T]) Ratio() float64 {
	return c.ristretto.Metrics.Ratio()
}
func (c *RistrettoCache[T]) KeysEvicted() uint64 {
	return c.ristretto.Metrics.KeysEvicted()
}
func (c *RistrettoCache[T]) CostAdded() uint64 {
	return c.ristretto.Metrics.CostAdded()
}
func (c *RistrettoCache[T]) CostEvicted() uint64 {
	return c.ristretto.Metrics.CostEvicted()
}
func (c *RistrettoCache[T]) KeysAdded() uint64 {
	return c.ristretto.Metrics.KeysAdded()
}

// Параметры NewRistrettoCache[T any]:
//
//   - config — параметры кэша (config.Config): NumCounters (количество счётчиков,
//     примерный верхний предел ключей), MaxCost (максимальный размер кэша в
//     байтах), BufferItems (размер буфера очереди команд), DefaultTTL (TTL по
//     умолчанию), Metrics (собирать ли метрики).
func NewRistrettoCache[T any](config *config.Config) (*RistrettoCache[T], error) {
	ristretto, err := ristretto.NewCache(&ristretto.Config[string, T]{
		NumCounters: config.NumCounters,
		MaxCost:     config.MaxCost,
		BufferItems: config.BufferItems,
		Metrics:     config.Metrics,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create cache: %w", err)
	}

	return &RistrettoCache[T]{ristretto: ristretto}, nil
}
