package cache

import (
	"context"
	"time"

	"github.com/ARM-auto-place/libs/pkg/interfaces"
)

const countDown = 10 * time.Minute

type WithFallbackCache[T any] struct {
	Cache    interfaces.Cache[T]
	Fallback func(ctx context.Context, key string) (*T, error)
}

func (c *WithFallbackCache[T]) Get(ctx context.Context, key string) (*T, error) {
	value, err := c.Cache.Get(ctx, key)
	if err != nil {
		value, err = c.Fallback(ctx, key)
		if err != nil {
			return nil, err
		}
		err = c.Cache.Set(ctx, key, *value, countDown)
		if err != nil {
			return nil, err
		}
	}
	return value, nil
}

func (c *WithFallbackCache[T]) Set(ctx context.Context, key string, value T, ttl time.Duration) error {
	return c.Cache.Set(ctx, key, value, ttl)
}

func (c *WithFallbackCache[T]) Wait() {
	c.Cache.Wait()
}
func (c *WithFallbackCache[T]) Remove(ctx context.Context, key string) {
	c.Cache.Remove(key)

}
func (c *WithFallbackCache[T]) Clear() {
	c.Cache.Clear()
}

// Параметры NewFallbackCache[T any]:
//
//   - cache — основной кэш (interfaces.Cache[T]: Get/Set/Wait/Remove/Clear и
//     статистика обращений), из которого читается значение до вызова fallback;
//   - fallbackFunc — источник данных, вызываемый при промахе в основном кэше,
//     возвращает значение или ошибку.
func NewFallbackCache[T any](
	cache interfaces.Cache[T],
	fallbackFunc func(ctx context.Context, key string) (*T, error),
) *WithFallbackCache[T] {
	return &WithFallbackCache[T]{
		Cache:    cache,
		Fallback: fallbackFunc,
	}
}
