package cache

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/ARM-auto-place/libs/pkg/config"
	"github.com/stretchr/testify/assert"
	gomock "go.uber.org/mock/gomock"
)

func Test_RistrettoCache(t *testing.T) {
	cfg := &config.Config{
		NumCounters: 100,
		MaxCost:     1000,
		BufferItems: 64,
	}

	t.Run("successful set and get", func(t *testing.T) {
		cache, err := NewRistrettoCache[string](cfg)
		assert.NoError(t, err)

		ctx := context.Background()
		key := "test_key"
		value := "test_value"

		err = cache.Set(ctx, key, value, time.Minute)
		assert.NoError(t, err)
		cache.Wait()

		result, err := cache.Get(ctx, key)
		assert.NoError(t, err)
		assert.Equal(t, value, *result)
	})

	t.Run("get non-existent key", func(t *testing.T) {
		cache, err := NewRistrettoCache[int](cfg)
		assert.NoError(t, err)

		_, err = cache.Get(context.Background(), "missing_key")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "value not found")
	})

}

func Test_WithFallbackCache(t *testing.T) {
	ctx := context.Background()
	const testKey = "test_key"
	testValue := "test_value"

	t.Run("get from cache", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mock := NewMockCache[string](ctrl)

		fallbackCalled := false
		fbCache := NewFallbackCache(
			mock,
			func(ctx context.Context, key string) (*string, error) {
				fallbackCalled = true
				return nil, errors.New("should not be called")
			},
		)
		mock.EXPECT().Get(ctx, testKey).Return(&testValue, nil)
		val, err := fbCache.Get(ctx, testKey)
		assert.NoError(t, err)
		assert.Equal(t, testValue, *val)
		assert.False(t, fallbackCalled)
	})

	t.Run("fallback on cache miss", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mock := NewMockCache[string](ctrl)
		fallbackValue := "fallback_value"

		fbCache := NewFallbackCache[string](
			mock,
			func(ctx context.Context, key string) (*string, error) {
				return &fallbackValue, nil
			},
		)
		mock.EXPECT().Get(ctx, testKey).Return(&fallbackValue, nil)
		val, err := fbCache.Get(ctx, testKey)
		assert.NoError(t, err)
		assert.Equal(t, fallbackValue, *val)

		mock.EXPECT().Get(ctx, testKey).Return(&fallbackValue, nil)
		cachedVal, err := mock.Get(ctx, testKey)
		assert.NoError(t, err)
		assert.Equal(t, fallbackValue, *cachedVal)
	})

	t.Run("fallback error propagates", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mock := NewMockCache[string](ctrl)
		expectedErr := errors.New("fallback failed")

		fbCache := NewFallbackCache(
			mock,
			func(ctx context.Context, key string) (*string, error) {
				return nil, expectedErr
			},
		)
		mock.EXPECT().Get(ctx, testKey).Return(nil, expectedErr)
		_, err := fbCache.Get(ctx, testKey)
		assert.Equal(t, expectedErr, err)
	})

	t.Run("set error after successful fallback", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mock := NewMockCache[string](ctrl)
		fallbackValue := "fallback_value"

		fbCache := NewFallbackCache(
			mock,
			func(ctx context.Context, key string) (*string, error) {
				return &fallbackValue, nil
			},
		)
		mock.EXPECT().Set(ctx, testKey, fallbackValue, 10*time.Minute).Return(fmt.Errorf("set error"))
		mock.EXPECT().Get(ctx, testKey).Return(nil, fmt.Errorf("set error"))
		val, err := fbCache.Get(ctx, testKey)
		assert.Error(t, err)
		assert.Equal(t, "set error", err.Error())
		assert.Nil(t, val)
	})

	t.Run("set operation", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mock := NewMockCache[string](ctrl)
		fbCache := NewFallbackCache(
			mock,
			nil,
		)
		mock.EXPECT().Set(ctx, testKey, testValue, time.Minute).Return(nil)
		err := fbCache.Set(ctx, testKey, testValue, time.Minute)
		assert.NoError(t, err)
		mock.EXPECT().Get(ctx, testKey).Return(&testValue, nil)
		val, err := mock.Get(ctx, testKey)
		assert.NoError(t, err)
		assert.Equal(t, testValue, *val)
	})
}
