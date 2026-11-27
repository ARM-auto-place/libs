package metrics

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ARM-auto-place/libs/pkg/cache"
	"github.com/ARM-auto-place/libs/pkg/config"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
)

func Test_CacheMetrics(t *testing.T) {
	cfg := &config.Config{
		NumCounters: 1000,
		MaxCost:     1 << 20,
		BufferItems: 200,
		Metrics:     true,
	}
	ctx := context.Background()
	t.Run("successful set and get", func(t *testing.T) {
		cache, err := cache.NewRistrettoCache[string](cfg)
		assert.NoError(t, err)

		ristrettoCollector, err := NewMetricsCollector(cache, "monetka", "test", prometheus.Labels{})
		assert.NoError(t, err)
		for i := 0; i < 123; i++ {
			cache.Set(ctx,
				fmt.Sprintf("key%d", i),
				fmt.Sprintf("val%d", i),
				1*time.Minute,
			)
		}

		cache.Wait()

		for i := 50; i < 99; i++ {
			key := fmt.Sprintf("key%d", i)
			if _, err := cache.Get(ctx, key); err != nil {
				assert.NoError(t, fmt.Errorf("expected key: %s", key))
			}
		}

		for i := 150; i < 170; i++ {
			key := fmt.Sprintf("key%d", i)
			if _, err := cache.Get(ctx, key); err != nil {
				assert.Error(t, fmt.Errorf("unexpected key: %s", key))
			}
		}
		expected := `
		# HELP monetka_test_cache_hits_ratio The percentage of successful Get calls (hits).
		# TYPE monetka_test_cache_hits_ratio gauge
        monetka_test_cache_hits_ratio 0.7101449275362319
        # HELP monetka_test_cache_hits_total The number of Get calls where a value was found for the corresponding key.
        # TYPE monetka_test_cache_hits_total counter
        monetka_test_cache_hits_total 49
		# HELP monetka_test_cache_keys_added_total The number of added keys in the cache.
        # TYPE monetka_test_cache_keys_added_total counter
        monetka_test_cache_keys_added_total 123
        # HELP monetka_test_cache_misses_total The number of Get calls where a value was not found for the corresponding key.
        # TYPE monetka_test_cache_misses_total counter
        monetka_test_cache_misses_total 20
	`

		if err := testutil.CollectAndCompare(ristrettoCollector, strings.NewReader(expected)); err != nil {
			assert.NoError(t, err)
		}
	})
}
