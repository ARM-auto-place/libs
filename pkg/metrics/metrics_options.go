package metrics

import (
	"github.com/ARM-auto-place/libs/pkg/cache"
	"github.com/prometheus/client_golang/prometheus"
)

type Option[T any] func(*Config[T])

type Config[T any] struct {
	Namespace string
	Subsystem string

	ConstLabels prometheus.Labels

	Metrics []Desc[T]
}

func (c *Config[T]) apply(namespace, subsystem string, constLabels prometheus.Labels) {
	WithNamespace[T](namespace)(c)
	WithSubsystem[T](subsystem)(c)
	WithHitsCounterMetric[T]()(c)
	WithMissesCounterMetric[T]()(c)
	WithConstLabels[T](constLabels)(c)
	WithMetric(Desc[T]{
		Name:      "cache_keys_added_total",
		Help:      "The number of added keys in the cache.",
		ValueType: prometheus.CounterValue,
		Extractor: func(m *cache.RistrettoCache[T]) float64 { return float64(m.KeysAdded()) },
	})(c)
	WithHitsRatioGaugeMetric[T]()(c)
	WithCostEvictedMetric[T]()(c)
	WithKeysEvictedMetric[T]()(c)
}

func WithNamespace[T any](namespace string) Option[T] {
	return func(o *Config[T]) {
		o.Namespace = namespace
	}
}

func WithSubsystem[T any](subsystem string) Option[T] {
	return func(o *Config[T]) {
		o.Subsystem = subsystem
	}
}

func WithConstLabels[T any](constLabels prometheus.Labels) Option[T] {
	return func(o *Config[T]) {
		o.ConstLabels = constLabels
	}
}

func WithHitsCounterMetric[T any]() Option[T] {
	return WithMetric(Desc[T]{
		Name:      "cache_hits_total",
		Help:      "The number of Get calls where a value was found for the corresponding key.",
		ValueType: prometheus.CounterValue,
		Extractor: func(m *cache.RistrettoCache[T]) float64 { return float64(m.Hits()) },
	})
}

func WithMissesCounterMetric[T any]() Option[T] {
	return WithMetric(Desc[T]{
		Name:      "cache_misses_total",
		Help:      "The number of Get calls where a value was not found for the corresponding key.",
		ValueType: prometheus.CounterValue,
		Extractor: func(m *cache.RistrettoCache[T]) float64 { return float64(m.Misses()) },
	})
}

func WithHitsRatioGaugeMetric[T any]() Option[T] {
	return WithMetric(Desc[T]{
		Name:      "cache_hits_ratio",
		Help:      "The percentage of successful Get calls (hits).",
		ValueType: prometheus.GaugeValue,
		Extractor: func(m *cache.RistrettoCache[T]) float64 { return m.Ratio() },
	})
}

func WithKeysEvictedMetric[T any]() Option[T] {
	return WithMetric(Desc[T]{
		Name:      "cache_keys_evicted_total",
		Help:      "The number of evicted keys from the cache.",
		ValueType: prometheus.CounterValue,
		Extractor: func(m *cache.RistrettoCache[T]) float64 { return float64(m.KeysEvicted()) },
	})
}

func WithCostAddedMetric[T any]() Option[T] {
	return WithMetric(Desc[T]{
		Name:      "cache_cost_added_total",
		Help:      "The sum of costs that have been added.",
		ValueType: prometheus.CounterValue,
		Extractor: func(m *cache.RistrettoCache[T]) float64 { return float64(m.CostAdded()) },
	})
}

func WithCostEvictedMetric[T any]() Option[T] {
	return WithMetric(Desc[T]{
		Name:      "cache_cost_evicted_total",
		Help:      "The sum of all costs that have been evicted.",
		ValueType: prometheus.CounterValue,
		Extractor: func(m *cache.RistrettoCache[T]) float64 { return float64(m.CostEvicted()) },
	})
}

func WithMetric[T any](d Desc[T]) Option[T] {
	return func(o *Config[T]) {
		o.Metrics = append(o.Metrics, d)
	}
}
