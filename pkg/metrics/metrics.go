package metrics

import (
	"errors"

	"github.com/ARM-auto-place/libs/pkg/cache"
	"github.com/prometheus/client_golang/prometheus"
)

var ErrDuplicateMetricName = errors.New("duplicate metric name")

type Collector[T any] struct {
	Source *cache.RistrettoCache[T]

	Metrics []Metric[T]
}
type Desc[T any] struct {
	Name      string
	Help      string
	ValueType prometheus.ValueType

	Extractor MetricValueExtractor[T]
}

type MetricValueExtractor[T any] func(m *cache.RistrettoCache[T]) float64

type Metric[T any] struct {
	Desc      *prometheus.Desc
	ValueType prometheus.ValueType
	Extractor MetricValueExtractor[T]
}

// Параметры NewMetricsCollector[T any]:
//
//   - source — кэш (RistrettoCache[T]), из статистики которого собираются метрики;
//   - namespace — namespace метрик;
//   - subsystem — subsystem метрик;
//   - constLabels — константные лейблы, добавляемые ко всем метрикам.
func NewMetricsCollector[T any](source *cache.RistrettoCache[T], namespace, subsystem string, constLabels prometheus.Labels) (*Collector[T], error) {
	var conf Config[T]
	conf.apply(namespace, subsystem, constLabels)

	uniqFQNames := make(map[string]struct{})
	metrics := make([]Metric[T], 0, len(conf.Metrics))
	for _, c := range conf.Metrics {
		fqName := prometheus.BuildFQName(conf.Namespace, conf.Subsystem, c.Name)
		if _, ok := uniqFQNames[fqName]; ok {
			return nil, ErrDuplicateMetricName
		}
		uniqFQNames[fqName] = struct{}{}

		metrics = append(metrics, Metric[T]{
			Desc:      prometheus.NewDesc(fqName, c.Help, nil, conf.ConstLabels),
			ValueType: c.ValueType,
			Extractor: c.Extractor,
		})
	}

	return &Collector[T]{
		Source:  source,
		Metrics: metrics,
	}, nil
}
func (c Collector[T]) Describe(ch chan<- *prometheus.Desc) {
	for _, m := range c.Metrics {
		ch <- m.Desc
	}
}

func (c Collector[T]) Collect(ch chan<- prometheus.Metric) {
	if c.Source == nil {
		return
	}
	for _, m := range c.Metrics {
		ch <- prometheus.MustNewConstMetric(m.Desc, m.ValueType, m.Extractor(c.Source))
	}
}
