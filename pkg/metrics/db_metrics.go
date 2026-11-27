package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type DBVecMetricOptions struct {
	Name    string
	Help    string
	Labels  []string
	Buckets []float64
}

type DBGaugeMetricOptions struct {
	Name string
	Help string
}

type DBMetricsOptions struct {
	Namespace string
	Subsystem string

	QueryDuration DBVecMetricOptions
	QueryErrors   DBVecMetricOptions
	Transactions  DBVecMetricOptions

	ConnectionsActive DBGaugeMetricOptions
	ConnectionsIdle   DBGaugeMetricOptions
	ConnectionsMax    DBGaugeMetricOptions
}

type DBMetrics struct {
	QueryDuration *prometheus.HistogramVec
	QueryErrors   *prometheus.CounterVec
	Transactions  *prometheus.CounterVec

	ConnectionsActive prometheus.Gauge
	ConnectionsIdle   prometheus.Gauge
	ConnectionsMax    prometheus.Gauge
}

func NewDBMetrics(opts DBMetricsOptions) *DBMetrics {
	m := &DBMetrics{}

	if opts.QueryDuration.Name != "" {
		m.QueryDuration = prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: opts.Namespace,
				Subsystem: opts.Subsystem,
				Name:      opts.QueryDuration.Name,
				Help:      opts.QueryDuration.Help,
				Buckets:   opts.QueryDuration.Buckets,
			},
			opts.QueryDuration.Labels,
		)
	}

	if opts.QueryErrors.Name != "" {
		m.QueryErrors = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: opts.Namespace,
				Subsystem: opts.Subsystem,
				Name:      opts.QueryErrors.Name,
				Help:      opts.QueryErrors.Help,
			},
			opts.QueryErrors.Labels,
		)
	}

	if opts.Transactions.Name != "" {
		m.Transactions = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: opts.Namespace,
				Subsystem: opts.Subsystem,
				Name:      opts.Transactions.Name,
				Help:      opts.Transactions.Help,
			},
			opts.Transactions.Labels,
		)
	}

	if opts.ConnectionsActive.Name != "" {
		m.ConnectionsActive = prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: opts.Namespace,
			Subsystem: opts.Subsystem,
			Name:      opts.ConnectionsActive.Name,
			Help:      opts.ConnectionsActive.Help,
		})
	}

	if opts.ConnectionsIdle.Name != "" {
		m.ConnectionsIdle = prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: opts.Namespace,
			Subsystem: opts.Subsystem,
			Name:      opts.ConnectionsIdle.Name,
			Help:      opts.ConnectionsIdle.Help,
		})
	}

	if opts.ConnectionsMax.Name != "" {
		m.ConnectionsMax = prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: opts.Namespace,
			Subsystem: opts.Subsystem,
			Name:      opts.ConnectionsMax.Name,
			Help:      opts.ConnectionsMax.Help,
		})
	}

	return m
}

func (m *DBMetrics) Collectors() []prometheus.Collector {
	if m == nil {
		return nil
	}

	collectors := make([]prometheus.Collector, 0, 6)
	if m.QueryDuration != nil {
		collectors = append(collectors, m.QueryDuration)
	}
	if m.QueryErrors != nil {
		collectors = append(collectors, m.QueryErrors)
	}
	if m.Transactions != nil {
		collectors = append(collectors, m.Transactions)
	}
	if m.ConnectionsActive != nil {
		collectors = append(collectors, m.ConnectionsActive)
	}
	if m.ConnectionsIdle != nil {
		collectors = append(collectors, m.ConnectionsIdle)
	}
	if m.ConnectionsMax != nil {
		collectors = append(collectors, m.ConnectionsMax)
	}

	return collectors
}

func (m *DBMetrics) Register() error {
	if m == nil {
		return nil
	}

	if m.QueryDuration != nil {
		if err := prometheus.Register(m.QueryDuration); err != nil {
			return err
		}
	}
	if m.QueryErrors != nil {
		if err := prometheus.Register(m.QueryErrors); err != nil {
			return err
		}
	}
	if m.Transactions != nil {
		if err := prometheus.Register(m.Transactions); err != nil {
			return err
		}
	}
	if m.ConnectionsActive != nil {
		if err := prometheus.Register(m.ConnectionsActive); err != nil {
			return err
		}
	}
	if m.ConnectionsIdle != nil {
		if err := prometheus.Register(m.ConnectionsIdle); err != nil {
			return err
		}
	}
	if m.ConnectionsMax != nil {
		if err := prometheus.Register(m.ConnectionsMax); err != nil {
			return err
		}
	}

	return nil
}

func (m *DBMetrics) ObserveQueryDuration(duration time.Duration, labels prometheus.Labels) {
	if m == nil || m.QueryDuration == nil {
		return
	}

	m.QueryDuration.With(labels).Observe(duration.Seconds())
}

func (m *DBMetrics) ObserveQueryDurationValues(duration time.Duration, values ...string) {
	if m == nil || m.QueryDuration == nil {
		return
	}

	m.QueryDuration.WithLabelValues(values...).Observe(duration.Seconds())
}

func (m *DBMetrics) IncQueryError(labels prometheus.Labels) {
	if m == nil || m.QueryErrors == nil {
		return
	}

	m.QueryErrors.With(labels).Inc()
}

func (m *DBMetrics) IncQueryErrorValues(values ...string) {
	if m == nil || m.QueryErrors == nil {
		return
	}

	m.QueryErrors.WithLabelValues(values...).Inc()
}

func (m *DBMetrics) IncTransaction(labels prometheus.Labels) {
	if m == nil || m.Transactions == nil {
		return
	}

	m.Transactions.With(labels).Inc()
}

func (m *DBMetrics) IncTransactionValues(values ...string) {
	if m == nil || m.Transactions == nil {
		return
	}

	m.Transactions.WithLabelValues(values...).Inc()
}

type DBPoolStats interface {
	AcquiredConns() int32
	IdleConns() int32
	MaxConns() int32
}

func (m *DBMetrics) SetPoolStats(pools ...DBPoolStats) {
	if m == nil {
		return
	}

	var active, idle, max int32
	for _, pool := range pools {
		if pool == nil {
			continue
		}

		active += pool.AcquiredConns()
		idle += pool.IdleConns()
		max += pool.MaxConns()
	}

	if m.ConnectionsActive != nil {
		m.ConnectionsActive.Set(float64(active))
	}
	if m.ConnectionsIdle != nil {
		m.ConnectionsIdle.Set(float64(idle))
	}
	if m.ConnectionsMax != nil {
		m.ConnectionsMax.Set(float64(max))
	}
}

type DBPoolStatsSource[T any] interface {
	Stat() T
}

type DBPoolStatMetric[T any] struct {
	Name      string
	Help      string
	ValueType prometheus.ValueType
	Extractor func(T) float64
}

type DBPoolStatsCollector[T any] struct {
	source  DBPoolStatsSource[T]
	metrics []dbPoolStatMetric[T]
}

type dbPoolStatMetric[T any] struct {
	desc      *prometheus.Desc
	valueType prometheus.ValueType
	extractor func(T) float64
}

// Параметры NewDBPoolStatsCollector[T any]:
//
//   - source — источник статистики пула (DBPoolStatsSource[T]: метод Stat()
//     возвращает текущее состояние и является источником значений);
//   - namespace — namespace метрик;
//   - subsystem — subsystem метрик;
//   - constLabels — константные лейблы, добавляемые ко всем метрикам;
//   - stats — список метрик пула (DBPoolStatMetric[T]): Name, Help, ValueType и
//     Extractor — функция, достающая значение из T.
func NewDBPoolStatsCollector[T any](
	source DBPoolStatsSource[T],
	namespace string,
	subsystem string,
	constLabels prometheus.Labels,
	stats []DBPoolStatMetric[T],
) *DBPoolStatsCollector[T] {
	metrics := make([]dbPoolStatMetric[T], 0, len(stats))
	for _, stat := range stats {
		metrics = append(metrics, dbPoolStatMetric[T]{
			desc: prometheus.NewDesc(
				prometheus.BuildFQName(namespace, subsystem, stat.Name),
				stat.Help,
				nil,
				constLabels,
			),
			valueType: stat.ValueType,
			extractor: stat.Extractor,
		})
	}

	return &DBPoolStatsCollector[T]{
		source:  source,
		metrics: metrics,
	}
}

func (c DBPoolStatsCollector[T]) Describe(ch chan<- *prometheus.Desc) {
	for _, metric := range c.metrics {
		ch <- metric.desc
	}
}

func (c DBPoolStatsCollector[T]) Collect(ch chan<- prometheus.Metric) {
	if c.source == nil {
		return
	}

	stats := c.source.Stat()
	for _, metric := range c.metrics {
		if metric.extractor == nil {
			continue
		}

		ch <- prometheus.MustNewConstMetric(
			metric.desc,
			metric.valueType,
			metric.extractor(stats),
		)
	}
}
