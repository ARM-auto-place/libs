package metrics

import (
	"errors"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// WorkerStep represents the step in worker processing pipeline
type WorkerStep string

const (
	StepPoll    WorkerStep = "poll"
	StepRead    WorkerStep = "read"
	StepProcess WorkerStep = "process"
	StepWrite   WorkerStep = "write"
	StepCommit  WorkerStep = "commit"
)

type IWorkerMetrics interface {
	// Processed count
	IncProcessedCount(name string)
	ObserveProcessDuration(name string, duration time.Duration)

	// Timing observations
	ObserveStepDuration(name string, step WorkerStep, duration time.Duration)

	// Queue/window tracking
	IncQueueLength(name string)
	DecQueueLength(name string)
	SetQueueLength(name string, len float64)

	IncActiveWorkers(name string)
	DecActiveWorkers(name string)

	// Error tracking
	IncError(name string, errorType string)

	// Retry tracking
	IncRetryCount(name string)
	ObserveRetryDelay(name string, duration time.Duration)

	// Worker health
	SetHealth(name string, healthy float64) // 1 = healthy, 0 = unhealthy
	IncRestartCount(name string)

	// Kafka lag
	SetKafkaLag(name string, lag float64)
}

type WorkerMetrics struct {
	Namespace string
	Subsystem string

	// Processed count
	CalcCountVec    *prometheus.CounterVec
	CalcDurationVec *prometheus.HistogramVec

	// Queue/window tracking
	TaskQueueBufferLenVec *prometheus.GaugeVec
	AddTaskWaitCountVec   *prometheus.GaugeVec
	WorkerProcessCountVec *prometheus.GaugeVec
	StepDurationVec       *prometheus.HistogramVec

	// Error tracking
	WorkerErrorCollector *prometheus.CounterVec

	// Retry tracking
	RetryCountVec *prometheus.CounterVec
	RetryDelayVec *prometheus.HistogramVec

	// Worker health
	HealthGauge     *prometheus.GaugeVec
	RestartCountVec *prometheus.CounterVec

	// Kafka lag
	KafkaLagGauge *prometheus.GaugeVec
}

func NewWorkerMetrics(namespace, subsystem string) (*WorkerMetrics, error) {
	// Processed count metrics
	calcCountVec := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "processed_total",
			Help:      "Number of items processed",
		},
		[]string{"name"},
	)

	calcDurationVec := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "process_duration_seconds",
			Help:      "Duration of processing in seconds",
			Buckets:   append(prometheus.DefBuckets, 15, 30, 60),
		},
		[]string{"name"},
	)

	// Queue/window tracking
	taskQueueBufferLenVec := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "queue_length",
			Help:      "The length of the queue",
		},
		[]string{"name"},
	)

	addTaskWaitCountVec := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "add_task_wait_count",
			Help:      "The number of tasks waiting to add to queue",
		},
		[]string{"name"},
	)

	workerProcessCountVec := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "active_workers",
			Help:      "The number of currently active workers",
		},
		[]string{"name"},
	)

	taskProcessDurationVec := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "step_duration_seconds",
			Help:      "The duration histogram of step processing in seconds",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{"name", "step"},
	)

	workerErrorCollector := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "errors_total",
			Help:      "The number of errors encountered",
		},
		[]string{"name", "error_type"},
	)

	// Retry tracking
	retryCountVec := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "retry_total",
			Help:      "Number of retries",
		},
		[]string{"name"},
	)

	retryDelayVec := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "retry_delay_seconds",
			Help:      "Retry delay in seconds",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{"name"},
	)

	// Worker health
	healthGauge := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "health",
			Help:      "Worker health status (1 = healthy, 0 = unhealthy)",
		},
		[]string{"name"},
	)

	restartCountVec := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "restart_total",
			Help:      "Number of worker restarts",
		},
		[]string{"name"},
	)

	// Kafka lag
	kafkaLagGauge := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "kafka_lag",
			Help:      "Kafka consumer lag",
		},
		[]string{"name"},
	)
	metrics := []prometheus.Collector{
		calcCountVec,
		calcDurationVec,
		taskQueueBufferLenVec,
		addTaskWaitCountVec,
		workerProcessCountVec,
		taskProcessDurationVec,
		workerErrorCollector,
		retryCountVec,
		retryDelayVec,
		healthGauge,
		restartCountVec,
		kafkaLagGauge,
	}
	for _, metric := range metrics {
		if err := prometheus.Register(metric); err != nil {

			var alreadyRegisteredErr prometheus.AlreadyRegisteredError
			if !errors.As(err, &alreadyRegisteredErr) {
				return nil, err
			}
		}
	}
	return &WorkerMetrics{
		Namespace: namespace,
		Subsystem: subsystem,

		CalcCountVec:    calcCountVec,
		CalcDurationVec: calcDurationVec,

		TaskQueueBufferLenVec: taskQueueBufferLenVec,
		AddTaskWaitCountVec:   addTaskWaitCountVec,
		WorkerProcessCountVec: workerProcessCountVec,
		StepDurationVec:       taskProcessDurationVec,
		WorkerErrorCollector:  workerErrorCollector,

		RetryCountVec: retryCountVec,
		RetryDelayVec: retryDelayVec,

		HealthGauge:     healthGauge,
		RestartCountVec: restartCountVec,

		KafkaLagGauge: kafkaLagGauge,
	}, nil
}

func (wm *WorkerMetrics) IncProcessedCount(name string) {
	wm.CalcCountVec.WithLabelValues(name).Inc()
}

func (wm *WorkerMetrics) ObserveProcessDuration(name string, duration time.Duration) {
	wm.CalcDurationVec.WithLabelValues(name).Observe(float64(duration.Seconds()))
}

func (wm *WorkerMetrics) ObserveStepDuration(name string, step WorkerStep, duration time.Duration) {
	wm.StepDurationVec.WithLabelValues(name, string(step)).Observe(float64(duration.Seconds()))
}

func (wm *WorkerMetrics) IncQueueLength(name string) {
	wm.TaskQueueBufferLenVec.WithLabelValues(name).Inc()
}

func (wm *WorkerMetrics) DecQueueLength(name string) {
	wm.TaskQueueBufferLenVec.WithLabelValues(name).Dec()
}

func (wm *WorkerMetrics) SetQueueLength(name string, len float64) {
	wm.TaskQueueBufferLenVec.WithLabelValues(name).Set(len)
}

func (wm *WorkerMetrics) IncActiveWorkers(name string) {
	wm.WorkerProcessCountVec.WithLabelValues(name).Inc()
}

func (wm *WorkerMetrics) DecActiveWorkers(name string) {
	wm.WorkerProcessCountVec.WithLabelValues(name).Dec()
}

func (wm *WorkerMetrics) IncError(name string, errorType string) {
	wm.WorkerErrorCollector.WithLabelValues(name, errorType).Inc()
}

func (wm *WorkerMetrics) IncRetryCount(name string) {
	wm.RetryCountVec.WithLabelValues(name).Inc()
}

func (wm *WorkerMetrics) ObserveRetryDelay(name string, duration time.Duration) {
	wm.RetryDelayVec.WithLabelValues(name).Observe(float64(duration.Seconds()))
}

func (wm *WorkerMetrics) SetHealth(name string, healthy float64) {
	wm.HealthGauge.WithLabelValues(name).Set(healthy)
}

func (wm *WorkerMetrics) IncRestartCount(name string) {
	wm.RestartCountVec.WithLabelValues(name).Inc()
}

func (wm *WorkerMetrics) SetKafkaLag(name string, lag float64) {
	wm.KafkaLagGauge.WithLabelValues(name).Set(lag)
}
