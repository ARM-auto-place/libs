package metrics

import (
	"time"
)

type NoopWorkerMetrics struct{}

func (NoopWorkerMetrics) IncProcessedCount(name string)                              {}
func (NoopWorkerMetrics) ObserveProcessDuration(name string, duration time.Duration) {}
func (NoopWorkerMetrics) ObserveStepDuration(name string, step WorkerStep, duration time.Duration) {
}
func (NoopWorkerMetrics) IncQueueLength(name string)                            {}
func (NoopWorkerMetrics) DecQueueLength(name string)                            {}
func (NoopWorkerMetrics) SetQueueLength(name string, len float64)               {}
func (NoopWorkerMetrics) IncActiveWorkers(name string)                          {}
func (NoopWorkerMetrics) DecActiveWorkers(name string)                          {}
func (NoopWorkerMetrics) IncError(name string, errorType string)                {}
func (NoopWorkerMetrics) IncRetryCount(name string)                             {}
func (NoopWorkerMetrics) ObserveRetryDelay(name string, duration time.Duration) {}
func (NoopWorkerMetrics) SetHealth(name string, healthy float64)                {}
func (NoopWorkerMetrics) IncRestartCount(name string)                           {}
func (NoopWorkerMetrics) SetKafkaLag(name string, lag float64)                  {}

func WorkerMetricsOrNoop(m *WorkerMetrics) IWorkerMetrics {
	if m == nil {
		return NoopWorkerMetrics{}
	}
	return m
}
