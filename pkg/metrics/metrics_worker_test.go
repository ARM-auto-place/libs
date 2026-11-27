package metrics

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
)

func Test_WorkerMetrics(t *testing.T) {
	useTestRegistry(t)

	wm, err := NewWorkerMetrics("test", "worker")
	assert.NoError(t, err)
	assert.NotNil(t, wm)

	t.Run("IncProcessedCount", func(t *testing.T) {
		wm.IncProcessedCount("test_worker")
		assert.Equal(t, float64(1), testutil.ToFloat64(wm.CalcCountVec.WithLabelValues("test_worker")))
	})

	t.Run("ObserveStepDuration", func(t *testing.T) {
		wm.ObserveStepDuration("test_worker", "poll", 100*time.Millisecond)
		wm.ObserveStepDuration("test_worker", "process", 200*time.Millisecond)
		assert.NotNil(t, wm.StepDurationVec)
	})

	t.Run("QueueLength", func(t *testing.T) {
		wm.IncQueueLength("test_worker")
		wm.IncQueueLength("test_worker")
		wm.DecQueueLength("test_worker")
		assert.Equal(t, float64(1), testutil.ToFloat64(wm.TaskQueueBufferLenVec.WithLabelValues("test_worker")))
		wm.SetQueueLength("test_worker", 5.0)
		assert.Equal(t, float64(5), testutil.ToFloat64(wm.TaskQueueBufferLenVec.WithLabelValues("test_worker")))
	})

	t.Run("ActiveWorkers", func(t *testing.T) {
		wm.IncActiveWorkers("test_worker")
		wm.IncActiveWorkers("test_worker")
		wm.DecActiveWorkers("test_worker")
		assert.Equal(t, float64(1), testutil.ToFloat64(wm.WorkerProcessCountVec.WithLabelValues("test_worker")))
	})

	t.Run("IncError", func(t *testing.T) {
		wm.IncError("test_worker", "timeout")
		wm.IncError("test_worker", "timeout")
		wm.IncError("test_worker", "connection_refused")
		assert.Equal(t, float64(2), testutil.ToFloat64(wm.WorkerErrorCollector.WithLabelValues("test_worker", "timeout")))
		assert.Equal(t, float64(1), testutil.ToFloat64(wm.WorkerErrorCollector.WithLabelValues("test_worker", "connection_refused")))
	})

	t.Run("RetryTracking", func(t *testing.T) {
		wm.IncRetryCount("test_worker")
		wm.IncRetryCount("test_worker")
		assert.Equal(t, float64(2), testutil.ToFloat64(wm.RetryCountVec.WithLabelValues("test_worker")))
		wm.ObserveRetryDelay("test_worker", 1000*time.Millisecond)
		assert.NotNil(t, wm.RetryDelayVec)
	})

	t.Run("WorkerHealth", func(t *testing.T) {
		wm.SetHealth("test_worker", 1.0)
		assert.Equal(t, float64(1.0), testutil.ToFloat64(wm.HealthGauge.WithLabelValues("test_worker")))
		wm.IncRestartCount("test_worker")
		wm.IncRestartCount("test_worker")
		wm.IncRestartCount("test_worker")
		assert.Equal(t, float64(3), testutil.ToFloat64(wm.RestartCountVec.WithLabelValues("test_worker")))
	})

	t.Run("KafkaLag", func(t *testing.T) {
		wm.SetKafkaLag("test_worker", 100.0)
		assert.Equal(t, float64(100.0), testutil.ToFloat64(wm.KafkaLagGauge.WithLabelValues("test_worker")))
		wm.SetKafkaLag("test_worker", 250.0)
		assert.Equal(t, float64(250.0), testutil.ToFloat64(wm.KafkaLagGauge.WithLabelValues("test_worker")))
	})
}

func Test_WorkerMetrics_StructFields(t *testing.T) {
	useTestRegistry(t)

	wm, err := NewWorkerMetrics("test", "worker")
	assert.NoError(t, err)
	assert.NotNil(t, wm)

	// Verify all expected fields are present and initialized
	assert.NotNil(t, wm.CalcCountVec, "CalcCountVec should be initialized")
	assert.NotNil(t, wm.CalcDurationVec, "CalcDurationVec should be initialized")
	assert.NotNil(t, wm.TaskQueueBufferLenVec, "TaskQueueBufferLenVec should be initialized")
	assert.NotNil(t, wm.AddTaskWaitCountVec, "AddTaskWaitCountVec should be initialized")
	assert.NotNil(t, wm.WorkerProcessCountVec, "WorkerProcessCountVec should be initialized")
	assert.NotNil(t, wm.StepDurationVec, "StepDurationVec should be initialized")
	assert.NotNil(t, wm.WorkerErrorCollector, "WorkerErrorCollector should be initialized")
	assert.NotNil(t, wm.RetryCountVec, "RetryCountVec should be initialized")
	assert.NotNil(t, wm.RetryDelayVec, "RetryDelayVec should be initialized")
	assert.NotNil(t, wm.HealthGauge, "HealthGauge should be initialized")
	assert.NotNil(t, wm.RestartCountVec, "RestartCountVec should be initialized")
	assert.NotNil(t, wm.KafkaLagGauge, "KafkaLagGauge should be initialized")
}

func useTestRegistry(t *testing.T) {
	t.Helper()

	registerer := prometheus.DefaultRegisterer
	prometheus.DefaultRegisterer = prometheus.NewRegistry()
	t.Cleanup(func() {
		prometheus.DefaultRegisterer = registerer
	})
}
