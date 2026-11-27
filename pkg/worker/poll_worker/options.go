package poll_worker

import (
	"context"

	"github.com/jackc/pgx/v5"
)

// PollWorkerOption конфигурирует PollWorker перед возвратом из NewPollWorker.
type PollWorkerOption[Req, Res any] func(*pollWorker[Req, Res])

// WithDedup включает дедупликацию сообщений Kafka. keyFunc вызывается для
// каждого сообщения и возвращает ключ дедупликации; если ключ уже помечен как
// обработанный, сообщение пропускается. dedupStore используется для атомарной
// пометки обработанных сообщений.
func WithDedup[Req, Res any](
	dedupStore DedupStore,
	keyFunc func(ctx context.Context, msg Req) (string, error),
) PollWorkerOption[Req, Res] {
	return func(w *pollWorker[Req, Res]) {
		w.dedupStore = dedupStore
		w.dedupKeyFunc = keyFunc
	}
}

// WithGetTransaction задаёт фабрику транзакции pgx: на каждое сообщение
// открывается своя транзакция, коммитится после успешной записи результата.
// Если опция не передана — воркер работает без транзакции.
func WithGetTransaction[Req, Res any](
	getTransaction func(ctx context.Context, isMaster bool) (pgx.Tx, error),
) PollWorkerOption[Req, Res] {
	return func(w *pollWorker[Req, Res]) {
		w.getTransaction = getTransaction
	}
}

// WithRetry задаёт продюсер Kafka и топик, в который отправляются упавшие
// сообщения (retry-топик) после исчерпания попыток обработки. Если опция не
// передана — упавшие сообщения не отправляются в retry.
func WithRetry[Req, Res any](
	producer Producer,
	topic string,
) PollWorkerOption[Req, Res] {
	return func(w *pollWorker[Req, Res]) {
		w.kafkaProducer = producer
		w.kafkaProducerTopic = topic
	}
}
