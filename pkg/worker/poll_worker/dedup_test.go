package poll_worker

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ARM-auto-place/libs/pkg/worker"
)

type fakeDedupStore struct {
	setValueNXReturns bool
	setValueNXCalled  int
	deletedKeys       []string
}

func (f *fakeDedupStore) SetValueNX(_ context.Context, key string, _ any, _ time.Duration) (bool, error) {
	f.setValueNXCalled++
	return f.setValueNXReturns, nil
}

func (f *fakeDedupStore) DeleteValue(_ context.Context, key string) error {
	f.deletedKeys = append(f.deletedKeys, key)
	return nil
}

type fakeProcessor[Req, Res any] struct {
	processed []Req
	err       error
	nilResult bool
}

func (p *fakeProcessor[Req, Res]) Process(_ context.Context, req Req, _ *pgx.Tx) (*Res, error) {
	p.processed = append(p.processed, req)
	if p.err != nil {
		return nil, p.err
	}
	if p.nilResult {
		return nil, nil
	}
	var res Res
	return &res, nil
}

type fakeWriter[Res any] struct {
	written []Res
	updated []Res
	err     error
}

func (w *fakeWriter[Res]) Write(_ context.Context, res Res, _ *pgx.Tx) error {
	w.written = append(w.written, res)
	return w.err
}

func (w *fakeWriter[Res]) Update(_ context.Context, res Res) error {
	w.updated = append(w.updated, res)
	return nil
}

func newDedupTestWorker(store DedupStore, maxRetries uint64) *pollWorker[string, string] {
	logger := zerolog.New(io.Discard)
	return NewPollWorker[string, string](
		&logger,
		time.Minute,
		maxRetries,
		nil,
		"test",
		WithDedup[string, string](store, func(_ context.Context, req string) (string, error) {
			return req, nil
		}),
	).(*pollWorker[string, string])
}

func testMessage() Message[string] {
	return Message[string]{Payload: "id-1", Metadata: &kgo.Record{}}
}

func TestDedup_NewMessage_Processed(t *testing.T) {
	store := &fakeDedupStore{setValueNXReturns: true}
	w := newDedupTestWorker(store, 1)

	proc := &fakeProcessor[string, string]{}
	wr := &fakeWriter[string]{}

	err := w.tick(context.Background(), proc, wr, testMessage())
	assert.NoError(t, err)
	assert.Equal(t, 1, store.setValueNXCalled)
	assert.Equal(t, []string{"id-1"}, proc.processed)
	assert.Empty(t, store.deletedKeys)
}

func TestDedup_DuplicateMessage_Skipped(t *testing.T) {
	store := &fakeDedupStore{setValueNXReturns: false}
	w := newDedupTestWorker(store, 1)

	proc := &fakeProcessor[string, string]{}
	wr := &fakeWriter[string]{}

	err := w.tick(context.Background(), proc, wr, testMessage())
	assert.NoError(t, err)
	assert.Equal(t, 1, store.setValueNXCalled)
	assert.Empty(t, proc.processed)
	assert.Empty(t, store.deletedKeys)
}

func TestDedup_ResultNil_DeletesKey(t *testing.T) {
	store := &fakeDedupStore{setValueNXReturns: true}
	w := newDedupTestWorker(store, 0)

	proc := &fakeProcessor[string, string]{nilResult: true}
	wr := &fakeWriter[string]{}

	err := w.tick(context.Background(), proc, wr, testMessage())
	assert.NoError(t, err)
	assert.Contains(t, store.deletedKeys, "processed-message:id-1")
}

func TestDedup_WriteError_UpdatesResult(t *testing.T) {
	store := &fakeDedupStore{setValueNXReturns: true}
	w := newDedupTestWorker(store, 0)

	proc := &fakeProcessor[string, string]{}
	wr := &fakeWriter[string]{err: errors.New("write boom")}

	err := w.tick(context.Background(), proc, wr, testMessage())
	assert.Error(t, err)
	// результат не затеняется — Update вызывается даже при ошибке записи
	assert.Equal(t, 1, len(wr.updated))
	assert.Contains(t, store.deletedKeys, "processed-message:id-1")
}

func TestDedup_ProcessingError_DeletesKey(t *testing.T) {
	store := &fakeDedupStore{setValueNXReturns: true}
	w := newDedupTestWorker(store, 0)

	proc := &fakeProcessor[string, string]{err: errors.New("boom")}
	wr := &fakeWriter[string]{}

	err := w.tick(context.Background(), proc, wr, testMessage())
	assert.Error(t, err)
	assert.Contains(t, store.deletedKeys, "processed-message:id-1")
}

func TestDedup_Disabled_ProcessesMessage(t *testing.T) {
	logger := zerolog.New(io.Discard)
	w := NewPollWorker[string, string](
		&logger,
		time.Minute,
		1,
		nil,
		"test",
	).(*pollWorker[string, string])

	proc := &fakeProcessor[string, string]{}
	wr := &fakeWriter[string]{}

	err := w.tick(context.Background(), proc, wr, testMessage())
	assert.NoError(t, err)
	assert.Equal(t, []string{"id-1"}, proc.processed)
}

var _ worker.Processor[string, string] = (*fakeProcessor[string, string])(nil)
var _ worker.Writer[string] = (*fakeWriter[string])(nil)
var _ DedupStore = (*fakeDedupStore)(nil)
