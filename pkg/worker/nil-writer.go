package worker

import (
	"context"
	"go/types"

	"github.com/jackc/pgx/v5"
)

type nilWriter struct{}

func (w *nilWriter) Write(ctx context.Context, result types.Nil, tx *pgx.Tx) error {
	return nil
}
func (w *nilWriter) Update(ctx context.Context, result types.Nil) error { return nil }

func NewNilWriter() Writer[types.Nil] {
	return &nilWriter{}
}
