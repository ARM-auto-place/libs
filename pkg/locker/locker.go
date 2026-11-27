package locker

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/go-redsync/redsync/v4"
	"github.com/go-redsync/redsync/v4/redis/goredis/v9"
	"github.com/redis/go-redis/v9"
	goredislib "github.com/redis/go-redis/v9"
)

var (
	ErrRedisUnavailable = errors.New("redis is unavailable")
)

//go:generate mockgen -source=locker.go -destination locker_mock.go -package locker
type Locker interface {
	AcquireLock(ctx context.Context, key string, ttl time.Duration) (Lock, error)
}
type locker struct {
	rs *redsync.Redsync
}

func (l *locker) AcquireLock(ctx context.Context, key string, ttl time.Duration) (Lock, error) {
	mutex := l.rs.NewMutex(key, redsync.WithExpiry(ttl))
	err := mutex.LockContext(ctx)
	if err != nil {
		if isRedisUnavailable(err) {
			return nil, fmt.Errorf("%w: %w", ErrRedisUnavailable, err)
		}
		return nil, fmt.Errorf("could not lock mutex: %w", err)
	}
	return mutex, nil
}

func isRedisUnavailable(err error) bool {
	if errors.Is(err, redis.ErrClosed) ||
		errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	return false
}

type Lock interface {
	UnlockContext(context.Context) (bool, error)
}

func NewLocker(redisCli goredislib.UniversalClient) (Locker, error) {
	pool := goredis.NewPool(redisCli)

	// Create an instance of redisync to be used to obtain a mutual exclusion
	// lock.
	rs := redsync.New(pool)
	return &locker{
		rs: rs,
	}, nil
}
