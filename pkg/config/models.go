package config

import (
	"time"
)

type Config struct {
	NumCounters int64
	MaxCost     int64
	BufferItems int64
	DefaultTTL  time.Duration
	Metrics     bool
}

type PostgresConfig struct {
	Addr            string `env:"PG_ADDR"`
	DB              string `env:"PG_DB"`
	User            string `env:"PG_USER"`
	Password        string `env:"PG_PASSWORD"`
	MaxConn         int    `env:"PG_MAX_CONN" envDefault:"10"`
	MaxIdleLifetime string `env:"PG_MAX_IDLE_LIFETIME" envDefault:"30s"`
	MaxLifetime     string `env:"PG_MAX_LIFETIME" envDefault:"3m"`
	PrepareCacheCap int    `env:"PG_PREPARE_CACHE_CAP" envDefault:"128"`
	ReplicaAddr     string `env:"PG_REPLICA_ADDR"`
	UserRO          string `env:"PG_REPLICA_USER"`
	PasswordRO      string `env:"PG_REPLICA_PASSWORD"`
}

type TestPostgresConfig struct {
	Host     string
	User     string
	Password string
	DB       string
	Port     string
}

type RedisConfig struct {
	RedisAddrs      []string `env:"REDIS_ADDRS" envDefault:"localhost:30001, localhost:30002, localhost:30003, localhost:30004"`
	RedisUsername   string   `env:"REDIS_USERNAME" envDefault:""`
	RedisPassword   string   `env:"REDIS_PASSWORD" envDefault:""`
	RedisMaxRetries int      `env:"REDIS_MAX_RETRIES" envDefault:"5"`

	RedisUseSentinel bool `env:"REDIS_USE_SENTINEL"`
}
