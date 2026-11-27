package config

import (
	"time"
)

type ConsumerKafkaConfig struct {
	Addr                  string `env:"KAFKA_ADDR"`
	Client                string `env:"KAFKA_CLIENT_ID" envDefault:""`
	Username              string `env:"KAFKA_USERNAME" envDefault:""`
	Password              string `env:"KAFKA_PASSWORD" envDefault:""`
	Group                 string `env:"KAFKA_GROUP" envDefault:""`
	Topic                 string `env:"KAFKA_TOPIC"`
	SecurityProtocol      string `env:"KAFKA_SECURITY_PROTOCOL" envDefault:"PLAINTEXT"`
	SASLMechanism         string `env:"KAFKA_SASL_MECHANISM" envDefault:"SCRAM-SHA-256"`
	AutoOffsetReset       string `env:"KAFKA_AUTO_OFFSET_RESET" envDefault:"earliest"`
	EnableSSLVerification bool   `env:"KAFKA_ENABLE_SSL_CERTIFICATE_VERIFICATION" envDefault:"true"`
	EnableAutoCommit      bool   `env:"KAFKA_ENABLE_AUTO_COMMIT" envDefault:"false"`
	CACertBase64          string `env:"KAFKA_CA_CERT_BASE64" envDefault:""`
}

type ProducerKafkaConfig struct {
	Addr             string `env:"KAFKA_PRODUCER_ADDR"`
	Client           string `env:"KAFKA_PRODUCER_CLIENT_ID" envDefault:""`
	Username         string `env:"KAFKA_PRODUCER_USERNAME" envDefault:""`
	Password         string `env:"KAFKA_PRODUCER_PASSWORD" envDefault:""`
	Topic            string `env:"KAFKA_PRODUCER_TOPIC"`
	SecurityProtocol string `env:"KAFKA_PRODUCER_SECURITY_PROTOCOL" envDefault:"PLAINTEXT"`
	SASLMechanism    string `env:"KAFKA_PRODUCER_SASL_MECHANISM" envDefault:"SCRAM-SHA-256"`
	CACertBase64     string `env:"KAFKA_PRODUCER_CA_CERT_BASE64" envDefault:""`
}

type KafkaConfig struct {
	ConsumerConfig ConsumerKafkaConfig
	ProducerConfig ProducerKafkaConfig
	HostName       string
	ClientName     string
}

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
