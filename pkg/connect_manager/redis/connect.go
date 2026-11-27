package redis

import (
	"github.com/ARM-auto-place/libs/pkg/config"
	"github.com/redis/go-redis/v9"
)

func NewRedisClient(config config.RedisConfig) redis.UniversalClient {
	if config.RedisUseSentinel {
		return redis.NewClient(&redis.Options{
			Addr:       config.RedisAddrs[0],
			Username:   config.RedisUsername,
			Password:   config.RedisPassword,
			MaxRetries: config.RedisMaxRetries,
		})
	} else {
		return redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:      config.RedisAddrs,
			Password:   config.RedisPassword,
			MaxRetries: config.RedisMaxRetries,
			ReadOnly:   false,
		})
	}
}
