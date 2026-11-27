package redis

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

var RedisPort = "6379"

func setupRedisContainer(ctx context.Context) (testcontainers.Container, error) {
	host := "docker"
	localRun, parserErr := strconv.ParseBool(os.Getenv("LOCAL_RUN"))
	if parserErr == nil && localRun {
		host = "localhost"
	}

	env := map[string]string{
		"REDIS_ADDRS":       fmt.Sprintf("%s:%s", host, RedisPort),
		"REDIS_PASSWORD":    "",
		"REDIS_MAX_RETRIES": "5",
	}

	req := testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "harbor.wildberries.ru/docker-hub-proxy/library/redis:latest",
			ExposedPorts: []string{"6379/tcp"}, // Ensure correct port
			Env:          env,
			WaitingFor:   wait.ForLog("Ready to accept connections").WithStartupTimeout(30 * time.Second),
		},

		Started: true,
	}
	container, err := testcontainers.GenericContainer(ctx, req)

	return container, err
}

func makeRedisClient(ctx context.Context, container testcontainers.Container) (*redis.Client, error) {
	host, err := container.Host(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get Redis container host: %w", err)
	}

	port, err := container.MappedPort(ctx, "6379/tcp")
	if err != nil {
		return nil, fmt.Errorf("failed to get Redis container port: %w", err)
	}

	redisAddr := fmt.Sprintf("%s:%s", host, port.Port())

	redisPwd := "" // Assuming no password for this local Redis setup

	redisClient := redis.NewClient(&redis.Options{
		Addr:       redisAddr,
		Password:   redisPwd, // no password set
		MaxRetries: 5,        // use default DB
		DB:         1,
	})

	// Ping Redis to ensure connection
	_, err = redisClient.Ping(ctx).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	return redisClient, nil
}

func SetupRedis(ctx context.Context) (testcontainers.Container, *redis.Client, error) {
	container, err := setupRedisContainer(ctx)
	if err != nil {
		return nil, nil, err
	}
	redisClient, err := makeRedisClient(ctx, container)
	if err != nil {
		return nil, nil, err
	}

	return container, redisClient, err
}
