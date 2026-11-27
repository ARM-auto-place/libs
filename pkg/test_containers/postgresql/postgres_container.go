package postgresql

import (
	"context"
	"fmt"
	"time"

	"github.com/ARM-auto-place/libs/pkg/config"
	"github.com/cenkalti/backoff/v4"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/moby/moby/api/types/network"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func NewTestConfig() config.TestPostgresConfig {
	return config.TestPostgresConfig{
		Host:     "docker",
		Port:     "5432",
		User:     "postgres",
		Password: "password",
		DB:       "limits",
	}

}

func SetupPostgres(ctx context.Context, config *config.TestPostgresConfig) (testcontainers.Container, *pgxpool.Pool, *pgxpool.Config, error) {
	container, url, err := createPostgresTestContainer(ctx, config)
	if err != nil {
		return nil, nil, nil, err
	}
	dbPool, conf, err := connectToPostgresContainer(ctx, url)
	if err != nil {
		return nil, nil, nil, err
	}
	return container, dbPool, conf, nil
}

func createPostgresTestContainer(ctx context.Context, config *config.TestPostgresConfig) (testcontainers.Container, string, error) {
	// port/tcp
	port := fmt.Sprintf("%s/tcp", config.Port)

	env := map[string]string{
		"POSTGRES_USER":     config.User,
		"POSTGRES_PASSWORD": config.Password,
		"POSTGRES_DB":       config.DB,
	}
	dbURL := func(host string, port network.Port) string {
		// for local replace docker to localhost
		return fmt.Sprintf("postgres://postgres:password@%s:%s/%s?sslmode=disable", host, port.Port(), config.DB)
	}
	req := testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "harbor.wildberries.ru/docker-hub-proxy/library/postgres:14.2",
			ExposedPorts: []string{port},
			Cmd: []string{
				"postgres",
				"-c", "fsync=off",
				"-c", "synchronous_commit=off",
				"-c", "full_page_writes=off",
				"-c", "shared_buffers=128MB",
				"-c", "max_connections=10000",
			},
			Env:        env,
			WaitingFor: wait.ForSQL(port, "postgres", dbURL).WithStartupTimeout(time.Second * 60),
		},
		Started: true,
	}
	var container testcontainers.Container

	err := backoff.Retry(func() (err error) {
		container, err = testcontainers.GenericContainer(ctx, req)
		if err != nil {
			return fmt.Errorf("could not start postgres container: %w", err)
		}
		return nil
	}, backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 10))

	var url string
	if err != nil {
		return container, url, fmt.Errorf("failed to start container: %w", err)
	}
	host, err := container.Host(ctx)
	if err != nil {
		return container, url, fmt.Errorf("failed to get container external host: %w", err)
	}
	config.Host = host
	mappedPort, err := container.MappedPort(ctx, port)
	if err != nil {
		return container, url, fmt.Errorf("failed to get container external port: %w", err)
	}
	url = dbURL(config.Host, mappedPort)
	config.Port = mappedPort.Port()
	return container, url, nil
}

func connectToPostgresContainer(ctx context.Context, connectionUrl string) (*pgxpool.Pool, *pgxpool.Config, error) {
	conf, err := pgxpool.ParseConfig(connectionUrl)
	if err != nil {
		return nil, nil, err
	}
	dbPool, err := pgxpool.NewWithConfig(ctx, conf)
	if err != nil {
		return nil, nil, fmt.Errorf("can not connect to db, %w", err)
	}
	return dbPool, conf, nil
}
