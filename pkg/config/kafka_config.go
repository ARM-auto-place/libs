package config

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"fmt"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

func MakeConsumerKafkaConfigMap(logger *zerolog.Logger, kafkaConfig KafkaConfig) []kgo.Opt {
	cfg := kafkaConfig.ConsumerConfig
	Addr := strings.Split(cfg.Addr, ",")
	clientID, groupInstanceID := GetClientIDAndInstance(cfg.Client, cfg.Group, kafkaConfig.HostName, kafkaConfig.ClientName)

	var offset kgo.Offset
	if cfg.AutoOffsetReset == "earliest" {
		offset = kgo.NewOffset().AtStart()
	} else {
		offset = kgo.NewOffset().AtEnd()
	}

	opts := []kgo.Opt{
		kgo.ConsumeTopics(cfg.Topic),
		kgo.SeedBrokers(Addr...),
		kgo.ConsumerGroup(cfg.Group),
		kgo.ClientID(clientID),
		kgo.InstanceID(groupInstanceID),
		kgo.ConsumeResetOffset(offset),
		kgo.SessionTimeout(60 * time.Second),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		kgo.RetryBackoffFn(func(attempt int) time.Duration {
			backoff := time.Second * time.Duration(attempt)
			if backoff > 10*time.Second {
				backoff = 10 * time.Second
			}
			return backoff
		}),

		kgo.OnPartitionsAssigned(func(
			ctx context.Context,
			cl *kgo.Client,
			assigned map[string][]int32,
		) {
			logger.Info().
				Interface("assigned", assigned).
				Msg("partitions assigned")
		}),

		kgo.OnPartitionsRevoked(func(
			ctx context.Context,
			cl *kgo.Client,
			revoked map[string][]int32,
		) {
			logger.Info().
				Interface("revoked", revoked).
				Msg("partitions revoked")
		}),

		// standart librdkafka balancers; need for migration
		kgo.Balancers(
			kgo.RangeBalancer(),
			kgo.RoundRobinBalancer(),
		),
	}

	if !cfg.EnableAutoCommit {
		opts = append(opts, kgo.DisableAutoCommit())
	}

	opts = addSASL(opts, cfg.SASLMechanism, cfg.SecurityProtocol, cfg.Username, cfg.Password, cfg.EnableSSLVerification, cfg.CACertBase64)

	return opts
}

func MakeProducerKafkaConfigMap(logger *zerolog.Logger, kafkaConfig KafkaConfig) []kgo.Opt {
	cfg := kafkaConfig.ProducerConfig

	Addr := strings.Split(cfg.Addr, ",")
	client, _ := GetClientIDAndInstance(cfg.Client, "", kafkaConfig.HostName, "")

	opts := []kgo.Opt{
		kgo.DefaultProduceTopic(cfg.Topic),
		kgo.SeedBrokers(Addr...),
		kgo.ClientID(client),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordDeliveryTimeout(60 * time.Second),
		kgo.ProducerLinger(50 * time.Millisecond),
		kgo.ProducerBatchMaxBytes(10 * 1024 * 1024),
	}

	opts = addSASL(opts, cfg.SASLMechanism, cfg.SecurityProtocol, cfg.Username, cfg.Password, true, cfg.CACertBase64)

	return opts
}

func addSASL(opts []kgo.Opt, SASLMechanism, sp, userName, password string, enableSSLVerification bool, caCertBase64 string) []kgo.Opt {
	switch SASLMechanism {
	case "SCRAM-SHA-256":
		opts = append(opts,
			kgo.SASL(
				scram.Auth{
					User: userName,
					Pass: password,
				}.AsSha256Mechanism(),
			),
		)

	case "SCRAM-SHA-512":
		opts = append(opts,
			kgo.SASL(
				scram.Auth{
					User: userName,
					Pass: password,
				}.AsSha512Mechanism(),
			),
		)
	}
	if sp == "SASL_SSL" || sp == "SSL" {
		opts = append(opts,
			kgo.DialTLSConfig(MakeKafkaTLSConfig(caCertBase64, enableSSLVerification)),
		)
	}

	return opts
}

// MakeKafkaTLSConfig builds a *tls.Config for a Kafka connection. When a CA
// certificate chain is provided, server verification is enforced against it.
// Otherwise verification is governed by enableSSLVerification.
func MakeKafkaTLSConfig(caCertBase64 string, enableSSLVerification bool) *tls.Config {
	if caCertBase64 == "" {
		return &tls.Config{
			InsecureSkipVerify: !enableSSLVerification,
		}
	}
	raw, err := base64.StdEncoding.DecodeString(caCertBase64)
	if err != nil {
		panic("could not decode base64 of pem")
	}

	caCertPool := x509.NewCertPool()
	if !caCertPool.AppendCertsFromPEM(raw) {
		panic(fmt.Sprintf("could not load kafka CA certificate: no certificates found"))
	}

	return &tls.Config{
		RootCAs:            caCertPool,
		InsecureSkipVerify: false,
	}
}

func GetClientIDAndInstance(client, group, hostName, clientName string) (string, string) {
	clientID := client + "-" + hostName
	var groupInstanceID string
	if clientName != "" {
		groupInstanceID = group + "_" + clientName + "-" + hostName
	} else {
		groupInstanceID = group + "-" + hostName
	}

	return clientID, groupInstanceID
}

// MakeCACertPool builds an x509.CertPool from a PEM-encoded CA certificate chain.
func MakeCACertPool(caCertPEM string) (*x509.CertPool, error) {
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM([]byte(caCertPEM)) {
		return nil, fmt.Errorf("no certificates found in CA cert PEM")
	}
	return pool, nil
}
