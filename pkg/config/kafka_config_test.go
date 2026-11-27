package config

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func generateTestCertPEM(t *testing.T) string {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			CommonName: "Ca-Test",
		},
		NotBefore: time.Now().Add(-time.Hour),
		NotAfter:  time.Now().Add(24 * time.Hour),
		IsCA:      true,
		KeyUsage:  x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	require.NoError(t, err)

	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: derBytes}))
}

func generateTestCertBase64(t *testing.T) string {
	t.Helper()
	return base64.StdEncoding.EncodeToString([]byte(generateTestCertPEM(t)))
}

func TestMakeKafkaTLSConfig_WithCAEnforcesVerification(t *testing.T) {
	cfg := MakeKafkaTLSConfig(generateTestCertBase64(t), false)
	assert.NotNil(t, cfg)
	assert.NotNil(t, cfg.RootCAs)
	assert.False(t, cfg.InsecureSkipVerify)
}

func TestMakeKafkaTLSConfig_WithCAAndVerificationFlagFalseStillVerifies(t *testing.T) {
	cfg := MakeKafkaTLSConfig(generateTestCertBase64(t), false)
	assert.False(t, cfg.InsecureSkipVerify)
}

func TestMakeKafkaTLSConfig_NoCAUsesFlag(t *testing.T) {
	cfg := MakeKafkaTLSConfig("", false)
	assert.True(t, cfg.InsecureSkipVerify)
	assert.Nil(t, cfg.RootCAs)

	cfgVerified := MakeKafkaTLSConfig("", true)
	assert.False(t, cfgVerified.InsecureSkipVerify)
}

func TestMakeKafkaTLSConfig_InvalidPEMPanics(t *testing.T) {
	assert.Panics(t, func() {
		MakeKafkaTLSConfig(base64.StdEncoding.EncodeToString([]byte("not a pem")), true)
	})
}

func TestMakeKafkaTLSConfig_InvalidBase64Panics(t *testing.T) {
	assert.Panics(t, func() {
		MakeKafkaTLSConfig("not a base64 string", true)
	})
}

func TestMakeCACertPool_ValidPEM(t *testing.T) {
	pool, err := MakeCACertPool(generateTestCertPEM(t))
	assert.NoError(t, err)
	assert.NotNil(t, pool)
	assert.IsType(t, &x509.CertPool{}, pool)
}

func TestMakeCACertPool_InvalidPEM(t *testing.T) {
	_, err := MakeCACertPool("not a pem")
	assert.Error(t, err)
}

func TestMakeCACertPool_Empty(t *testing.T) {
	_, err := MakeCACertPool("")
	assert.Error(t, err)
}

func TestAddSASL_NoTLSWithoutSSL(t *testing.T) {
	opts := addSASL(nil, "SCRAM-SHA-256", "PLAINTEXT", "u", "p", true, generateTestCertBase64(t))
	assert.NotNil(t, opts)
}
