package config

import (
	"io"
	"os"
	"strings"
	"time"
	"unicode"

	env "github.com/caarlos0/env/v10"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/pkgerrors"
)

const formatJSON = "json"

type BuildInfo struct {
	gitSHA      *string
	version     *string
	nodeName    *string
	buildStamp  *string
	buildNumber *string
	serviceName *string
}

func (b *BuildInfo) ServiceName() string {
	return getLinkedString(b.serviceName)
}

func (b *BuildInfo) NodeName() string {
	return getLinkedString(b.nodeName)
}

func (b *BuildInfo) Version() string {
	return getLinkedString(b.version)
}

func (b *BuildInfo) GitSHA() string {
	return getLinkedString(b.gitSHA)
}

func (b *BuildInfo) BuildStamp() string {
	return getLinkedString(b.buildStamp)
}

func (b *BuildInfo) BuildNumber() string {
	return getLinkedString(b.buildNumber)
}

func (b *BuildInfo) Set(serviceName, gitSHA, version, buildStamp, buildNumber string) {
	nodeName, err := GetHostName()
	if err != nil {
		nodeName = "unset"
	}

	setLinkedString(&b.serviceName, serviceName)
	setLinkedString(&b.gitSHA, gitSHA)
	setLinkedString(&b.version, version)
	setLinkedString(&b.buildStamp, buildStamp)
	setLinkedString(&b.buildNumber, buildNumber)
	setLinkedString(&b.nodeName, nodeName)
}

func GetHostName() (string, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return "", err
	}

	hostname = strings.Map(
		func(r rune) rune {
			switch {
			case unicode.IsLetter(r) || unicode.IsDigit(r):
				return r
			case r == '-' || r == '_':
				return r
			default:
				return '-'
			}
		},
		hostname)

	return hostname, nil
}

func NewLogger(level, format, serviceName string) zerolog.Logger {
	lvl := zerolog.InfoLevel
	if parsed, err := zerolog.ParseLevel(level); err == nil {
		lvl = parsed
	}

	var out io.Writer = os.Stdout
	if format != formatJSON {
		out = zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.StampMicro}
	}

	zerolog.ErrorStackMarshaler = pkgerrors.MarshalStack

	return zerolog.New(out).
		Level(lvl).
		With().
		Timestamp().
		Str("serviceName", serviceName).
		Logger()
}

func MustParse(cfg any) {
	if err := env.Parse(cfg); err != nil {
		panic(err)
	}
}

func getLinkedString(s *string) string {
	if s != nil {
		return *s
	}
	return "unset"
}

func setLinkedString(s **string, value string) {
	if *s == nil {
		*s = &value
	}
}
