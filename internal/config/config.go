package config

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/spf13/viper"
)

type Config struct {
	App       App       `mapstructure:"app"`
	Server    Server    `mapstructure:"server"`
	Signaling Signaling `mapstructure:"signaling"`
}

type App struct {
	Name    string `mapstructure:"name"`
	Version string `mapstructure:"version"`
	Env     string `mapstructure:"env"`
}

type Server struct {
	Host                   string        `mapstructure:"host"`
	Port                   int           `mapstructure:"port"`
	IdleTimeout            time.Duration `mapstructure:"idleTimeout"`
	HandshakeTimeout       time.Duration `mapstructure:"handshakeTimeout"`
	KeepAlivePeriodTimeout time.Duration `mapstructure:"keepAlivePeriodTimeout"`
	CloseTimeout           time.Duration `mapstructure:"closeTimeout"`
	StartTimeout           time.Duration `mapstructure:"startTimeout"`
	NextProtos             []string      `mapstructure:"nextProtos"`
	CertsPath              string        `mapstructure:"certsPath"`
	MaxIncomingStreams     int64         `mapstructure:"maxIncomingStreams"`
}

type Signaling struct {
	Secret   string        `mapstructure:"secret"`
	CredsTTL time.Duration `mapstructure:"credsTTL"`
}

func NewConfig(path string) *Config {
	if path == "" {
		panic(fmt.Errorf("config path is empty"))
	}
	filename := filepath.Join(path, "config.yaml")
	data, err := os.ReadFile(filename)
	if err != nil {
		panic(fmt.Errorf("failed to read config file: %w", err))
	}
	data = []byte(os.ExpandEnv(string(data)))
	v := viper.New()
	v.SetConfigName("config")
	v.SetConfigType("yaml")
	cfg := &Config{}
	if err := v.ReadConfig(bytes.NewBuffer(data)); err != nil {
		panic(fmt.Errorf("failed to read config: %w", err))
	}
	if err := v.Unmarshal(cfg); err != nil {
		panic(fmt.Errorf("failed to unmarshal config: %w", err))
	}
	return cfg
}
