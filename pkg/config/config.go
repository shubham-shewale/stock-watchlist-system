package config

import (
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/joho/godotenv"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

// Config holds all configuration for the application
type Config struct {
	App       AppConfig       `mapstructure:"app"`
	Redis     RedisConfig     `mapstructure:"redis"`
	Kafka     KafkaConfig     `mapstructure:"kafka"`
	Logger    LoggerConfig    `mapstructure:"logger"`
	Generator GeneratorConfig `mapstructure:"generator"`
	Processor ProcessorConfig `mapstructure:"processor"`
	Gateway   GatewayConfig   `mapstructure:"gateway"`
}

type AppConfig struct {
	Port string `mapstructure:"port"`
	Env  string `mapstructure:"env"` // e.g., "local", "prod"
}

type RedisConfig struct {
	Addr     string `mapstructure:"addr"`
	Password string `mapstructure:"password"`
	DB       int    `mapstructure:"db"`
}

type KafkaConfig struct {
	Brokers []string `mapstructure:"brokers"`
	Topic   string   `mapstructure:"topic"`
	GroupID string   `mapstructure:"group_id"`
}

type LoggerConfig struct {
	Level  string `mapstructure:"level"`
	Format string `mapstructure:"format"` // "json" or "console"
}

type GeneratorConfig struct {
	Tickers    []string           `mapstructure:"tickers"`
	BasePrices map[string]float64 `mapstructure:"base_prices"`
}

type ProcessorConfig struct {
	NumWorkers int `mapstructure:"num_workers"`
}

type GatewayConfig struct {
	ValidTickers   []string      `mapstructure:"valid_tickers"`
	Timeouts       TimeoutConfig `mapstructure:"timeouts"`
	MaxConnections int           `mapstructure:"max_connections"`
	AllowedOrigins []string      `mapstructure:"allowed_origins"`
}

type TimeoutConfig struct {
	WriteWait      time.Duration `mapstructure:"write_wait"`
	PongWait       time.Duration `mapstructure:"pong_wait"`
	PingPeriod     time.Duration `mapstructure:"ping_period"`
	SendBufferSize int           `mapstructure:"send_buffer_size"`
}

// LoadConfig reads configuration from .env file, environment variables, and defaults.
func LoadConfig() (*Config, error) {
	v := viper.New()

	// 1. Load .env file into System Environment (if it exists)
	// This ensures variables like APP_PORT are available as real env vars
	if err := godotenv.Load(); err != nil {
		log.Println("Note: No .env file found, relying on System Env Vars")
	}

	// 2. Set Defaults (12-Factor App: Dev/Prod Parity)
	v.SetDefault("app.port", ":8080")
	v.SetDefault("app.env", "local")

	v.SetDefault("redis.addr", "localhost:6379")
	v.SetDefault("redis.password", "")
	v.SetDefault("redis.db", 0)

	v.SetDefault("kafka.brokers", []string{"localhost:9092"})
	v.SetDefault("kafka.topic", "market_ticks")
	v.SetDefault("kafka.group_id", "stock-processor-group")

	v.SetDefault("logger.level", "info")
	v.SetDefault("logger.format", "json")

	v.SetDefault("generator.tickers", []string{"AAPL", "GOOG", "TSLA", "AMZN"})
	v.SetDefault("generator.base_prices", map[string]float64{
		"AAPL": 150.0, "GOOG": 2800.0, "TSLA": 700.0, "AMZN": 3400.0,
	})

	v.SetDefault("processor.num_workers", 10)

	v.SetDefault("gateway.valid_tickers", []string{"AAPL", "GOOG", "TSLA", "AMZN"})
	v.SetDefault("gateway.timeouts.write_wait", "10s")
	v.SetDefault("gateway.timeouts.pong_wait", "60s")
	v.SetDefault("gateway.timeouts.ping_period", "54s") // (pongWait * 9) / 10
	v.SetDefault("gateway.timeouts.send_buffer_size", 256)
	v.SetDefault("gateway.max_connections", 1000)
	v.SetDefault("gateway.allowed_origins", []string{"*"})

	// 3. Configure Viper to read Environment Variables
	// This maps dot-notation to underscores (e.g., "app.port" -> "APP_PORT")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	// 4. Explicitly Bind Env Vars to Keys
	// This is crucial for Viper to map flat Env Vars (APP_PORT) to nested structs (App.Port)
	bindEnv(v, "app.port", "app.env")
	bindEnv(v, "redis.addr", "redis.password", "redis.db")
	bindEnv(v, "kafka.brokers", "kafka.topic", "kafka.group_id")
	bindEnv(v, "logger.level", "logger.format")
	bindEnv(v, "generator.tickers", "generator.base_prices")
	bindEnv(v, "processor.num_workers")
	bindEnv(v, "gateway.valid_tickers", "gateway.timeouts.write_wait", "gateway.timeouts.pong_wait", "gateway.timeouts.ping_period", "gateway.timeouts.send_buffer_size")
	bindEnv(v, "gateway.max_connections", "gateway.allowed_origins")

	// 5. Unmarshal into Struct
	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unable to decode config into struct: %v", err)
	}

	// 6. Basic Validation
	if len(cfg.Kafka.Brokers) == 0 {
		return nil, fmt.Errorf("kafka brokers cannot be empty")
	}

	return &cfg, nil
}

// bindEnv is a helper to bind multiple keys at once
func bindEnv(v *viper.Viper, keys ...string) {
	for _, key := range keys {
		if err := v.BindEnv(key); err != nil {
			log.Printf("Could not bind env var for key %s: %v", key, err)
		}
	}
}

// NewLogger creates a Zap logger based on the configuration
func NewLogger(cfg LoggerConfig) (*zap.Logger, error) {
	var config zap.Config

	if cfg.Format == "console" {
		config = zap.NewDevelopmentConfig()
	} else {
		config = zap.NewProductionConfig()
	}

	level, err := zap.ParseAtomicLevel(cfg.Level)
	if err != nil {
		return nil, fmt.Errorf("invalid log level %s: %v", cfg.Level, err)
	}
	config.Level = level

	return config.Build()
}
