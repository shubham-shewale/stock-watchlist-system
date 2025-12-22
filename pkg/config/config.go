package config

import (
	"fmt"
	"log"
	"strings"

	"github.com/joho/godotenv"
	"github.com/spf13/viper"
)

// Config holds all configuration for the application
type Config struct {
	App   AppConfig   `mapstructure:"app"`
	Redis RedisConfig `mapstructure:"redis"`
	Kafka KafkaConfig `mapstructure:"kafka"`
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

	// 3. Configure Viper to read Environment Variables
	// This maps dot-notation to underscores (e.g., "app.port" -> "APP_PORT")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	// 4. Explicitly Bind Env Vars to Keys
	// This is crucial for Viper to map flat Env Vars (APP_PORT) to nested structs (App.Port)
	bindEnv(v, "app.port", "app.env")
	bindEnv(v, "redis.addr", "redis.password", "redis.db")
	bindEnv(v, "kafka.brokers", "kafka.topic", "kafka.group_id")

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
