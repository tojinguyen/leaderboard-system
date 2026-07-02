package config

import (
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

type Config struct {
	HTTPServerPort string
	HTTPTimeout    time.Duration

	KafkaBrokers []string
	KafkaTopic   string
	KafkaGroupDB string
	KafkaGroupRD string

	PostgresDSN string
	RedisAddr   string
	RedisPass   string
	RedisDB     int
}

func LoadConfig() *Config {
	// Tải file .env nếu có (dành cho phát triển local)
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found, reading from environment variables")
	}

	brokersStr := getEnv("KAFKA_BROKERS", "localhost:9092")
	brokers := strings.Split(brokersStr, ",")

	redisDBStr := getEnv("REDIS_DB", "0")
	redisDB, err := strconv.Atoi(redisDBStr)
	if err != nil {
		redisDB = 0
	}

	httpTimeoutStr := getEnv("HTTP_TIMEOUT_SECONDS", "10")
	httpTimeoutSec, err := strconv.Atoi(httpTimeoutStr)
	if err != nil {
		httpTimeoutSec = 10
	}

	return &Config{
		HTTPServerPort: getEnv("HTTP_PORT", "8080"),
		HTTPTimeout:    time.Duration(httpTimeoutSec) * time.Second,
		KafkaBrokers:   brokers,
		KafkaTopic:     getEnv("KAFKA_TOPIC", "player-scores"),
		KafkaGroupDB:   getEnv("KAFKA_GROUP_DB", "db-persistence-group"),
		KafkaGroupRD:   getEnv("KAFKA_GROUP_RD", "redis-cache-updater-group"),
		PostgresDSN:    getEnv("POSTGRES_DSN", "postgres://postgres:postgres@localhost:5432/leaderboard?sslmode=disable"),
		RedisAddr:      getEnv("REDIS_ADDR", "localhost:6379"),
		RedisPass:      getEnv("REDIS_PASSWORD", ""),
		RedisDB:        redisDB,
	}
}

func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}
