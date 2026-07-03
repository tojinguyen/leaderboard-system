package config

import (
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

type RedisShardConfig struct {
	MinScore int64
	MaxScore int64 // Dùng math.MaxInt64 cho shard cuối
	Addr     string
	Password string
	DB       int
}

type Config struct {
	HTTPServerPort string
	HTTPTimeout    time.Duration

	KafkaBrokers []string
	KafkaTopic   string
	KafkaGroupDB string
	KafkaGroupRD string

	PostgresDSN string
	RedisShards []RedisShardConfig
}

func LoadConfig() *Config {
	// Tải file .env nếu có (dành cho phát triển local)
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found, reading from environment variables")
	}

	brokersStr := getEnv("KAFKA_BROKERS", "localhost:9092")
	brokers := strings.Split(brokersStr, ",")

	redisPass := getEnv("REDIS_PASSWORD", "")
	shardsStr := getEnv("REDIS_SHARDS", "0:999@localhost:6379/0,1000:4999@localhost:6379/1,5000:max@localhost:6379/2")
	shards := parseRedisShards(shardsStr, redisPass)

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
		RedisShards:    shards,
	}
}

func parseRedisShards(shardsStr, redisPass string) []RedisShardConfig {
	var shardConfigs []RedisShardConfig
	parts := strings.Split(shardsStr, ",")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		// định dạng: min:max@addr/db
		// Ví dụ: 0:999@localhost:6379/0
		atParts := strings.Split(part, "@")
		if len(atParts) != 2 {
			log.Printf("Invalid shard format: %s", part)
			continue
		}

		rangeStr := atParts[0]
		connStr := atParts[1]

		// Phân tích dải điểm (rangeStr)
		rangeParts := strings.Split(rangeStr, ":")
		if len(rangeParts) != 2 {
			log.Printf("Invalid shard range format: %s", rangeStr)
			continue
		}

		var minScore, maxScore int64
		var err error

		minScore, err = strconv.ParseInt(rangeParts[0], 10, 64)
		if err != nil {
			log.Printf("Invalid min score %s: %v", rangeParts[0], err)
			continue
		}

		if strings.ToLower(rangeParts[1]) == "max" {
			maxScore = 9223372036854775807 // math.MaxInt64
		} else {
			maxScore, err = strconv.ParseInt(rangeParts[1], 10, 64)
			if err != nil {
				log.Printf("Invalid max score %s: %v", rangeParts[1], err)
				continue
			}
		}

		// Phân tích kết nối (connStr) dạng addr/db
		slashParts := strings.Split(connStr, "/")
		if len(slashParts) != 2 {
			log.Printf("Invalid shard connection format: %s", connStr)
			continue
		}

		addr := slashParts[0]
		dbVal, err := strconv.Atoi(slashParts[1])
		if err != nil {
			log.Printf("Invalid DB value %s: %v", slashParts[1], err)
			continue
		}

		shardConfigs = append(shardConfigs, RedisShardConfig{
			MinScore: minScore,
			MaxScore: maxScore,
			Addr:     addr,
			Password: redisPass,
			DB:       dbVal,
		})
	}
	return shardConfigs
}

func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}
