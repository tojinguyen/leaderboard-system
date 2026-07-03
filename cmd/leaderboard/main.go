package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"leaderboard-system/internal/config"
	deliveryHTTP "leaderboard-system/internal/delivery/http"
	"leaderboard-system/internal/infrastructure/cache"
	"leaderboard-system/internal/infrastructure/db"
	"leaderboard-system/internal/infrastructure/kafka"
	"leaderboard-system/internal/service"
)

func main() {
	log.Println("=== Initializing Real-Time Leaderboard Service ===")

	// 1. Tải cấu hình từ config (.env & env vars)
	cfg := config.LoadConfig()

	// 2. Khởi tạo root context để điều phối shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 3. Khởi tạo PostgreSQL repository (có retry)
	repo, err := db.NewPostgresRepository(ctx, cfg.PostgresDSN)
	if err != nil {
		log.Fatalf("Fatal: failed to connect to PostgreSQL: %v", err)
	}
	defer func() {
		log.Println("Gracefully closing PostgreSQL connection pool...")
		if err := repo.Close(); err != nil {
			log.Printf("Error closing PostgreSQL: %v", err)
		}
	}()

	// 4. Khởi tạo Sharded Redis cache (có retry)
	redisCache, err := cache.NewShardedRedisCache(ctx, cfg.RedisShards)
	if err != nil {
		log.Fatalf("Fatal: failed to initialize Sharded Redis: %v", err)
	}
	defer func() {
		log.Println("Gracefully closing Redis shards...")
		if err := redisCache.Close(); err != nil {
			log.Printf("Error closing Redis shards: %v", err)
		}
	}()

	// 5. Khởi tạo Kafka Producer
	producer, err := kafka.NewKafkaProducer(cfg.KafkaBrokers, cfg.KafkaTopic)
	if err != nil {
		log.Fatalf("Fatal: failed to initialize Kafka Producer: %v", err)
	}
	defer func() {
		log.Println("Gracefully closing Kafka Producer...")
		if err := producer.Close(); err != nil {
			log.Printf("Error closing Kafka Producer: %v", err)
		}
	}()

	// 6. Khởi tạo Consumer Service chứa logic nghiệp vụ
	consumerSvc := service.NewConsumerService(repo, redisCache)

	// 7. Khởi tạo và chạy 2 Consumer Group độc lập
	dbConsumer := kafka.NewKafkaConsumer(cfg.KafkaBrokers, cfg.KafkaTopic, cfg.KafkaGroupDB)
	redisConsumer := kafka.NewKafkaConsumer(cfg.KafkaBrokers, cfg.KafkaTopic, cfg.KafkaGroupRD)

	go dbConsumer.Start(ctx, consumerSvc.HandleDBConsumption)
	go redisConsumer.Start(ctx, consumerSvc.HandleRedisConsumption)

	defer func() {
		log.Println("Gracefully closing Kafka Consumers...")
		if err := dbConsumer.Close(); err != nil {
			log.Printf("Error closing DB Consumer: %v", err)
		}
		if err := redisConsumer.Close(); err != nil {
			log.Printf("Error closing Redis Consumer: %v", err)
		}
	}()

	// 8. Khởi tạo HTTP Gateway Server
	httpHandler := deliveryHTTP.NewLeaderboardHandler(producer, redisCache, repo)
	router := deliveryHTTP.SetupRouter(httpHandler)

	server := &http.Server{
		Addr:         ":" + cfg.HTTPServerPort,
		Handler:      router,
		ReadTimeout:  cfg.HTTPTimeout,
		WriteTimeout: cfg.HTTPTimeout,
	}

	go func() {
		log.Printf("HTTP Gateway Server is running on port %s", cfg.HTTPServerPort)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("Fatal: HTTP Server crashed: %v", err)
		}
	}()

	// 9. Lắng nghe tín hiệu từ OS để thực hiện Graceful Shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	<-quit
	log.Println("Shutdown signal received! Starting graceful shutdown process...")

	// Gửi tín hiệu cancel để các consumers thoát khỏi loop và dừng đọc Kafka
	cancel()

	// Đóng cổng HTTP Gateway, không nhận request mới
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("HTTP Server forced to shutdown: %v", err)
	} else {
		log.Println("HTTP Server stopped successfully.")
	}

	// Defer blocks sẽ tự động đóng kết nối DB, Redis, Kafka theo thứ tự ngược lại (LIFO)
	log.Println("=== Real-Time Leaderboard Service shutdown complete ===")
}
