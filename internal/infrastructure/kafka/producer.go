package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
	"leaderboard-system/internal/domain"
)

type KafkaProducer struct {
	writer *kafka.Writer
}

// NewKafkaProducer khởi tạo Kafka Writer để publish score events
func NewKafkaProducer(brokers []string, topic string) (domain.ScoreProducer, error) {
	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.Hash{}, // Hash balancer đảm bảo cùng một partition key (user_id) sẽ luôn vào cùng partition
		MaxAttempts:  5,
		WriteTimeout: 10 * time.Second,
		RequiredAcks: kafka.RequireAll, // Đảm bảo độ tin cậy tối đa (acks=all)
	}

	log.Printf("Kafka Producer initialized for topic: %s", topic)
	return &KafkaProducer{writer: writer}, nil
}

// PublishScoreEvent gửi score event lên Kafka, sử dụng UserID làm partition key
func (p *KafkaProducer) PublishScoreEvent(ctx context.Context, event domain.ScoreEvent) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal score event: %w", err)
	}

	msg := kafka.Message{
		Key:   []byte(event.UserID), // Đảm bảo tính tuần tự theo từng user
		Value: payload,
	}

	err = p.writer.WriteMessages(ctx, msg)
	if err != nil {
		return fmt.Errorf("failed to write message to Kafka: %w", err)
	}

	return nil
}

// Close đóng producer connection
func (p *KafkaProducer) Close() error {
	if p.writer != nil {
		return p.writer.Close()
	}
	return nil
}
