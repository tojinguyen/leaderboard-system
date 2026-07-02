package kafka

import (
	"context"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

// MessageHandler là kiểu hàm callback để xử lý message nhận được
type MessageHandler func(ctx context.Context, msg kafka.Message) error

type KafkaConsumer struct {
	reader *kafka.Reader
}

// NewKafkaConsumer khởi tạo một Kafka Consumer Group
func NewKafkaConsumer(brokers []string, topic string, groupID string) *KafkaConsumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		GroupID:        groupID,
		Topic:          topic,
		MinBytes:       10e3, // 10KB
		MaxBytes:       10e6, // 10MB
		MaxWait:        1 * time.Second,
		CommitInterval: 0, // Thiết lập CommitInterval = 0 để tắt auto-commit hoàn toàn
		StartOffset:    kafka.FirstOffset,
	})

	return &KafkaConsumer{reader: reader}
}

// Start khởi chạy vòng lặp consume tin nhắn và xử lý đồng bộ
func (c *KafkaConsumer) Start(ctx context.Context, handler MessageHandler) {
	log.Printf("Consumer group [%s] started listening to topic [%s]", c.reader.Config().GroupID, c.reader.Config().Topic)

	for {
		select {
		case <-ctx.Done():
			log.Printf("Stopping consumer group [%s] due to context cancellation", c.reader.Config().GroupID)
			return
		default:
			// FetchMessage nhận message về nhưng chưa commit offset
			msg, err := c.reader.FetchMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("Consumer [%s] error fetching message: %v", c.reader.Config().GroupID, err)
				time.Sleep(1 * time.Second) // Nghỉ một chút trước khi thử lại
				continue
			}

			// Gọi handler để xử lý logic nghiệp vụ (DB upsert hoặc Redis update)
			err = handler(ctx, msg)
			if err != nil {
				log.Printf("Consumer [%s] failed to process message at offset %d: %v. Retrying offset without commit...", c.reader.Config().GroupID, msg.Offset, err)
				// Do handler đã có cơ chế retry nội bộ nhưng vẫn lỗi (ví dụ DB chết hẳn),
				// ta KHÔNG commit offset để message không bị mất. Consumer sẽ tiếp tục consume lại partition này sau khi restart.
				continue
			}

			// Chỉ commit offset đồng bộ sau khi handler xử lý THÀNH CÔNG
			err = c.reader.CommitMessages(ctx, msg)
			if err != nil {
				log.Printf("Consumer [%s] failed to commit offset %d: %v", c.reader.Config().GroupID, msg.Offset, err)
			}
		}
	}
}

// Close đóng consumer connection
func (c *KafkaConsumer) Close() error {
	if c.reader != nil {
		return c.reader.Close()
	}
	return nil
}

type ConsumerManager struct {
	Consumers []*KafkaConsumer
}

func (cm *ConsumerManager) CloseAll() {
	for _, consumer := range cm.Consumers {
		if err := consumer.Close(); err != nil {
			log.Printf("Error closing consumer: %v", err)
		}
	}
}
