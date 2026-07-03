package db

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresRepository struct {
	pool *pgxpool.Pool
}

// NewPostgresRepository khởi tạo kết nối PostgreSQL với cơ chế Retry Exponential Backoff
func NewPostgresRepository(ctx context.Context, dsn string) (*PostgresRepository, error) {
	var pool *pgxpool.Pool
	var err error

	backoff := 1 * time.Second
	maxBackoff := 30 * time.Second

	for {
		pool, err = pgxpool.New(ctx, dsn)
		if err == nil {
			err = pool.Ping(ctx)
			if err == nil {
				log.Println("Successfully connected to PostgreSQL")
				break
			}
		}

		log.Printf("Failed to connect to PostgreSQL, retrying in %v: %v", backoff, err)
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("context cancelled during DB connection retry: %w", ctx.Err())
		case <-time.After(backoff):
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}

	return &PostgresRepository{pool: pool}, nil
}

// UpsertScore cộng dồn scoreDelta vào điểm số hiện tại của user_id trong DB
func (r *PostgresRepository) UpsertScore(ctx context.Context, userID string, scoreDelta int64) error {
	query := `
		INSERT INTO leaderboards (user_id, total_score)
		VALUES ($1, $2)
		ON CONFLICT (user_id)
		DO UPDATE SET total_score = leaderboards.total_score + EXCLUDED.total_score, updated_at = CURRENT_TIMESTAMP;
	`
	_, err := r.pool.Exec(ctx, query, userID, scoreDelta)
	if err != nil {
		return fmt.Errorf("failed to upsert score: %w", err)
	}
	return nil
}

// Close giải phóng connection pool
func (r *PostgresRepository) Close() error {
	if r.pool != nil {
		r.pool.Close()
	}
	return nil
}
