package http

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"leaderboard-system/internal/domain"
	"leaderboard-system/internal/infrastructure/cache"
)

// ScoreProducer định nghĩa giao tiếp để gửi event (Consumer side)
type ScoreProducer interface {
	PublishScoreEvent(ctx context.Context, event domain.ScoreEvent) error
}

// LeaderboardCache định nghĩa giao tiếp đọc cache (Consumer side)
type LeaderboardCache interface {
	GetTopPlayers(ctx context.Context, limit int, mode string) ([]domain.LeaderboardEntry, error)
	GetUserRank(ctx context.Context, userID string, mode string) (*domain.LeaderboardEntry, error)
	RebuildCache(ctx context.Context, repo cache.RebuildRepository) error
}

type LeaderboardHandler struct {
	producer ScoreProducer
	cache    LeaderboardCache
	dbRepo   cache.RebuildRepository
}

// NewLeaderboardHandler khởi tạo handler cho Leaderboard HTTP APIs
func NewLeaderboardHandler(producer ScoreProducer, cache LeaderboardCache, dbRepo cache.RebuildRepository) *LeaderboardHandler {
	return &LeaderboardHandler{
		producer: producer,
		cache:    cache,
		dbRepo:   dbRepo,
	}
}

type AddScoreRequest struct {
	UserID     string `json:"user_id"`
	ScoreDelta *int64 `json:"score_delta"`
	Timestamp  *int64 `json:"timestamp"` // Unix timestamp tùy chọn từ client
}

// AddScore tiếp nhận thay đổi điểm số của người chơi và gửi lên Kafka
func (h *LeaderboardHandler) AddScore(w http.ResponseWriter, r *http.Request) {
	var req AddScoreRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondWithError(w, http.StatusBadRequest, "Invalid request payload")
		return
	}

	if req.UserID == "" {
		respondWithError(w, http.StatusBadRequest, "user_id is required")
		return
	}

	if req.ScoreDelta == nil {
		respondWithError(w, http.StatusBadRequest, "score_delta is required")
		return
	}

	eventTimestamp := time.Now().Unix()
	if req.Timestamp != nil {
		eventTimestamp = *req.Timestamp
	}

	event := domain.ScoreEvent{
		UserID:     req.UserID,
		ScoreDelta: *req.ScoreDelta,
		Timestamp:  eventTimestamp,
	}

	// Gửi event lên Kafka một cách đồng bộ/bất đồng bộ từ Gateway
	if err := h.producer.PublishScoreEvent(r.Context(), event); err != nil {
		respondWithError(w, http.StatusInternalServerError, "Failed to publish score event: "+err.Error())
		return
	}

	respondWithJSON(w, http.StatusAccepted, map[string]string{"status": "accepted"})
}

// GetTopPlayers trả về Top N người chơi toàn cầu dựa trên mode (daily, weekly, monthly, alltime)
func (h *LeaderboardHandler) GetTopPlayers(w http.ResponseWriter, r *http.Request) {
	nStr := r.URL.Query().Get("n")
	n := 100 // Mặc định là 100 người
	if nStr != "" {
		if parsedN, err := strconv.Atoi(nStr); err == nil && parsedN > 0 {
			n = parsedN
		}
	}

	mode := r.URL.Query().Get("mode")
	if mode == "" {
		mode = "alltime"
	}

	entries, err := h.cache.GetTopPlayers(r.Context(), n, mode)
	if err != nil {
		respondWithError(w, http.StatusInternalServerError, "Failed to retrieve top players: "+err.Error())
		return
	}

	respondWithJSON(w, http.StatusOK, entries)
}

// GetUserRank trả về rank (1-indexed) và điểm số hiện tại của user_id cụ thể dựa trên mode
func (h *LeaderboardHandler) GetUserRank(w http.ResponseWriter, r *http.Request) {
	userID := chi.URLParam(r, "user_id")
	if userID == "" {
		respondWithError(w, http.StatusBadRequest, "user_id path parameter is required")
		return
	}

	mode := r.URL.Query().Get("mode")
	if mode == "" {
		mode = "alltime"
	}

	entry, err := h.cache.GetUserRank(r.Context(), userID, mode)
	if err != nil {
		if errors.Is(err, cache.ErrUserNotFound) {
			respondWithError(w, http.StatusNotFound, "User not found on the leaderboard")
			return
		}
		respondWithError(w, http.StatusInternalServerError, "Failed to retrieve user rank: "+err.Error())
		return
	}

	respondWithJSON(w, http.StatusOK, entry)
}

func respondWithError(w http.ResponseWriter, code int, message string) {
	respondWithJSON(w, code, map[string]string{"error": message})
}

func respondWithJSON(w http.ResponseWriter, code int, payload interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(payload)
}

// RebuildCache kích hoạt quá trình rebuild cache từ Postgres sang Sharded Redis
func (h *LeaderboardHandler) RebuildCache(w http.ResponseWriter, r *http.Request) {
	if err := h.cache.RebuildCache(r.Context(), h.dbRepo); err != nil {
		respondWithError(w, http.StatusInternalServerError, "Failed to rebuild cache: "+err.Error())
		return
	}
	respondWithJSON(w, http.StatusOK, map[string]string{"status": "rebuild completed successfully"})
}
