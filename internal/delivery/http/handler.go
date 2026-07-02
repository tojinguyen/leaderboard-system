package http

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
	"leaderboard-system/internal/domain"
	"leaderboard-system/internal/infrastructure/cache"
)

type LeaderboardHandler struct {
	producer domain.ScoreProducer
	cache    domain.LeaderboardCache
}

// NewLeaderboardHandler khởi tạo handler cho Leaderboard HTTP APIs
func NewLeaderboardHandler(producer domain.ScoreProducer, cache domain.LeaderboardCache) *LeaderboardHandler {
	return &LeaderboardHandler{
		producer: producer,
		cache:    cache,
	}
}

type AddScoreRequest struct {
	UserID     string `json:"user_id"`
	ScoreDelta *int64 `json:"score_delta"` // Pointer giúp kiểm tra xem trường này có được truyền hay không
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

	event := domain.ScoreEvent{
		UserID:     req.UserID,
		ScoreDelta: *req.ScoreDelta,
	}

	// Gửi event lên Kafka một cách đồng bộ/bất đồng bộ từ Gateway
	if err := h.producer.PublishScoreEvent(r.Context(), event); err != nil {
		respondWithError(w, http.StatusInternalServerError, "Failed to publish score event: "+err.Error())
		return
	}

	respondWithJSON(w, http.StatusAccepted, map[string]string{"status": "accepted"})
}

// GetTopPlayers trả về Top N người chơi toàn cầu
func (h *LeaderboardHandler) GetTopPlayers(w http.ResponseWriter, r *http.Request) {
	nStr := r.URL.Query().Get("n")
	n := 100 // Mặc định là 100 người
	if nStr != "" {
		if parsedN, err := strconv.Atoi(nStr); err == nil && parsedN > 0 {
			n = parsedN
		}
	}

	entries, err := h.cache.GetTopPlayers(r.Context(), n)
	if err != nil {
		respondWithError(w, http.StatusInternalServerError, "Failed to retrieve top players: "+err.Error())
		return
	}

	respondWithJSON(w, http.StatusOK, entries)
}

// GetUserRank trả về rank (1-indexed) và điểm số hiện tại của user_id cụ thể
func (h *LeaderboardHandler) GetUserRank(w http.ResponseWriter, r *http.Request) {
	userID := chi.URLParam(r, "user_id")
	if userID == "" {
		respondWithError(w, http.StatusBadRequest, "user_id path parameter is required")
		return
	}

	entry, err := h.cache.GetUserRank(r.Context(), userID)
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
