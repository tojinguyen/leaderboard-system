package http

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

// SetupRouter khởi tạo chi router và đăng ký các HTTP API endpoint
func SetupRouter(handler *LeaderboardHandler) http.Handler {
	r := chi.NewRouter()

	// Đăng ký các Middleware cơ bản
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)

	// Định nghĩa API endpoints
	r.Route("/api/v1", func(r chi.Router) {
		r.Post("/scores", handler.AddScore)
		r.Get("/leaderboard/top", handler.GetTopPlayers)
		r.Get("/leaderboard/user/{user_id}", handler.GetUserRank)
	})

	return r
}
