package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"sync"
	"time"
)

type AddScoreRequest struct {
	UserID     string `json:"user_id"`
	ScoreDelta int64  `json:"score_delta"`
}

type LeaderboardEntry struct {
	UserID string `json:"user_id"`
	Score  int64  `json:"score"`
	Rank   int64  `json:"rank"`
}

func main() {
	log.Println("=== Initializing highly concurrent Leaderboard Client Simulator ===")
	rand.Seed(time.Now().UnixNano())

	// Số lượng người chơi ảo
	numPlayers := 20
	players := make([]string, numPlayers)
	for i := 0; i < numPlayers; i++ {
		players[i] = fmt.Sprintf("player_%d", i+1)
	}

	// Lưu trữ điểm dự kiến của client để đối chiếu
	expectedScores := make(map[string]int64)
	var expectedMutex sync.Mutex

	var wg sync.WaitGroup
	numRequests := 500
	concurrencyLimit := 20
	sem := make(chan struct{}, concurrencyLimit)

	client := &http.Client{Timeout: 5 * time.Second}

	log.Printf("Sending %d score update requests concurrently (concurrency limit: %d)...", numRequests, concurrencyLimit)
	startTime := time.Now()

	for i := 0; i < numRequests; i++ {
		wg.Add(1)
		go func(reqIndex int) {
			defer wg.Done()
			sem <- struct{}{}        // Acquire token
			defer func() { <-sem }() // Release token

			// Chọn user ngẫu nhiên
			user := players[rand.Intn(numPlayers)]
			delta := int64(rand.Intn(201) - 100) // Delta ngẫu nhiên từ -100 đến 100

			// Cập nhật điểm dự kiến
			expectedMutex.Lock()
			expectedScores[user] += delta
			expectedMutex.Unlock()

			// Gửi request POST
			reqBody, _ := json.Marshal(AddScoreRequest{
				UserID:     user,
				ScoreDelta: delta,
			})

			resp, err := client.Post("http://localhost:8080/api/v1/scores", "application/json", bytes.NewBuffer(reqBody))
			if err != nil {
				log.Printf("[Req %d] Failed: %v", reqIndex, err)
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusAccepted {
				bodyBytes, _ := io.ReadAll(resp.Body)
				log.Printf("[Req %d] Rejected with status %d: %s", reqIndex, resp.StatusCode, string(bodyBytes))
			}
		}(i)
	}

	wg.Wait()
	duration := time.Since(startTime)
	log.Printf("All %d requests sent in %v (Avg: %v/req)", numRequests, duration, duration/time.Duration(numRequests))

	// Chờ 3 giây để Kafka Consumers xử lý hết hàng đợi
	log.Println("Waiting 3 seconds for Kafka consumer processing and DB/Cache sync...")
	time.Sleep(3 * time.Second)

	// Gọi API lấy Top Leaderboard
	log.Println("Retrieving top leaderboard from API...")
	resp, err := client.Get("http://localhost:8080/api/v1/leaderboard/top?n=20")
	if err != nil {
		log.Fatalf("Failed to fetch leaderboard: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		log.Fatalf("Failed to fetch leaderboard. Status: %d, Body: %s", resp.StatusCode, string(bodyBytes))
	}

	var leaderboard []LeaderboardEntry
	if err := json.NewDecoder(resp.Body).Decode(&leaderboard); err != nil {
		log.Fatalf("Failed to decode leaderboard response: %v", err)
	}

	log.Println("\n=== TOP 20 GLOBAL LEADERBOARD ===")
	for _, entry := range leaderboard {
		expected := expectedScores[entry.UserID]
		matchStatus := "MATCH"
		if entry.Score != expected {
			matchStatus = fmt.Sprintf("MISMATCH (Expected: %d)", expected)
		}
		log.Printf("Rank #%d: User [%-9s] | Score: %5d | Status: %s", entry.Rank, entry.UserID, entry.Score, matchStatus)
	}

	// Kiểm tra rank cụ thể của một user ngẫu nhiên
	testUser := fmt.Sprintf("player_%d", rand.Intn(numPlayers)+1)
	log.Printf("\nQuerying rank of specific user: %s", testUser)
	respUser, err := client.Get(fmt.Sprintf("http://localhost:8080/api/v1/leaderboard/user/%s", testUser))
	if err != nil {
		log.Fatalf("Failed to fetch user rank: %v", err)
	}
	defer respUser.Body.Close()

	if respUser.StatusCode == http.StatusOK {
		var userEntry LeaderboardEntry
		_ = json.NewDecoder(respUser.Body).Decode(&userEntry)
		log.Printf("User: %s | Rank: #%d | Score: %d | Expected Score: %d", userEntry.UserID, userEntry.Rank, userEntry.Score, expectedScores[testUser])
	} else {
		bodyBytes, _ := io.ReadAll(respUser.Body)
		log.Printf("Failed to get rank of user %s. Status: %d, Body: %s", testUser, respUser.StatusCode, string(bodyBytes))
	}
}
