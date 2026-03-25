package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/redis/go-redis/v9"
)

// DeviceHealth represents the fast-moving telemetry data
type DeviceHealth struct {
	DeviceID  string `json:"deviceId"`
	Battery   string `json:"battery"`
	Compliant bool   `json:"compliant"`
}

var ctx = context.Background()
var rdb *redis.Client

func main() {
	// 1. CONNECT TO REDIS
	// Connecting to the Redis container we spun up on port 6379
	rdb = redis.NewClient(&redis.Options{
		Addr:     "telemetry-db:6379",
		Password: "", // No password set for local dev
		DB:       0,  // Default DB
	})

	// Test the connection
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("Failed to connect to Redis: %v", err)
	}

	// 2. AUTOMATIC DATA SEEDING
	seedRedis()

	// 3. SET UP THE API ROUTE
	http.HandleFunc("/health", getHealthHandler)

	// 4. START THE SERVER
	fmt.Println("✅ Telemetry Service running on http://localhost:8082")
	log.Fatal(http.ListenAndServe(":8082", nil))
}

// seedRedis injects some dummy telemetry data so we have something to fetch
func seedRedis() {
	// We map the health data to the exact same IDs we used in PostgreSQL
	healthData := map[string]string{
		"health:dev-001": `{"deviceId": "dev-001", "battery": "85%", "compliant": true}`,
		"health:dev-002": `{"deviceId": "dev-002", "battery": "12%", "compliant": false}`,
		"health:dev-003": `{"deviceId": "dev-003", "battery": "100%", "compliant": true}`,
	}

	for key, value := range healthData {
		err := rdb.Set(ctx, key, value, 0).Err()
		if err != nil {
			log.Fatalf("Failed to seed Redis: %v", err)
		}
	}
	fmt.Println("📦 Seeded Redis with initial Helios telemetry data.")
}

// getHealthHandler fetches all health records from Redis
func getHealthHandler(w http.ResponseWriter, r *http.Request) {
	// Find all keys in Redis that start with "health:"
	keys, err := rdb.Keys(ctx, "health:*").Result()
	if err != nil {
		http.Error(w, "Redis error", http.StatusInternalServerError)
		return
	}

	var allHealth []DeviceHealth
	for _, key := range keys {
		val, err := rdb.Get(ctx, key).Result()
		if err != nil {
			continue
		}

		var h DeviceHealth
		json.Unmarshal([]byte(val), &h)
		allHealth = append(allHealth, h)
	}

	// Send the JSON response to the client
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(allHealth)
}
