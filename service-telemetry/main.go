package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

type DeviceHealth struct {
	DeviceID  string `json:"deviceId"`
	Battery   string `json:"battery"`
	Compliant bool   `json:"compliant"`
}

var ctx = context.Background()
var rdb *redis.Client

func main() {
	// 1. CONNECT TO REDIS
	rdb = redis.NewClient(&redis.Options{
		Addr:     "telemetry-db:6379",
		Password: "",
		DB:       0,
	})

	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("Failed to connect to Redis: %v", err)
	}

	// 2. AUTOMATIC DATA SEEDING
	seedRedis()

	// 3. START THE KAFKA BACKGROUND WORKER
	// The 'go' keyword spins this off into a separate, parallel thread!
	go startKafkaConsumer()

	// 4. SET UP THE API ROUTE
	http.HandleFunc("/health", getHealthHandler)

	// 5. START THE SERVER
	fmt.Println("✅ Telemetry Service running on http://localhost:8082")
	log.Fatal(http.ListenAndServe(":8082", nil))
}

// startKafkaConsumer runs in an infinite loop listening for messages
func startKafkaConsumer() {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{"kafka:29092"}, // Point to our Docker Kafka
		Topic:     "device-events",         // The channel we are listening to
		Partition: 0,
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e6, // 10MB
	})
	defer reader.Close()

	fmt.Println("🎧 Kafka Consumer listening for new devices...")

	for {
		// This will block and wait until a new message arrives
		m, err := reader.ReadMessage(ctx)
		if err != nil {
			fmt.Println("⚠️ Kafka read error:", err)
			continue
		}

		// Parse the JSON message
		var eventPayload map[string]string
		json.Unmarshal(m.Value, &eventPayload)

		// If the event is a DeviceCreated event, react to it!
		if eventPayload["event"] == "DeviceCreated" {
			deviceID := eventPayload["deviceId"]
			fmt.Printf("📥 RECEIVED BROADCAST: Initializing health for %s\n", deviceID)

			// Create a default health profile for the new device
			newHealth := DeviceHealth{
				DeviceID:  deviceID,
				Battery:   "100%", // Brand new phone!
				Compliant: true,
			}

			healthJSON, _ := json.Marshal(newHealth)
			redisKey := "health:" + deviceID

			// Save it to Redis asynchronously
			err = rdb.Set(ctx, redisKey, healthJSON, 0).Err()
			if err != nil {
				fmt.Println("❌ Failed to save to Redis:", err)
			} else {
				fmt.Println("💾 Successfully initialized Redis data for", deviceID)
			}
		}
	}
}

func seedRedis() {
	healthData := map[string]string{
		"health:d290f1ee-6c54-4b01-90e6-d701748f0851": `{"deviceId": "d290f1ee-6c54-4b01-90e6-d701748f0851", "battery": "85%", "compliant": true}`,
		"health:a83b2767-75e1-4c1d-847d-c36bfa332152": `{"deviceId": "a83b2767-75e1-4c1d-847d-c36bfa332152", "battery": "12%", "compliant": false}`,
		"health:f95701eb-4c07-4220-b08e-5b1cf3109a1a": `{"deviceId": "f95701eb-4c07-4220-b08e-5b1cf3109a1a", "battery": "100%", "compliant": true}`,
	}

	for key, value := range healthData {
		rdb.Set(ctx, key, value, 0)
	}
	fmt.Println("📦 Seeded Redis with initial Helios UUID telemetry data.")
}

func getHealthHandler(w http.ResponseWriter, r *http.Request) {
	keys, err := rdb.Keys(ctx, "health:*").Result()
	if err != nil {
		http.Error(w, "Redis error", http.StatusInternalServerError)
		return
	}

	var allHealth []DeviceHealth
	for _, key := range keys {
		val, _ := rdb.Get(ctx, key).Result()
		var h DeviceHealth
		json.Unmarshal([]byte(val), &h)
		allHealth = append(allHealth, h)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(allHealth)
}
