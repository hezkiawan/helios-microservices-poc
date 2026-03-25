package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	_ "github.com/lib/pq"
	"github.com/segmentio/kafka-go"

	"github.com/google/uuid"
)

// Device represents our static MDM registry data
type Device struct {
	ID    string `json:"id"`
	Model string `json:"model"`
	OS    string `json:"os"`
	Owner string `json:"owner"`
}

var db *sql.DB
var kafkaWriter *kafka.Writer // 📻 Our Kafka broadcaster

func main() {
	// 1. CONNECT TO POSTGRES
	connStr := "host=registry-db port=5432 user=helios_admin password=supersecret dbname=registry_db sslmode=disable"
	var err error

	db, err = sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal("Failed to open DB connection:", err)
	}
	defer db.Close()

	// 2. CONNECT TO KAFKA
	// We point the writer to our internal Docker Kafka network
	kafkaWriter = &kafka.Writer{
		Addr:     kafka.TCP("kafka:29092"), // Internal Docker routing
		Topic:    "device-events",          // The "Radio Channel"
		Balancer: &kafka.LeastBytes{},
	}
	defer kafkaWriter.Close()

	// 3. AUTOMATIC DB MIGRATION
	initDB()

	// 4. SET UP THE API ROUTES (Now handling both GET and POST)
	http.HandleFunc("/devices", devicesHandler)

	// 5. START THE SERVER
	fmt.Println("✅ Registry Service running on http://localhost:8081")
	log.Fatal(http.ListenAndServe(":8081", nil))
}

func initDB() {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS devices (
			id VARCHAR(50) PRIMARY KEY,
			model VARCHAR(50),
			os VARCHAR(50),
			owner VARCHAR(50)
		);
	`)
	if err != nil {
		log.Fatalf("Error creating table: %v", err)
	}

	var count int
	db.QueryRow("SELECT COUNT(*) FROM devices").Scan(&count)

	if count == 0 {
		_, err = db.Exec(`
			INSERT INTO devices (id, model, os, owner) VALUES 
			('d290f1ee-6c54-4b01-90e6-d701748f0851', 'iPhone 15 Pro', 'iOS 17', 'Hezki'),
			('a83b2767-75e1-4c1d-847d-c36bfa332152', 'Samsung Galaxy S24', 'Android 14', 'Alice'),
			('f95701eb-4c07-4220-b08e-5b1cf3109a1a', 'MacBook Pro M3', 'macOS Sonoma', 'Bob');
		`)
		if err != nil {
			log.Fatalf("Error inserting dummy data: %v", err)
		}
		fmt.Println("📦 Seeded PostgreSQL with initial Helios UUID devices.")
	}
}

// ROUTER: Directs traffic based on HTTP method
func devicesHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		getDevices(w, r)
	} else if r.Method == http.MethodPost {
		createDevice(w, r)
	} else {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// GET: Fetches data from Postgres
func getDevices(w http.ResponseWriter, r *http.Request) {
	rows, err := db.Query("SELECT id, model, os, owner FROM devices")
	if err != nil {
		http.Error(w, "Database error", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var devices []Device
	for rows.Next() {
		var d Device
		if err := rows.Scan(&d.ID, &d.Model, &d.OS, &d.Owner); err != nil {
			continue
		}
		devices = append(devices, d)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(devices)
}

// POST: Creates data and broadcasts an event
// POST: Creates data, assigns a UUID, and broadcasts an event
func createDevice(w http.ResponseWriter, r *http.Request) {
	var d Device
	if err := json.NewDecoder(r.Body).Decode(&d); err != nil {
		http.Error(w, "Invalid input", http.StatusBadRequest)
		return
	}

	// 🚨 THE MAGIC: Auto-generate the UUID right here on the server
	// It will look something like this: "550e8400-e29b-41d4-a716-446655440000"
	d.ID = uuid.New().String()

	// Step 1: Save to Postgres (now using the generated UUID)
	_, err := db.Exec("INSERT INTO devices (id, model, os, owner) VALUES ($1, $2, $3, $4)", d.ID, d.Model, d.OS, d.Owner)
	if err != nil {
		http.Error(w, "Failed to save device", http.StatusInternalServerError)
		return
	}

	// Step 2: Formulate the Event payload with the new UUID
	eventPayload := map[string]string{
		"event":    "DeviceCreated",
		"deviceId": d.ID,
	}
	eventBytes, _ := json.Marshal(eventPayload)

	// Step 3: Broadcast the message to Kafka
	err = kafkaWriter.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte(d.ID),
			Value: eventBytes,
		},
	)

	if err != nil {
		fmt.Println("⚠️ Failed to publish event to Kafka:", err)
	} else {
		fmt.Printf("📢 BROADCAST SENT: Device %s created\n", d.ID)
	}

	w.WriteHeader(http.StatusCreated)

	// Send the final object (including the new UUID) back to the Next.js frontend
	json.NewEncoder(w).Encode(d)
}
