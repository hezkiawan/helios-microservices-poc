package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	_ "github.com/lib/pq" // The Postgres driver
)

// Device represents our static MDM registry data
type Device struct {
	ID    string `json:"id"`
	Model string `json:"model"`
	OS    string `json:"os"`
	Owner string `json:"owner"`
}

var db *sql.DB

func main() {
	// 1. CONNECT TO POSTGRES
	// Notice we use 'localhost' because we are currently running this Go code directly on Windows,
	// communicating with the exposed Docker port 5432.
	connStr := "host=registry-db port=5432 user=helios_admin password=supersecret dbname=registry_db sslmode=disable"
	var err error

	db, err = sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal("Failed to open DB connection:", err)
	}
	defer db.Close()

	// 2. AUTOMATIC DB MIGRATION
	// This ensures the table exists and has dummy data so you don't have to write manual SQL scripts.
	initDB()

	// 3. SET UP THE API ROUTE
	http.HandleFunc("/devices", getDevicesHandler)

	// 4. START THE SERVER
	fmt.Println("✅ Registry Service running on http://localhost:8081")
	log.Fatal(http.ListenAndServe(":8081", nil))
}

// initDB creates our tables and injects test data if the database is empty
func initDB() {
	// Create the table
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

	// Check if data already exists
	var count int
	db.QueryRow("SELECT COUNT(*) FROM devices").Scan(&count)

	if count == 0 {
		// Insert our Helios test devices
		_, err = db.Exec(`
			INSERT INTO devices (id, model, os, owner) VALUES 
			('dev-001', 'iPhone 15 Pro', 'iOS 17', 'Hezki'),
			('dev-002', 'Samsung Galaxy S24', 'Android 14', 'Alice'),
			('dev-003', 'MacBook Pro M3', 'macOS Sonoma', 'Bob');
		`)
		if err != nil {
			log.Fatalf("Error inserting dummy data: %v", err)
		}
		fmt.Println("📦 Seeded PostgreSQL with initial Helios devices.")
	}
}

// getDevicesHandler fetches data from Postgres and returns it as JSON
func getDevicesHandler(w http.ResponseWriter, r *http.Request) {
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

	// Send the JSON response to the client
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(devices)
}
