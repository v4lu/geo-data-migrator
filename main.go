package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"runtime"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	_ "github.com/mattn/go-sqlite3"
)

type Country struct {
	ID        int64   `json:"id"`
	Name      string  `json:"name"`
	ISOCode   string  `json:"iso_code"`
	Flag      string  `json:"flag"`
	Latitude  float64 `json:"lat"`
	Longitude float64 `json:"lng"`
}

type City struct {
	ID             int64   `json:"id"`
	Name           string  `json:"name"`
	CountryISOCode string  `json:"country_iso_code"`
	Latitude       float64 `json:"lat"`
	Longitude      float64 `json:"lng"`
}

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	if len(os.Args) != 2 {
		log.Fatal("Usage: go run . [country|city]")
	}

	sqliteDB, err := sql.Open("sqlite3", "./db.sqlite3")
	if err != nil {
		log.Fatal(err)
	}
	defer sqliteDB.Close()

	pgPool, err := pgxpool.New(context.Background(), os.Getenv("DB_URL"))
	if err != nil {
		log.Fatal(err)
	}
	defer pgPool.Close()

	switch os.Args[1] {
	case "country":
		migrateCountries(sqliteDB, pgPool)
	case "city":
		migrateCitiesConcurrently(sqliteDB, pgPool)
	default:
		log.Fatal("Invalid argument. Use 'country' or 'city'")
	}
}

func migrateCountries(sqliteDB *sql.DB, pgPool *pgxpool.Pool) {
	rows, err := sqliteDB.Query("SELECT name, iso2, emoji, latitude, longitude FROM countries")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	tx, err := pgPool.Begin(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	defer tx.Rollback(context.Background())

	_, err = tx.Prepare(context.Background(), "insert_country",
		"INSERT INTO countries (name, iso_code, flag, lat, lng) VALUES ($1, $2, $3, $4, $5)")
	if err != nil {
		log.Fatal(err)
	}

	var count int
	for rows.Next() {
		var country Country
		err := rows.Scan(&country.Name, &country.ISOCode, &country.Flag, &country.Latitude, &country.Longitude)
		if err != nil {
			log.Printf("Error scanning row: %v", err)
			continue
		}

		_, err = tx.Exec(context.Background(), "insert_country",
			country.Name, country.ISOCode, country.Flag, country.Latitude, country.Longitude)
		if err != nil {
			log.Printf("Error inserting country %s: %v", country.Name, err)
			continue
		}
		count++
	}

	err = tx.Commit(context.Background())
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Migrated %d countries from SQLite to PostgreSQL\n", count)
}

func migrateCitiesConcurrently(sqliteDB *sql.DB, pgPool *pgxpool.Pool) {
	rows, err := sqliteDB.Query("SELECT name, country_code, latitude, longitude FROM cities")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	numWorkers := runtime.NumCPU() * 2
	cityChannel := make(chan City, numWorkers)
	var wg sync.WaitGroup

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go worker(pgPool, cityChannel, &wg)
	}

	go func() {
		for rows.Next() {
			var city City
			err := rows.Scan(&city.Name, &city.CountryISOCode, &city.Latitude, &city.Longitude)
			if err != nil {
				log.Printf("Error scanning row: %v", err)
				continue
			}
			cityChannel <- city
		}
		close(cityChannel)
	}()

	wg.Wait()
	fmt.Println("City migration completed")
}

func worker(pgPool *pgxpool.Pool, cityChannel <-chan City, wg *sync.WaitGroup) {
	defer wg.Done()

	ctx := context.Background()
	tx, err := pgPool.Begin(ctx)
	if err != nil {
		log.Printf("Error starting transaction: %v", err)
		return
	}
	defer tx.Rollback(ctx)

	_, err = tx.Prepare(ctx, "insert_city",
		`INSERT INTO cities (name, country_iso_code, lat, lng, country_id)
		 VALUES ($1, $2, $3, $4, (SELECT id FROM countries WHERE iso_code = $2))`)
	if err != nil {
		log.Printf("Error preparing statement: %v", err)
		return
	}

	var count int
	for city := range cityChannel {
		_, err = tx.Exec(ctx, "insert_city",
			city.Name, city.CountryISOCode, city.Latitude, city.Longitude)
		if err != nil {
			log.Printf("Error inserting city %s: %v", city.Name, err)
			continue
		}
		count++
	}

	err = tx.Commit(ctx)
	if err != nil {
		log.Printf("Error committing transaction: %v", err)
		return
	}

	fmt.Printf("Worker finished, migrated %d cities\n", count)
}
