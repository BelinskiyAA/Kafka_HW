package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/BelinskiyAA/kafka/Final/Client/internal/analytics"
	"github.com/BelinskiyAA/kafka/Final/Client/internal/repository"
	"github.com/joho/godotenv"
)

const (
	defaultDatabaseURL = "postgres://shop:shop@postgres:5432/shop?sslmode=disable"
)

func getEnv(key, fallback string) string {
	if val, exists := os.LookupEnv(key); exists {
		return val
	}
	return fallback
}

func main() {
	user := flag.String("user", "", "имя пользователя")
	product := flag.String("product", "", "название товара для поиска")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: search -user <name> -product <query>\n")
		flag.PrintDefaults()
	}
	flag.Parse()

	if *user == "" || *product == "" {
		flag.Usage()
		os.Exit(2)
	}

	// Загружаем .env
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found, using defaults or system env")
	}

	// Читаем переменные
	databaseURL := getEnv("DATABASE_URL", defaultDatabaseURL)

	repo, err := repository.NewProductRepository(databaseURL)
	if err != nil {
		log.Fatalf("db: %v", err)
	}
	defer repo.Close()

	analyticsSvc, err := analytics.NewService()
	if err != nil {
		log.Fatalf("analytics: %v", err)
	}
	defer analyticsSvc.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	results, err := repo.SearchByName(ctx, *product)
	if err != nil {
		log.Fatalf("search: %v", err)
	}

	ids := make([]string, 0, len(results))
	for _, p := range results {
		ids = append(ids, p.ProductID)
	}

	if err := analyticsSvc.PublishSearch(ctx, *user, *product, ids); err != nil {
		log.Fatalf("analytics event: %v", err)
	}

	if len(results) == 0 {
		fmt.Printf("user=%s query=%q: ничего не найдено\n", *user, *product)
		return
	}

	fmt.Printf("user=%s query=%q: найдено %d\n", *user, *product, len(results))
	for _, p := range results {
		fmt.Printf("- %s | %s | %.2f %s | %s\n",
			p.ProductID, p.Name, p.PriceAmount, p.PriceCurrency, p.Brand)
	}
}