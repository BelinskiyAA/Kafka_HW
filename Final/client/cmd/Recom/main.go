package main

import (
	"context"
	"errors" // <-- добавлен импорт
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/BelinskiyAA/kafka/Final/Client/internal/repository"
	"github.com/joho/godotenv"
)

const (
	defaultDatabaseURL = "postgres://shop:shop@postgres:5432/shop?sslmode=disable"
)

func main() {
	user := flag.String("user", "", "имя пользователя")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: recommendations -user <name>\n")
		flag.PrintDefaults()
	}
	flag.Parse()

	if *user == "" {
		flag.Usage()
		os.Exit(2)
	}

	if err := Load(); err != nil {
		log.Fatalf("load .env: %v", err)
	}

	repo, err := repository.NewRecommendationRepository(Get("DATABASE_URL", defaultDatabaseURL))
	if err != nil {
		log.Fatalf("db: %v", err)
	}
	defer repo.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	recs, err := repo.ListByUser(ctx, *user)
	if err != nil {
		log.Fatalf("recommendations: %v", err)
	}

	if len(recs) == 0 {
		fmt.Printf("user=%s: рекомендаций нет\n", *user)
		return
	}

	fmt.Printf("user=%s: %d рекомендаций\n", *user, len(recs))
	for _, r := range recs {
		if r.Name != "" {
			fmt.Printf("- %s | %s | %.2f %s | %s\n",
				r.ProductID, r.Name, r.PriceAmount, r.PriceCurrency, r.Brand)
			continue
		}
		fmt.Printf("- %s\n", r.ProductID)
	}
}

func Load(filenames ...string) error {
	err := godotenv.Load(filenames...)
	if err == nil || errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

func Get(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}