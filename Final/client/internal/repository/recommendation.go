package repository

import (
	"context"
	"database/sql"
	"fmt"
)

type Recommendation struct {
	ProductID     string
	Name          string
	PriceAmount   float64
	PriceCurrency string
	Brand         string
}

type RecommendationRepository struct {
	db *sql.DB
}

func NewRecommendationRepository(databaseURL string) (*RecommendationRepository, error) {
	db, err := openDB(databaseURL)
	if err != nil {
		return nil, err
	}
	return &RecommendationRepository{db: db}, nil
}

func (r *RecommendationRepository) Close() error {
	return r.db.Close()
}

func (r *RecommendationRepository) ListByUser(ctx context.Context, user string) ([]Recommendation, error) {
	const q = `
SELECT r.product_id,
       COALESCE(p.name, ''),
       COALESCE(p.price_amount, 0),
       COALESCE(p.price_currency, ''),
       COALESCE(p.brand, '')
FROM recommendations r
LEFT JOIN products p ON p.product_id = r.product_id
WHERE r."user" = $1
ORDER BY r.product_id`

	rows, err := r.db.QueryContext(ctx, q, user)
	if err != nil {
		return nil, fmt.Errorf("list recommendations: %w", err)
	}
	defer rows.Close()

	var out []Recommendation
	for rows.Next() {
		var rec Recommendation
		if err := rows.Scan(
			&rec.ProductID,
			&rec.Name, &rec.PriceAmount, &rec.PriceCurrency, &rec.Brand,
		); err != nil {
			return nil, fmt.Errorf("scan recommendation: %w", err)
		}
		out = append(out, rec)
	}
	return out, rows.Err()
}
