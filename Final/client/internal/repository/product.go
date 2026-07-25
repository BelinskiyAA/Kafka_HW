package repository

import (
	"context"
	"database/sql"
	"fmt"
)

type Product struct {
	ProductID     string
	Name          string
	PriceAmount   float64
	PriceCurrency string
	Brand         string
}

type ProductRepository struct {
	db *sql.DB
}

func NewProductRepository(databaseURL string) (*ProductRepository, error) {
	db, err := openDB(databaseURL)
	if err != nil {
		return nil, err
	}
	return &ProductRepository{db: db}, nil
}

func (r *ProductRepository) Close() error {
	return r.db.Close()
}

func (r *ProductRepository) SearchByName(ctx context.Context, query string) ([]Product, error) {
	const q = `
SELECT product_id, name, price_amount, price_currency, brand
FROM products
WHERE search_vector @@ plainto_tsquery('russian', $1)
ORDER BY ts_rank(search_vector, plainto_tsquery('russian', $1)) DESC,
         name ASC
LIMIT 50`

	rows, err := r.db.QueryContext(ctx, q, query)
	if err != nil {
		return nil, fmt.Errorf("search products: %w", err)
	}
	defer rows.Close()

	var out []Product
	for rows.Next() {
		var p Product
		if err := rows.Scan(
			&p.ProductID, &p.Name,
			&p.PriceAmount, &p.PriceCurrency, &p.Brand,
		); err != nil {
			return nil, fmt.Errorf("scan product: %w", err)
		}
		out = append(out, p)
	}
	return out, rows.Err()
}
