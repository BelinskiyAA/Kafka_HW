-- 1. Создание таблицы
CREATE TABLE products (
    product_id       TEXT PRIMARY KEY,
    name             TEXT NOT NULL,
    description      TEXT NOT NULL DEFAULT '',
    price_amount     NUMERIC(12, 2) NOT NULL,
    price_currency   TEXT NOT NULL DEFAULT 'RUB',
    category         TEXT NOT NULL DEFAULT '',
    brand            TEXT NOT NULL DEFAULT '',
    stock_available  INTEGER NOT NULL DEFAULT 0,
    stock_reserved   INTEGER NOT NULL DEFAULT 0,
    sku              TEXT NOT NULL DEFAULT '',
    tags             JSONB NOT NULL DEFAULT '[]'::jsonb,
    images           JSONB NOT NULL DEFAULT '[]'::jsonb,
    specifications   JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at       TIMESTAMPTZ NOT NULL,
    updated_at       TIMESTAMPTZ NOT NULL,
    "index"          TEXT NOT NULL DEFAULT 'products',
    store_id         TEXT NOT NULL DEFAULT ''
);

CREATE INDEX idx_products_store_id ON products (store_id);
CREATE INDEX idx_products_category ON products (category);
CREATE INDEX idx_products_sku ON products (sku);

-- 2. Добавление столбца для полнотекстового поиска
ALTER TABLE products
    ADD COLUMN search_vector tsvector
        GENERATED ALWAYS AS (
            to_tsvector(
                'russian',
                coalesce(name, '') || ' ' || coalesce(description, '')
            )
        ) STORED;

CREATE INDEX idx_products_search_vector ON products USING GIN (search_vector);