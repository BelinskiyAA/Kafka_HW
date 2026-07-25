CREATE TABLE recommendations (
    "user"     TEXT NOT NULL,
    product_id TEXT NOT NULL,
    PRIMARY KEY ("user", product_id)
);

CREATE INDEX idx_recommendations_user ON recommendations ("user");
CREATE INDEX idx_recommendations_product_id ON recommendations (product_id);
