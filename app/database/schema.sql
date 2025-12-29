-- Database schema for localities table

-- Enable pg_trgm extension for fuzzy text search
CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE TABLE IF NOT EXISTS localities_finland (
    id INTEGER PRIMARY KEY,
    feature_class CHAR(1),
    name VARCHAR(256) NOT NULL,
    source VARCHAR(8) NOT NULL,
    updated TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Create GIN index on name for fast fuzzy search with pg_trgm
CREATE INDEX IF NOT EXISTS idx_localities_finland_name_trgm ON localities_finland USING GIN (name gin_trgm_ops);

