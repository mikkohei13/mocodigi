-- Database schema for localities table

CREATE TABLE IF NOT EXISTS localities_finland (
    id INTEGER PRIMARY KEY,
    feature_class CHAR(1),
    name VARCHAR(256) NOT NULL,
    source VARCHAR(8) NOT NULL,
    updated TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

