-- Database schema for localities table

CREATE TABLE IF NOT EXISTS localities (
    id SERIAL PRIMARY KEY,
    data VARCHAR(256) NOT NULL,
    source VARCHAR(32) NOT NULL,
    updated TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

