CREATE DATABASE IF NOT EXISTS dbName;

USE dbName;

CREATE TABLE IF NOT EXISTS bosk_table (
    id varchar(10) PRIMARY KEY,
    state jsonb
)
