-- PostgreSQL Database Initialization Script
-- 
-- Purpose: Initialize PostgreSQL database for Arni integration testing
-- Database: test_db
-- User: test_user
-- 
-- This script runs automatically when the PostgreSQL container starts for the first time.
-- It creates the test schema, tables, and populates them with sample data.
--
-- Usage: This file is mounted in compose.yml and executed by PostgreSQL's
-- docker-entrypoint-initdb.d mechanism.

-- Create users table (idempotent - drop if exists)
DROP TABLE IF EXISTS users CASCADE;

CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert sample data
INSERT INTO users (name, email, active, created_at) VALUES
    ('Alice Johnson', 'alice@example.com', TRUE, CURRENT_TIMESTAMP),
    ('Bob Smith', 'bob@example.com', TRUE, CURRENT_TIMESTAMP),
    ('Charlie Brown', 'charlie@example.com', FALSE, CURRENT_TIMESTAMP),
    ('Diana Prince', 'diana@example.com', TRUE, CURRENT_TIMESTAMP),
    ('Eve Adams', 'eve@example.com', TRUE, CURRENT_TIMESTAMP);

-- Create indexes for performance
CREATE INDEX idx_users_email ON users(email);
CREATE INDEX idx_users_active ON users(active);

