-- SQL Server Database Initialization Script
-- 
-- Purpose: Initialize SQL Server database for Arni integration testing
-- Database: test_db
-- User: sa
-- 
-- This script runs automatically when the SQL Server container starts for the first time.
-- It creates the test database, schema, tables, and populates them with sample data.
--
-- Usage: This file is mounted in compose.yml and executed by SQL Server's
-- docker-entrypoint-initdb.d mechanism.
--
-- Note: SQL Server syntax differs from PostgreSQL/MySQL (e.g., IDENTITY for auto-increment)

-- Create test database if it doesn't exist
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'test_db')
BEGIN
    CREATE DATABASE test_db;
END
GO

USE test_db;
GO

-- Create users table (idempotent - drop if exists)
IF OBJECT_ID('users', 'U') IS NOT NULL
    DROP TABLE users;
GO

CREATE TABLE users (
    id INT IDENTITY(1,1) PRIMARY KEY,
    name NVARCHAR(100) NOT NULL,
    email NVARCHAR(255) UNIQUE NOT NULL,
    active BIT DEFAULT 1,
    created_at DATETIME DEFAULT GETDATE()
);
GO

-- Insert sample data
INSERT INTO users (name, email, active, created_at) VALUES
    ('Alice Johnson', 'alice@example.com', 1, GETDATE()),
    ('Bob Smith', 'bob@example.com', 1, GETDATE()),
    ('Charlie Brown', 'charlie@example.com', 0, GETDATE()),
    ('Diana Prince', 'diana@example.com', 1, GETDATE()),
    ('Eve Adams', 'eve@example.com', 1, GETDATE());
GO

-- Create indexes for performance
CREATE INDEX idx_users_email ON users(email);
CREATE INDEX idx_users_active ON users(active);
GO

