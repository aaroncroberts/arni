// MongoDB Database Initialization Script
// 
// Purpose: Initialize MongoDB database for Arni integration testing
// Database: test_db
// User: test_user
// 
// This script runs automatically when the MongoDB container starts for the first time.
// It creates the test collections and populates them with sample documents.
//
// Usage: This file is mounted in compose.yml and executed by MongoDB's
// docker-entrypoint-initdb.d mechanism.
//
// Note: MongoDB uses JavaScript for initialization scripts, not SQL

// Switch to test_db database
db = db.getSiblingDB('test_db');

// Drop collection if exists (idempotent)
db.users.drop();

// Create users collection and insert sample data
db.users.insertMany([
    {
        name: 'Alice Johnson',
        email: 'alice@example.com',
        active: true,
        created_at: new Date()
    },
    {
        name: 'Bob Smith',
        email: 'bob@example.com',
        active: true,
        created_at: new Date()
    },
    {
        name: 'Charlie Brown',
        email: 'charlie@example.com',
        active: false,
        created_at: new Date()
    },
    {
        name: 'Diana Prince',
        email: 'diana@example.com',
        active: true,
        created_at: new Date()
    },
    {
        name: 'Eve Adams',
        email: 'eve@example.com',
        active: true,
        created_at: new Date()
    }
]);

// Create indexes for performance
db.users.createIndex({ email: 1 }, { unique: true });
db.users.createIndex({ active: 1 });

// Verify data was inserted
print('Inserted ' + db.users.countDocuments() + ' users into test_db.users');

