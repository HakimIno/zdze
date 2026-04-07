-- setup_demo.sql

-- 1. Create Publication for logical replication
CREATE PUBLICATION zdze_pub FOR ALL TABLES;

-- 2. Create Sample Tables
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(255),
    password VARCHAR(255), -- Sensitive info for masking test
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    user_id INT REFERENCES users(id),
    product VARCHAR(100),
    amount DECIMAL(10, 2),
    status VARCHAR(50) DEFAULT 'pending',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. Insert Initial Data (To test Snapshot Mode)
INSERT INTO users (name, email, password) VALUES 
('Alice Johnson', 'alice@example.com', 'scrypt_hash_1'),
('Bob Smith', 'bob@work.com', 'bcrypt_hash_2'),
('Charlie Brown', 'charlie@gmail.com', 'argon2_hash_3');

INSERT INTO orders (user_id, product, amount) VALUES 
(1, 'Logitech Mouse', 49.99),
(1, 'Mechanical Keyboard', 120.00),
(2, 'ThinkPad X1', 1500.00);

-- 4. Create Replication Slot
SELECT * FROM pg_create_logical_replication_slot('zdze_demo_slot', 'pgoutput');
