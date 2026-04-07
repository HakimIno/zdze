#!/bin/bash

# simulate_traffic.sh - Continuously insert and update data in Postgres to show CDC in action

# Configuration
DB_URL="postgresql://postgres:password@localhost:5432/zdze_demo"
INTERVAL=2

echo "🚀 Starting zdze Traffic Simulator..."
echo "📊 Database: $DB_URL"
echo "⏱️ Interval: $INTERVAL seconds"

while true; do
    # 1. Random user activity
    echo "👤 Adding new user..."
    psql "$DB_URL" -c "INSERT INTO users (name, email, password) VALUES ('Random User', 'user_$(date +%s)@demo.com', 'secret_$(date +%s)');"
    
    # 2. Random order activity
    echo "🛒 Placing new order..."
    USER_ID=$(( ( RANDOM % 3 ) + 1 ))
    AMOUNT=$(( ( RANDOM % 1000 ) + 100 ))
    psql "$DB_URL" -c "INSERT INTO orders (user_id, product, amount) VALUES ($USER_ID, 'Random Product 7', $AMOUNT);"
    
    # 3. Random order update (DDL Drift test)
    echo "📦 Updating order status..."
    psql "$DB_URL" -c "UPDATE orders SET status = 'shipped' WHERE status = 'pending' LIMIT 1;"
    
    echo "-----------------------------------"
    sleep $INTERVAL
done
