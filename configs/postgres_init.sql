-- Initialize PostgreSQL for CDC
CREATE TABLE IF NOT EXISTS customers (
    customer_id VARCHAR(50) PRIMARY KEY,
    email VARCHAR(255) NOT NULL,
    name VARCHAR(255) NOT NULL,
    region VARCHAR(20) DEFAULT 'us-east',
    tier VARCHAR(20) DEFAULT 'standard',
    lifetime_orders INTEGER DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS orders (
    order_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50) REFERENCES customers(customer_id),
    total_amount DECIMAL(12, 2) NOT NULL,
    currency VARCHAR(3) DEFAULT 'USD',
    status VARCHAR(20) DEFAULT 'placed',
    payment_method VARCHAR(20) NOT NULL,
    shipping_address TEXT,
    placed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS payments (
    payment_id VARCHAR(50) PRIMARY KEY,
    order_id VARCHAR(50) REFERENCES orders(order_id),
    amount DECIMAL(12, 2) NOT NULL,
    status VARCHAR(20) NOT NULL,
    transaction_ref VARCHAR(100),
    processed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE PUBLICATION orders_publication FOR TABLE customers, orders, payments;

CREATE INDEX idx_orders_customer ON orders(customer_id);
CREATE INDEX idx_orders_status ON orders(status);
CREATE INDEX idx_payments_order ON payments(order_id);

INSERT INTO customers (customer_id, email, name, region, tier, lifetime_orders) VALUES
    ('cust-001', 'alice@example.com', 'Alice Johnson', 'us-east', 'premium', 156),
    ('cust-002', 'bob@example.com', 'Bob Smith', 'us-west', 'standard', 23),
    ('cust-003', 'carol@example.com', 'Carol Williams', 'eu-west', 'enterprise', 892)
ON CONFLICT DO NOTHING;
