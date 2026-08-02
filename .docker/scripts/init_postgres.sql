CREATE TABLE IF NOT EXISTS users (
    user_id SERIAL PRIMARY KEY,
    full_name VARCHAR(100),
    email VARCHAR(100),
    signup_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS orders (
    order_id SERIAL PRIMARY KEY,
    user_id INT REFERENCES users(user_id),
    order_status VARCHAR(20),
    order_amount DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert dummy data for initial testing
INSERT INTO users (full_name, email) VALUES 
('John Doe', 'john.doe@example.com'),
('Jane Smith', 'jane.smith@example.com'),
('Alice Johnson', 'alice.j@example.com');

INSERT INTO orders (user_id, order_status, order_amount) VALUES 
(1, 'COMPLETED', 149.99),
(1, 'COMPLETED', 25.50),
(2, 'PENDING', 89.00),
(3, 'CANCELLED', 12.00);