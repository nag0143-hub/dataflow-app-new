CREATE SCHEMA IF NOT EXISTS hr;
CREATE SCHEMA IF NOT EXISTS sales;

CREATE TABLE hr.employees (
    employee_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100),
    department VARCHAR(50),
    hire_date DATE,
    salary NUMERIC(12, 2),
    ssn VARCHAR(11),
    phone VARCHAR(20),
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE hr.departments (
    department_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    manager_id INTEGER,
    budget NUMERIC(15, 2),
    location VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE sales.orders (
    order_id SERIAL PRIMARY KEY,
    customer_name VARCHAR(100) NOT NULL,
    order_date DATE NOT NULL,
    total_amount NUMERIC(12, 2),
    status VARCHAR(20) DEFAULT 'pending',
    region VARCHAR(50),
    sales_rep_id INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE sales.products (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(200) NOT NULL,
    category VARCHAR(50),
    unit_price NUMERIC(10, 2),
    stock_quantity INTEGER DEFAULT 0,
    supplier VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE sales.order_items (
    item_id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES sales.orders(order_id),
    product_id INTEGER REFERENCES sales.products(product_id),
    quantity INTEGER NOT NULL,
    unit_price NUMERIC(10, 2),
    discount_pct NUMERIC(5, 2) DEFAULT 0
);

INSERT INTO hr.departments (name, manager_id, budget, location) VALUES
    ('Engineering', 1, 2500000.00, 'Minneapolis'),
    ('Human Resources', 5, 800000.00, 'Minneapolis'),
    ('Sales', 10, 1500000.00, 'Chicago'),
    ('Finance', 15, 1200000.00, 'New York'),
    ('Operations', 20, 950000.00, 'Dallas');

INSERT INTO hr.employees (first_name, last_name, email, department, hire_date, salary, ssn, phone) VALUES
    ('John', 'Smith', 'john.smith@example.com', 'Engineering', '2020-01-15', 95000.00, '123-45-6789', '612-555-0101'),
    ('Jane', 'Doe', 'jane.doe@example.com', 'Engineering', '2019-06-01', 105000.00, '234-56-7890', '612-555-0102'),
    ('Bob', 'Johnson', 'bob.j@example.com', 'Sales', '2021-03-20', 72000.00, '345-67-8901', '312-555-0103'),
    ('Alice', 'Williams', 'alice.w@example.com', 'HR', '2018-11-10', 68000.00, '456-78-9012', '612-555-0104'),
    ('Charlie', 'Brown', 'charlie.b@example.com', 'Finance', '2022-07-05', 88000.00, '567-89-0123', '212-555-0105'),
    ('Diana', 'Prince', 'diana.p@example.com', 'Engineering', '2020-09-14', 115000.00, '678-90-1234', '612-555-0106'),
    ('Eric', 'Taylor', 'eric.t@example.com', 'Operations', '2021-01-22', 62000.00, '789-01-2345', '214-555-0107'),
    ('Fiona', 'Garcia', 'fiona.g@example.com', 'Sales', '2019-04-30', 78000.00, '890-12-3456', '312-555-0108'),
    ('George', 'Martinez', 'george.m@example.com', 'Engineering', '2023-02-14', 92000.00, '901-23-4567', '612-555-0109'),
    ('Hannah', 'Lee', 'hannah.l@example.com', 'Finance', '2020-08-18', 85000.00, '012-34-5678', '212-555-0110');

INSERT INTO sales.products (product_name, category, unit_price, stock_quantity, supplier) VALUES
    ('Widget A', 'Hardware', 29.99, 500, 'Acme Corp'),
    ('Widget B', 'Hardware', 49.99, 300, 'Acme Corp'),
    ('Software License Pro', 'Software', 199.99, 9999, 'TechSoft'),
    ('Data Cable 6ft', 'Accessories', 9.99, 2000, 'CableCo'),
    ('Monitor 27in', 'Hardware', 349.99, 150, 'DisplayTech'),
    ('Keyboard Wireless', 'Accessories', 59.99, 800, 'InputDev'),
    ('Analytics Platform', 'Software', 499.99, 9999, 'DataCo'),
    ('USB Hub 7-port', 'Accessories', 24.99, 600, 'CableCo');

INSERT INTO sales.orders (customer_name, order_date, total_amount, status, region, sales_rep_id) VALUES
    ('Contoso Ltd', '2026-01-15', 1249.95, 'completed', 'Midwest', 3),
    ('Northwind Traders', '2026-01-20', 599.98, 'completed', 'Northeast', 8),
    ('Adventure Works', '2026-02-01', 2499.90, 'shipped', 'West', 3),
    ('Fabrikam Inc', '2026-02-10', 349.99, 'pending', 'South', 8),
    ('Tailspin Toys', '2026-02-15', 789.93, 'completed', 'Midwest', 3),
    ('WingTip Toys', '2026-02-20', 1049.97, 'processing', 'Northeast', 8),
    ('Datum Corp', '2026-02-25', 499.99, 'pending', 'West', 3);

INSERT INTO sales.order_items (order_id, product_id, quantity, unit_price, discount_pct) VALUES
    (1, 1, 10, 29.99, 0), (1, 2, 5, 49.99, 5.0), (1, 4, 20, 9.99, 0),
    (2, 3, 3, 199.99, 0),
    (3, 5, 5, 349.99, 10.0), (3, 7, 2, 499.99, 0),
    (4, 5, 1, 349.99, 0),
    (5, 1, 15, 29.99, 5.0), (5, 6, 5, 59.99, 0),
    (6, 2, 10, 49.99, 0), (6, 7, 1, 499.99, 10.0),
    (7, 7, 1, 499.99, 0);
