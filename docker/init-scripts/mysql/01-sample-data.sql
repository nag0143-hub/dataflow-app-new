CREATE TABLE IF NOT EXISTS customers (
    customer_id INT AUTO_INCREMENT PRIMARY KEY,
    company_name VARCHAR(100) NOT NULL,
    contact_name VARCHAR(100),
    contact_email VARCHAR(100),
    phone VARCHAR(20),
    address VARCHAR(200),
    city VARCHAR(50),
    state VARCHAR(50),
    country VARCHAR(50) DEFAULT 'USA',
    credit_limit DECIMAL(12, 2),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS invoices (
    invoice_id INT AUTO_INCREMENT PRIMARY KEY,
    customer_id INT,
    invoice_date DATE NOT NULL,
    due_date DATE,
    total_amount DECIMAL(12, 2),
    paid_amount DECIMAL(12, 2) DEFAULT 0,
    status ENUM('draft', 'sent', 'paid', 'overdue', 'cancelled') DEFAULT 'draft',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

INSERT INTO customers (company_name, contact_name, contact_email, phone, city, state, credit_limit) VALUES
    ('Contoso Ltd', 'Alice Manager', 'alice@contoso.com', '555-0101', 'Minneapolis', 'MN', 50000.00),
    ('Northwind Traders', 'Bob Trader', 'bob@northwind.com', '555-0102', 'Chicago', 'IL', 75000.00),
    ('Adventure Works', 'Carol Builder', 'carol@adventure.com', '555-0103', 'Seattle', 'WA', 100000.00),
    ('Fabrikam Inc', 'Dave Maker', 'dave@fabrikam.com', '555-0104', 'Dallas', 'TX', 60000.00),
    ('Tailspin Toys', 'Eve Spinner', 'eve@tailspin.com', '555-0105', 'Denver', 'CO', 40000.00);

INSERT INTO invoices (customer_id, invoice_date, due_date, total_amount, paid_amount, status) VALUES
    (1, '2026-01-10', '2026-02-10', 12500.00, 12500.00, 'paid'),
    (2, '2026-01-15', '2026-02-15', 8750.00, 8750.00, 'paid'),
    (1, '2026-02-01', '2026-03-01', 15000.00, 0, 'sent'),
    (3, '2026-02-05', '2026-03-05', 22000.00, 10000.00, 'sent'),
    (4, '2026-02-10', '2026-03-10', 5500.00, 0, 'draft'),
    (2, '2026-02-20', '2026-03-20', 9200.00, 0, 'sent'),
    (5, '2026-02-25', '2026-03-25', 3800.00, 0, 'draft');
