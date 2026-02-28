CREATE DATABASE source_db;
GO

USE source_db;
GO

CREATE SCHEMA hr;
GO

CREATE SCHEMA finance;
GO

CREATE TABLE hr.employees (
    employee_id INT IDENTITY(1,1) PRIMARY KEY,
    first_name NVARCHAR(50) NOT NULL,
    last_name NVARCHAR(50) NOT NULL,
    email NVARCHAR(100),
    department NVARCHAR(50),
    hire_date DATE,
    salary DECIMAL(12, 2),
    ssn NVARCHAR(11),
    phone NVARCHAR(20),
    is_active BIT DEFAULT 1,
    created_at DATETIME2 DEFAULT GETDATE(),
    updated_at DATETIME2 DEFAULT GETDATE()
);

CREATE TABLE finance.transactions (
    transaction_id INT IDENTITY(1,1) PRIMARY KEY,
    account_number NVARCHAR(20) NOT NULL,
    transaction_date DATETIME2 NOT NULL,
    amount DECIMAL(15, 2) NOT NULL,
    transaction_type NVARCHAR(20),
    description NVARCHAR(200),
    category NVARCHAR(50),
    merchant NVARCHAR(100),
    status NVARCHAR(20) DEFAULT 'completed',
    created_at DATETIME2 DEFAULT GETDATE()
);

INSERT INTO hr.employees (first_name, last_name, email, department, hire_date, salary, ssn, phone) VALUES
    (N'Sarah', N'Connor', N'sarah.c@example.com', N'Engineering', '2020-03-15', 98000.00, N'111-22-3333', N'612-555-0201'),
    (N'James', N'Kirk', N'james.k@example.com', N'Operations', '2019-07-01', 87000.00, N'222-33-4444', N'612-555-0202'),
    (N'Maria', N'Garcia', N'maria.g@example.com', N'Finance', '2021-01-10', 92000.00, N'333-44-5555', N'212-555-0203'),
    (N'David', N'Chen', N'david.c@example.com', N'Engineering', '2022-05-20', 105000.00, N'444-55-6666', N'612-555-0204'),
    (N'Lisa', N'Park', N'lisa.p@example.com', N'Sales', '2020-11-01', 76000.00, N'555-66-7777', N'312-555-0205');

INSERT INTO finance.transactions (account_number, transaction_date, amount, transaction_type, description, category, merchant, status) VALUES
    (N'ACCT-001', '2026-02-01 09:30:00', 1500.00, N'debit', N'Office supplies purchase', N'operations', N'Office Depot', N'completed'),
    (N'ACCT-001', '2026-02-02 14:15:00', 25000.00, N'credit', N'Client payment received', N'revenue', N'Contoso Ltd', N'completed'),
    (N'ACCT-002', '2026-02-03 10:00:00', 4200.00, N'debit', N'Cloud infrastructure', N'technology', N'Azure', N'completed'),
    (N'ACCT-001', '2026-02-05 16:45:00', 850.00, N'debit', N'Travel expenses', N'travel', N'Delta Airlines', N'pending'),
    (N'ACCT-003', '2026-02-10 11:30:00', 12000.00, N'debit', N'Software licenses', N'technology', N'Microsoft', N'completed'),
    (N'ACCT-002', '2026-02-12 09:00:00', 18500.00, N'credit', N'Service fee payment', N'revenue', N'Northwind', N'completed'),
    (N'ACCT-001', '2026-02-15 13:20:00', 3200.00, N'debit', N'Equipment lease', N'operations', N'TechLease Co', N'completed'),
    (N'ACCT-003', '2026-02-20 15:00:00', 750.00, N'debit', N'Team lunch event', N'employee', N'Catering Plus', N'completed');
GO
