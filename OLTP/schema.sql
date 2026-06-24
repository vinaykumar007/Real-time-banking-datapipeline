-- =====================================================
-- Banking OLTP Schema
-- =====================================================

CREATE SCHEMA IF NOT EXISTS banking;

SET search_path TO banking;

-- =====================================================
-- Customers
-- =====================================================

CREATE TABLE IF NOT EXISTS customers (
customer_id BIGSERIAL PRIMARY KEY,

```
first_name VARCHAR(100) NOT NULL,
last_name VARCHAR(100) NOT NULL,

email VARCHAR(255) UNIQUE NOT NULL,
phone_number VARCHAR(20),

date_of_birth DATE,

created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
```

);

-- =====================================================
-- Accounts
-- =====================================================

CREATE TABLE IF NOT EXISTS accounts (
account_id BIGSERIAL PRIMARY KEY,

```
account_number VARCHAR(20) UNIQUE NOT NULL,

customer_id BIGINT NOT NULL,

account_type VARCHAR(20) NOT NULL
    CHECK (
        account_type IN (
            'SAVINGS',
            'CHECKING',
            'BUSINESS'
        )
    ),

account_status VARCHAR(20) NOT NULL
    DEFAULT 'ACTIVE'
    CHECK (
        account_status IN (
            'ACTIVE',
            'FROZEN',
            'CLOSED'
        )
    ),

balance NUMERIC(18,2) NOT NULL
    DEFAULT 0
    CHECK (balance >= 0),

currency_code CHAR(3) NOT NULL
    DEFAULT 'USD',

created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,

CONSTRAINT fk_accounts_customer
    FOREIGN KEY (customer_id)
    REFERENCES customers(customer_id)
```

);

-- =====================================================
-- Transactions
-- =====================================================

CREATE TABLE IF NOT EXISTS transactions (
transaction_id BIGSERIAL PRIMARY KEY,

```
account_id BIGINT NOT NULL,

transaction_type VARCHAR(20) NOT NULL
    CHECK (
        transaction_type IN (
            'DEPOSIT',
            'WITHDRAWAL',
            'TRANSFER'
        )
    ),

amount NUMERIC(18,2) NOT NULL
    CHECK (amount > 0),

related_account_id BIGINT,

status VARCHAR(20) NOT NULL
    DEFAULT 'COMPLETED'
    CHECK (
        status IN (
            'PENDING',
            'COMPLETED',
            'FAILED'
        )
    ),

description TEXT,

transaction_timestamp TIMESTAMPTZ
    DEFAULT CURRENT_TIMESTAMP,

created_at TIMESTAMPTZ
    DEFAULT CURRENT_TIMESTAMP,

CONSTRAINT fk_transactions_account
    FOREIGN KEY (account_id)
    REFERENCES accounts(account_id),

CONSTRAINT fk_transactions_related_account
    FOREIGN KEY (related_account_id)
    REFERENCES accounts(account_id)
```

);

-- =====================================================
-- Indexes
-- =====================================================

CREATE INDEX IF NOT EXISTS idx_accounts_customer
ON accounts(customer_id);

CREATE INDEX IF NOT EXISTS idx_transactions_account
ON transactions(account_id);

CREATE INDEX IF NOT EXISTS idx_transactions_timestamp
ON transactions(transaction_timestamp);

CREATE INDEX IF NOT EXISTS idx_transactions_type
ON transactions(transaction_type);

-- =====================================================
-- Useful Queries
-- =====================================================

-- List tables
-- SELECT * FROM information_schema.tables
-- WHERE table_schema = 'banking';

-- Verify schema
-- SELECT table_name
-- FROM information_schema.tables
-- WHERE table_schema = 'banking';
