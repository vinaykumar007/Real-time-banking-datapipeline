"""
Generate realistic banking data and load it into PostgreSQL.
"""

import random
from datetime import datetime

from faker import Faker
from sqlalchemy import text

from data_generator.database import db

fake = Faker()

# -----------------------------------------------------
# Configuration
# -----------------------------------------------------

NUM_CUSTOMERS = 1000
MIN_ACCOUNTS_PER_CUSTOMER = 1
MAX_ACCOUNTS_PER_CUSTOMER = 3

NUM_TRANSACTIONS = 100000

ACCOUNT_TYPES = [
    "SAVINGS",
    "CHECKING",
    "BUSINESS",
]

TRANSACTION_TYPES = [
    "DEPOSIT",
    "WITHDRAWAL",
    "TRANSFER",
]

# -----------------------------------------------------
# Customers
# -----------------------------------------------------


def generate_customers(session):
    print("Generating customers...")

    customer_ids = []

    for _ in range(NUM_CUSTOMERS):

        result = session.execute(
            text(
                """
                INSERT INTO banking.customers (
                    first_name,
                    last_name,
                    email,
                    phone_number,
                    date_of_birth
                )
                VALUES (
                    :first_name,
                    :last_name,
                    :email,
                    :phone_number,
                    :date_of_birth
                )
                RETURNING customer_id
                """
            ),
            {
                "first_name": fake.first_name(),
                "last_name": fake.last_name(),
                "email": fake.unique.email(),
                "phone_number": fake.phone_number()[:20],
                "date_of_birth": fake.date_of_birth(
                    minimum_age=18,
                    maximum_age=85
                ),
            },
        )

        customer_ids.append(result.scalar())

    session.commit()

    print(f"Created {len(customer_ids)} customers")

    return customer_ids


# -----------------------------------------------------
# Accounts
# -----------------------------------------------------


def generate_accounts(session, customer_ids):
    print("Generating accounts...")

    account_ids = []

    account_number = 1000000000

    for customer_id in customer_ids:

        num_accounts = random.randint(
            MIN_ACCOUNTS_PER_CUSTOMER,
            MAX_ACCOUNTS_PER_CUSTOMER,
        )

        for _ in range(num_accounts):

            result = session.execute(
                text(
                    """
                    INSERT INTO banking.accounts (
                        account_number,
                        customer_id,
                        account_type,
                        account_status,
                        balance,
                        currency_code
                    )
                    VALUES (
                        :account_number,
                        :customer_id,
                        :account_type,
                        'ACTIVE',
                        0,
                        'USD'
                    )
                    RETURNING account_id
                    """
                ),
                {
                    "account_number": str(account_number),
                    "customer_id": customer_id,
                    "account_type": random.choice(
                        ACCOUNT_TYPES
                    ),
                },
            )

            account_ids.append(result.scalar())

            account_number += 1

    session.commit()

    print(f"Created {len(account_ids)} accounts")

    return account_ids


# -----------------------------------------------------
# Transactions
# -----------------------------------------------------


def generate_transactions(session, account_ids):
    print("Generating transactions...")

    balances = {
        account_id: 0
        for account_id in account_ids
    }

    transaction_counter = 0

    while transaction_counter < NUM_TRANSACTIONS:

        account_id = random.choice(account_ids)

        transaction_type = random.choices(
            population=TRANSACTION_TYPES,
            weights=[50, 35, 15],
            k=1,
        )[0]

        amount = round(
            random.uniform(10, 5000),
            2,
        )

        related_account_id = None

        # -----------------------------------------
        # Deposits
        # -----------------------------------------

        if transaction_type == "DEPOSIT":

            balances[account_id] += amount

        # -----------------------------------------
        # Withdrawals
        # -----------------------------------------

        elif transaction_type == "WITHDRAWAL":

            if balances[account_id] < amount:
                continue

            balances[account_id] -= amount

        # -----------------------------------------
        # Transfers
        # -----------------------------------------

        elif transaction_type == "TRANSFER":

            if balances[account_id] < amount:
                continue

            related_account_id = random.choice(
                account_ids
            )

            if related_account_id == account_id:
                continue

            balances[account_id] -= amount
            balances[related_account_id] += amount

        # -----------------------------------------
        # Insert Transaction
        # -----------------------------------------

        session.execute(
            text(
                """
                INSERT INTO banking.transactions (
                    account_id,
                    transaction_type,
                    amount,
                    related_account_id,
                    status,
                    description,
                    transaction_timestamp
                )
                VALUES (
                    :account_id,
                    :transaction_type,
                    :amount,
                    :related_account_id,
                    'COMPLETED',
                    :description,
                    :transaction_timestamp
                )
                """
            ),
            {
                "account_id": account_id,
                "transaction_type": transaction_type,
                "amount": amount,
                "related_account_id": related_account_id,
                "description": (
                    f"{transaction_type} transaction"
                ),
                "transaction_timestamp": datetime.utcnow(),
            },
        )

        transaction_counter += 1

        if transaction_counter % 5000 == 0:
            session.commit()
            print(
                f"{transaction_counter:,} transactions loaded"
            )

    # -----------------------------------------
    # Update Final Balances
    # -----------------------------------------

    print("Updating account balances...")

    for account_id, balance in balances.items():

        session.execute(
            text(
                """
                UPDATE banking.accounts
                SET balance = :balance,
                    updated_at = CURRENT_TIMESTAMP
                WHERE account_id = :account_id
                """
            ),
            {
                "account_id": account_id,
                "balance": round(balance, 2),
            },
        )

    session.commit()

    print(
        f"Created {transaction_counter:,} transactions"
    )


# -----------------------------------------------------
# Main
# -----------------------------------------------------


def main():

    session = db.get_session()

    try:

        customer_ids = generate_customers(session)

        account_ids = generate_accounts(
            session,
            customer_ids,
        )

        generate_transactions(
            session,
            account_ids,
        )

        print("Data generation complete")

    except Exception as e:

        session.rollback()

        print(f"Error: {e}")

        raise

    finally:

        session.close()


if __name__ == "__main__":
    main()