"""
Database connection utilities.
"""

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

import os

# Load .env variables
load_dotenv()


class Database:
    def __init__(self):
        self.host = os.getenv("POSTGRES_HOST", "localhost")
        self.port = os.getenv("POSTGRES_PORT", "5432")
        self.database = os.getenv("POSTGRES_DB")
        self.user = os.getenv("POSTGRES_USER")
        self.password = os.getenv("POSTGRES_PASSWORD")

        self.connection_string = (
            f"postgresql+psycopg://"
            f"{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
        )

        self.engine = create_engine(
            self.connection_string,
            pool_pre_ping=True,
            echo=False
        )

        self.SessionLocal = sessionmaker(
            bind=self.engine,
            autoflush=False,
            autocommit=False
        )

    def get_engine(self):
        return self.engine

    def get_session(self):
        return self.SessionLocal()

    def test_connection(self):
        try:
            with self.engine.connect() as conn:
                print("✅ PostgreSQL connection successful")
                return True

        except Exception as e:
            print(f"❌ Connection failed: {e}")
            return False


# Singleton instance
db = Database()