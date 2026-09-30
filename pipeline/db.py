
import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = dict(
    host=os.getenv("POSTGRES_HOST", "localhost"),
    port=os.getenv("POSTGRES_PORT", "5432"),
    database=os.getenv("POSTGRES_DB", "crypto_db"),
    user=os.getenv("POSTGRES_USER", "postgres"),
    password=os.getenv("POSTGRES_PASSWORD", "postgres"),
)

KAFKA_BOOTSTRAP_SERVERS = [os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")]
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "binance-trades")


def get_connection():
    return psycopg2.connect(**DB_CONFIG)
