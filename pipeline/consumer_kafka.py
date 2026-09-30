"""
CONSUMER: Redpanda / Kafka -> zapis surowych transakcji do PostgreSQL.
Ma wbudowany limit (MAX_MESSAGES), zeby przy pracy lokalnej/testowej
nie dzialac w nieskonczonosc.
"""
import json
from datetime import datetime

from kafka import KafkaConsumer

from db import get_connection, KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC

MAX_MESSAGES = 500

INSERT_QUERY = """
INSERT INTO crypto_trades (trade_id, symbol, price, quantity, trade_time, is_buyer_maker)
VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT (trade_id) DO NOTHING;
"""


def main():
    conn = get_connection()
    cursor = conn.cursor()

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="crypto-multi-asset-group",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )

    print("Konsument uruchomiony, czekam na transakcje...")
    message_count = 0

    for message in consumer:
        data = message.value
        trade_dt = datetime.fromtimestamp(data["trade_time"] / 1000.0)

        cursor.execute(INSERT_QUERY, (
            data["trade_id"], data["symbol"], data["price"],
            data["quantity"], trade_dt, data["is_buyer_maker"],
        ))
        conn.commit()
        message_count += 1
        print(f"[CONSUMER] ({message_count}) zapisano -> {data['symbol']} | cena: {data['price']}")

        if message_count >= MAX_MESSAGES:
            print("Limit osiagniety, konczę.")
            break

    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()
