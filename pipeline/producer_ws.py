"""
PRODUCER: Binance Multi-Asset WebSocket -> Redpanda / Kafka
Ma wbudowany limit (MAX_MESSAGES / MAX_RUNTIME_SEC), zeby przy pracy
lokalnej/testowej nie sciagac nieograniczonej ilosci danych z Binance.
Ustaw limity w .env albo zmniejsz liste SYMBOLS ponizej.
"""
import json
import time

import websocket
from kafka import KafkaProducer

from db import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC

SYMBOLS = [
    "btcusdt", "ethusdt", "solusdt", "bnbusdt", "adausdt",
    "xrpusdt", "dogeusdt", "avaxusdt", "linkusdt", "dotusdt",
]

MAX_RUNTIME_SEC = 300   # zamknij po 5 minutach
MAX_MESSAGES = 500      # albo po 500 transakcjach, co pierwsze nastapi

message_count = 0
start_time = time.time()

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    key_serializer=lambda k: k.encode("utf-8"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


def on_message(ws, message):
    global message_count

    parsed_message = json.loads(message)
    raw_data = parsed_message.get("data", parsed_message)

    trade_payload = {
        "trade_id": raw_data["t"],
        "symbol": raw_data["s"],
        "price": float(raw_data["p"]),
        "quantity": float(raw_data["q"]),
        "trade_time": raw_data["T"],
        "is_buyer_maker": raw_data["m"],
    }

    producer.send(topic=KAFKA_TOPIC, key=trade_payload["symbol"], value=trade_payload)
    message_count += 1
    print(f"[PRODUCER] ({message_count}) -> {trade_payload['symbol']} | cena: {trade_payload['price']}")

    if message_count >= MAX_MESSAGES or (time.time() - start_time) > MAX_RUNTIME_SEC:
        print("Limit osiagniety, zamykam polaczenie.")
        ws.close()


def on_open(ws):
    print("Polaczono z Binance!")


def on_error(ws, error):
    print(f"Blad WebSocket: {error}")


def on_close(ws, close_status_code, close_msg):
    print("Polaczenie z Binance zostalo zamkniete.")


def main():
    streams_string = "/".join(f"{s}@trade" for s in SYMBOLS)
    socket_url = f"wss://stream.binance.com:9443/stream?streams={streams_string}"

    ws = websocket.WebSocketApp(
        socket_url,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )
    ws.run_forever()


if __name__ == "__main__":
    main()
