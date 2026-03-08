import json
import time
import logging
import websocket
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO)

# Use the localhost port defined in docker-compose
KAFKA_BROKER = '127.0.0.1:29092'
KAFKA_TOPIC = 'bronze_market_trades'

def get_producer():
    while True:
        try:
            return KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all' # Guarantee data delivery
            )
        except Exception:
            logging.info("Waiting for Kafka to be ready...")
            time.sleep(5)

producer = get_producer()

def on_message(ws, message):
    data = json.loads(message)
    # Filter for trade events only
    if data.get('e') == 'trade':
        producer.send(KAFKA_TOPIC, data)
        print(f"Sent: {data['s']} at {data['p']}", end='\r')

def on_open(ws):
    logging.info("Stream Started.")

if __name__ == "__main__":
    ws = websocket.WebSocketApp(
        "wss://stream.binance.com:9443/ws/btcusdt@trade",
        on_open=on_open,
        on_message=on_message
    )
    ws.run_forever()