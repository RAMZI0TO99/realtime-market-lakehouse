import json
import time
import logging
import websocket
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

KAFKA_BROKER = 'localhost:29092'
KAFKA_TOPIC = 'bronze_market_trades'

SYMBOLS = ["btcusdt", "ethusdt", "solusdt"]

def get_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all'
            )
            return producer
        except Exception as e:
            logging.warning(f"Kafka not ready, retrying... ({e})")
            time.sleep(5)

producer = get_producer()

def on_message(ws, message):
    try:
        msg = json.loads(message)
        # Handle multiplexed stream wrapper
        trade_data = msg.get('data', msg) 
        
        if trade_data.get('e') == 'trade':
            producer.send(KAFKA_TOPIC, trade_data)
            symbol = trade_data['s']
            price = float(trade_data['p'])
            print(f"📡 Ingesting: {symbol} @ ${price:,.2f}          ", end='\r')
    except Exception as e:
        logging.error(f"Error processing message: {e}")

def on_open(ws):
    logging.info(f"Connected to Binance. Streaming: {', '.join(SYMBOLS)}")

if __name__ == "__main__":
    streams = "/".join([f"{s}@trade" for s in SYMBOLS])
    BINANCE_WS_URL = f"wss://stream.binance.com:9443/stream?streams={streams}"
    
    ws = websocket.WebSocketApp(BINANCE_WS_URL, on_open=on_open, on_message=on_message)
    
    try:
        # Run the WebSocket
        ws.run_forever()
    except KeyboardInterrupt:
        logging.info("Keyboard interrupt received. Stopping producer...")
    finally:
        # Force the producer to close with a fast 3-second timeout
        # This prevents the atexit KafkaTimeoutError traceback
        if producer:
            logging.info("Flushing lingering messages and closing Kafka connection...")
            producer.close(timeout=3)
            logging.info("Producer shutdown complete.")