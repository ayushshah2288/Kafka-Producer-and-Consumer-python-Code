import json
from confluent_kafka import Consumer, KafkaError

# --- CONFIGURATION ---
conf = {
    'bootstrap.servers': '', 
    'security.protocol': '',
    'sasl.mechanisms': '',
    'sasl.username': '',
    'sasl.password': '',
    'group.id': '',
    'auto.offset.reset': ''
}

# --- INITIALIZE CONSUMER ---
consumer = Consumer(conf)
topic = 'e_commerce'
consumer.subscribe([topic])

# --- FIXED PROCESSING FUNCTION ---
def process_message(msg):
    """
    Decodes and processes a single message.
    Now includes ERROR HANDLING for bad JSON.
    """
    # 1. Decode Key
    key = msg.key().decode('utf-8') if msg.key() else None
    
    # 2. Decode Value with Safety Check
    value_str = msg.value().decode('utf-8') if msg.value() else None
    
    try:
        # Try to parse the JSON
        value = json.loads(value_str) if value_str else None
        print(f"✅ Received: Key={key} | Value={value}")
        
    except json.JSONDecodeError:
        # If parsing fails, print a warning but DO NOT CRASH
        print(f"⚠️  SKIPPING INVALID JSON: {value_str}")

# --- MAIN LOOP ---
try:
    print(f"Listening for messages on topic '{topic}'...")
    
    while True:
        msg = consumer.poll(timeout=1.0)

        if msg is None:
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(f"Error: {msg.error()}")
        else:
            process_message(msg)

except KeyboardInterrupt:
    print("\nInterrupted by user. Shutting down...")

finally:
    consumer.close() 
    print("Consumer closed successfully.")