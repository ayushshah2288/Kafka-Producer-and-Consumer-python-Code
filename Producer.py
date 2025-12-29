import pandas as pd
import json
from confluent_kafka import Producer

# --- CONFIGURATION ---
configuration = {
    'bootstrap.servers': '', 
    'security.protocol': '',
    'sasl.mechanisms': '',
    'sasl.username': '',
    'sasl.password': '',
    'client.id': '',
}

# --- CALLBACK FUNCTION ---
def delivery_status(err, msg):
    """
    Reports the delivery status of the message to the console.
    """
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')

# --- DATA PREPARATION ---
print("Reading CSV data...")
csv_file = 'customers_data_first_100_customers.csv'

# Read CSV using Pandas
try:
    df = pd.read_csv(csv_file)
    print(f"Successfully read {len(df)} records from {csv_file}")
except FileNotFoundError:
    print(f"Error: The file '{csv_file}' was not found.")
    exit()

# --- PRODUCER INITIALIZATION ---
producer = Producer(configuration)
topic = 'e_commerce'

# --- SENDING MESSAGES LOOP ---
print("Starting message production...")

# Convert dataframe to a list of dictionaries (easier to loop)
records = df.to_dict(orient='records')

for record in records:
    try:
        # 1. Prepare Key and Value
        # Ensure customer_id exists and convert to string for the key
        key = str(record['customer_id'])
        
        # Serialize the entire record row to a JSON string
        value = json.dumps(record)

        # 2. Produce Message
        producer.produce(
            topic=topic,
            key=key.encode('utf-8'),    # Key must be bytes
            value=value.encode('utf-8'), # Value must be bytes
            callback=delivery_status
        )

        # 3. Handle Events (Trigger the callback), 
        #The 0 in poll(0) means "Check right now and don't wait." This ensures your loop stays fast.
        
        producer.poll(0)

    except Exception as e:
        print(f"Error sending record {record.get('customer_id', 'Unknown')}: {e}")

# --- CLEANUP ---
# Wait for any outstanding messages to be delivered and delivery report callbacks to be triggered.
print("Flushing records...")
producer.flush()

print("All messages sent successfully!")
