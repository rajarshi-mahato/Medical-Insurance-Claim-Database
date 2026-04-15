import json
import time

# 1. THE MESSAGE (The Medical Claim)
claim_data = {
    "claim_id": "KAFKA-999",
    "hospital": "City Medical",
    "amount": 1250.00,
    "diagnosis": "MRI Scan"
}

print("--- Kafka Simulation Started ---")

# 2. PRODUCER SIDE (Sending Data)
def producer_simulate(data):
    print(f"\n[PRODUCER] Sending claim {data['claim_id']} to topic 'medical_claims'...")
    # In Kafka, data must be sent as bytes. We convert JSON to a string, then to bytes.
    message_bytes = json.dumps(data).encode('utf-8')
    print(f"[PRODUCER] Data converted to bytes: {message_bytes[:30]}...")
    return message_bytes

# 3. KAFKA TOPIC (The Buffer)
# Imagine the data sitting on a Kafka server here...
kafka_buffer = producer_simulate(claim_data)
time.sleep(1) # Simulating network travel

# 4. CONSUMER SIDE (Receiving Data)
def consumer_simulate(buffer):
    print("\n[CONSUMER] New message detected in topic 'medical_claims'!")
    # The consumer receives raw bytes and must "decode" them back into JSON
    decoded_data = json.loads(buffer.decode('utf-8'))
    print(f"[CONSUMER] Claim Received: {decoded_data['claim_id']}")
    print(f"[CONSUMER] Processing amount: ${decoded_data['amount']}")

consumer_simulate(kafka_buffer)
