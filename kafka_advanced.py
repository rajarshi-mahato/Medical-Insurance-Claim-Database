import hashlib

print("--- Kafka Advanced: Partitioning & Offsets ---")

# 1. Setup our "Partitions"
# In a real cluster, these would be on different physical servers.
topic_partitions = {0: [], 1: [], 2: []}
consumer_offsets = {0: 0, 1: 0, 2: 0} # The "bookmarks"

claims = [
    {"id": "CLM-001", "hospital": "City_A"},
    {"id": "CLM-002", "hospital": "City_B"},
    {"id": "CLM-003", "hospital": "City_A"},
    {"id": "CLM-004", "hospital": "City_C"}
]

# 2. PRODUCER: Routing by Key
# We want all claims from the SAME hospital to go to the SAME partition 
# to keep them in order. We use a Hash of the hospital name.
def get_partition(key):
    return int(hashlib.md5(key.encode()).hexdigest(), 16) % 3

print("\n[PRODUCER] Routing claims to partitions...")
for c in claims:
    p = get_partition(c['hospital'])
    topic_partitions[p].append(c)
    print(f"Claim {c['id']} from {c['hospital']} -> Partition {p}")

# 3. CONSUMER: Tracking Offsets
def consume_data():
    print("\n[CONSUMER] Starting read...")
    for p, messages in topic_partitions.items():
        for i, msg in enumerate(messages):
            # The "Offset" is simply the index in the list
            current_offset = i
            print(f"Reading Partition {p}, Offset {current_offset}: {msg['id']}")
            
            # Commit the offset (save the bookmark)
            consumer_offsets[p] = current_offset + 1

consume_data()

print("\n--- Final Consumer State (Bookmarks) ---")
print(f"Next expected offsets: {consumer_offsets}")
