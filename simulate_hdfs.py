import random

print("--- HDFS Architecture Simulation ---")

# 1. The Setup
# Imagine a massive 500GB file of historical medical claims.
# HDFS chops it into smaller chunks (Blocks).
file_name = "historical_claims_2020_2025.csv"
file_blocks = ["Block_1 (Claims A-E)", "Block_2 (Claims F-J)", "Block_3 (Claims K-O)", "Block_4 (Claims P-Z)"]

# Our simulated worker servers
datanodes = ["DataNode_1", "DataNode_2", "DataNode_3", "DataNode_4", "DataNode_5"]
replication_factor = 3  # Copy every block 3 times for safety

# The NameNode's Memory (The Map)
namenode_metadata = {}
# The Physical Hard Drives of the Workers
cluster_storage = {dn: [] for dn in datanodes}

print(f"Uploading '{file_name}' to HDFS...")
print(f"Chopping file into {len(file_blocks)} blocks with a replication factor of {replication_factor}.\n")

# 2. Simulate the NameNode distributing the blocks
for block in file_blocks:
    # The NameNode picks 3 random worker nodes to store the block
    assigned_nodes = random.sample(datanodes, replication_factor)
    
    # NameNode updates its map
    namenode_metadata[block] = assigned_nodes
    
    # The DataNodes physically save the data
    for node in assigned_nodes:
        cluster_storage[node].append(block)

# 3. View the Results

print("=== NAMENODE METADATA (The Map) ===")
# This is what the NameNode uses to find your data when you query it
for block, nodes in namenode_metadata.items():
    print(f"{block} is stored on: {', '.join(nodes)}")

print("\n=== DATANODE STORAGE (Physical Hard Drives) ===")
# This is what is actually sitting on the worker servers
for node, blocks in cluster_storage.items():
    print(f"{node} holds -> {', '.join(blocks)}")
    
print("\nSimulation Complete: Notice how if any single DataNode crashes, your data is still safe on the others!")
