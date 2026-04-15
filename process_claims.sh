#!/bin/bash

# ==========================================
# Medical Insurance Claim Processing D - Automation Script
# ==========================================

# 1. Define Variables
BASE_DIR="$HOME/med_claims_d"
LANDING="$BASE_DIR/landing"
PROCESSING="$BASE_DIR/processing"
ARCHIVE="$BASE_DIR/archive"
LOG_FILE="$BASE_DIR/logs/pipeline.log"

# 2. Define a Logging Function
log_action() {
    # This prints the current date/time and the message passed to the function
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $1" >> "$LOG_FILE"
}

log_action "--- Pipeline Execution Started ---"

# 3. Check if there are files in the landing zone
if [ -z "$(ls -A "$LANDING")" ]; then
    log_action "No new claims found in the landing zone. Exiting."
    exit 0
fi

# 4. Automate File Movement
# Loop through all files in the landing directory
for file in "$LANDING"/*; do
    
    # Extract just the filename from the path
    filename=$(basename "$file")
    
    log_action "Detected new file: $filename"
    
    # Step A: Move to processing
    mv "$file" "$PROCESSING/"
    log_action "Moved $filename to processing zone."
    
    # Step B: Simulate the actual "processing" work (e.g., a Python script parsing the claim)
    # In a real pipeline, you would call `python3 process_data.py "$PROCESSING/$filename"` here
    sleep 1 
    
    # Step C: Move to archive upon completion
    mv "$PROCESSING/$filename" "$ARCHIVE/"
    log_action "Successfully processed and archived $filename."

done

log_action "--- Pipeline Execution Completed ---"
echo "Pipeline run successfully. Check the logs for details."
