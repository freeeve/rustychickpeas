#!/bin/bash
# Script to monitor LDBC SF1 data generation progress

echo "=== LDBC SF1 Data Generation Monitor ==="
echo ""

# Check if generation process is running
if pgrep -f "run.py.*scale-factor 1" > /dev/null; then
    echo "✓ Data generation is RUNNING"
    PID=$(pgrep -f "run.py.*scale-factor 1" | head -1)
    echo "  Process ID: $PID"
    echo "  Runtime: $(ps -p $PID -o etime= 2>/dev/null | tr -d ' ' || echo 'unknown')"
else
    echo "✗ Data generation process is NOT running"
fi

echo ""

# Check output directory
OUT_DIR="/Users/efreeman/rustychickpeas/ldbc_snb_datagen_spark/out"
if [ -d "$OUT_DIR" ]; then
    echo "✓ Output directory exists: $OUT_DIR"
    
    # Count files generated
    FILE_COUNT=$(find "$OUT_DIR" -type f -name "*.parquet" 2>/dev/null | wc -l | tr -d ' ')
    echo "  Parquet files generated: $FILE_COUNT"
    
    # Calculate total size
    TOTAL_SIZE=$(du -sh "$OUT_DIR" 2>/dev/null | cut -f1)
    echo "  Total size: $TOTAL_SIZE"
    
    # Show directory structure
    echo ""
    echo "Directory structure:"
    find "$OUT_DIR" -type d -maxdepth 3 | head -20 | sed 's|^|  |'
    
    # Show some example files
    echo ""
    echo "Sample files:"
    find "$OUT_DIR" -type f -name "*.parquet" | head -5 | sed 's|^|  |'
else
    echo "✗ Output directory not found yet"
fi

echo ""

# Check log file
LOG_FILE="/tmp/ldbc_gen_output.log"
if [ -f "$LOG_FILE" ]; then
    echo "✓ Log file: $LOG_FILE"
    LOG_SIZE=$(du -h "$LOG_FILE" | cut -f1)
    echo "  Log size: $LOG_SIZE"
    echo ""
    echo "Recent log entries (last 10 lines):"
    tail -10 "$LOG_FILE" | sed 's|^|  |'
else
    echo "✗ Log file not found"
fi

echo ""
echo "To view full log: tail -f $LOG_FILE"
echo "To check process: ps aux | grep run.py"

