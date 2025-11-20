#!/bin/bash
# Quick script to monitor SF10 generation progress

echo "=== LDBC SF10 Generation Monitor ==="
echo ""

# Check if generation process is running
if pgrep -f "run.py.*scale-factor 10" > /dev/null; then
    echo "✓ SF10 generation is RUNNING"
    PID=$(pgrep -f "run.py.*scale-factor 10" | head -1)
    echo "  Process ID: $PID"
    RUNTIME=$(ps -p $PID -o etime= 2>/dev/null | tr -d ' ' || echo 'unknown')
    echo "  Runtime: $RUNTIME"
    
    # Check for Spark processes
    SPARK_COUNT=$(pgrep -f "spark-submit" | wc -l | tr -d ' ')
    if [ "$SPARK_COUNT" -gt 0 ]; then
        echo "  Spark processes: $SPARK_COUNT"
    fi
else
    echo "✗ SF10 generation process is NOT running"
fi

echo ""

# Check output directory
OUT_DIR="/Users/efreeman/rustychickpeas/ldbc_snb_datagen_spark/out"
if [ -d "$OUT_DIR/graphs" ]; then
    echo "✓ Output directory exists: $OUT_DIR/graphs"
    
    # Count files generated
    FILE_COUNT=$(find "$OUT_DIR" -type f -name "*.parquet" 2>/dev/null | wc -l | tr -d ' ')
    echo "  Parquet files generated: $FILE_COUNT"
    
    # Calculate total size
    TOTAL_SIZE=$(du -sh "$OUT_DIR" 2>/dev/null | cut -f1)
    echo "  Total size: $TOTAL_SIZE"
    
    # Check for initial_snapshot
    if [ -d "$OUT_DIR/graphs/parquet/bi/composite-merged-fk/initial_snapshot" ]; then
        SNAPSHOT_SIZE=$(du -sh "$OUT_DIR/graphs/parquet/bi/composite-merged-fk/initial_snapshot" 2>/dev/null | cut -f1)
        echo "  Initial snapshot size: $SNAPSHOT_SIZE"
    fi
else
    echo "⚠ Output directory not created yet (generation in early stages)"
fi

echo ""

# Check log file
LOG_FILE="/tmp/ldbc_sf10_gen.log"
if [ -f "$LOG_FILE" ]; then
    echo "✓ Log file: $LOG_FILE"
    LOG_SIZE=$(du -h "$LOG_FILE" | cut -f1)
    echo "  Log size: $LOG_SIZE"
    echo ""
    echo "Recent activity (last 5 lines):"
    tail -5 "$LOG_FILE" | sed 's|^|  |'
else
    echo "✗ Log file not found"
fi

echo ""
echo "To view full log: tail -f $LOG_FILE"
echo "Expected completion: 3-6 hours from start"
echo "Expected output size: ~20GB parquet"

