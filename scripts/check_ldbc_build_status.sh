#!/bin/bash
# Quick script to check the status of LDBC datagen build

echo "=== LDBC Datagen Build Status ==="
echo ""

# Check if SBT is running
if pgrep -f "sbt.*assembly" > /dev/null; then
    echo "✓ SBT build process is running"
    echo "  PID: $(pgrep -f 'sbt.*assembly' | head -1)"
else
    echo "✗ SBT build process is not running"
fi

echo ""

# Check for JAR file
JAR_FILE=$(find /Users/efreeman/rustychickpeas/ldbc_snb_datagen_spark/target -name "*.jar" -path "*/assembly/*" 2>/dev/null | head -1)
if [ -n "$JAR_FILE" ]; then
    echo "✓ JAR file found:"
    echo "  $JAR_FILE"
    echo "  Size: $(du -h "$JAR_FILE" | cut -f1)"
else
    echo "✗ JAR file not found yet (build may still be in progress)"
fi

echo ""

# Check for generated data
DATA_DIR="/Users/efreeman/rustychickpeas/ldbc_data/social-network-sf1-bi-parquet"
if [ -d "$DATA_DIR" ]; then
    echo "✓ Generated data directory exists:"
    echo "  $DATA_DIR"
    echo "  Size: $(du -sh "$DATA_DIR" 2>/dev/null | cut -f1 || echo 'calculating...')"
else
    echo "✗ Generated data directory not found yet"
fi

echo ""

# Check Java version being used
if [ -n "$JAVA_HOME" ]; then
    echo "Java: $JAVA_HOME"
    "$JAVA_HOME/bin/java" -version 2>&1 | head -1
else
    echo "Java: Using system default"
    java -version 2>&1 | head -1
fi

