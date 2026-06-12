#!/bin/bash
# Script to generate LDBC SNB SF10 and SF30 BI datasets using Spark datagen
#
# This script generates both SF10 and SF30 datasets sequentially.
# SF10: ~20GB parquet, ~500GB-1TB memory after load
# SF30: ~60GB parquet, ~1.5TB-3TB memory after load

set -e

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
LDBC_DATA_DIR="${LDBC_DATA_DIR:-${PROJECT_ROOT}/ldbc_data}"
DATAGEN_DIR="${PROJECT_ROOT}/ldbc_snb_datagen_spark"
SEPARATOR="=========================================="

# Set up Java PATH
if [[ -d "/opt/homebrew/opt/openjdk@11" ]]; then
    export PATH="/opt/homebrew/opt/openjdk@11/bin:$PATH"
    export JAVA_HOME="/opt/homebrew/opt/openjdk@11"
elif [[ -d "/opt/homebrew/opt/openjdk" ]]; then
    export PATH="/opt/homebrew/opt/openjdk/bin:$PATH"
    export JAVA_HOME="/opt/homebrew/opt/openjdk"
fi

# Set up Spark
export SPARK_HOME="${HOME}/spark-3.2.2-bin-hadoop3.2"
export PATH="${SPARK_HOME}/bin:${PATH}"

echo "=== LDBC SNB SF10 and SF30 Generation Script ==="
echo ""
echo "This script will generate:"
echo "  - SF10: ~20GB parquet, ~500GB-1TB memory after load"
echo "  - SF30: ~60GB parquet, ~1.5TB-3TB memory after load"
echo ""
echo "IMPORTANT:"
echo "  - SF10 requires ~20GB disk space (parquet)"
echo "  - SF30 requires ~60GB disk space (parquet)"
echo "  - Generation may take 3-6 hours for SF10, 9-18 hours for SF30"
echo "  - Ensure you have sufficient RAM for loading (500GB+ for SF10, 1.5TB+ for SF30)"
echo ""

cd "${DATAGEN_DIR}"

# Set JAR path
export LDBC_SNB_DATAGEN_JAR="${DATAGEN_DIR}/target/ldbc_snb_datagen_2.12_spark3.2-0.5.1+23-1d60a657-jar-with-dependencies.jar"

if [[ ! -f "${LDBC_SNB_DATAGEN_JAR}" ]]; then
    echo "ERROR: JAR file not found. Please build first:" >&2
    echo "  cd ${DATAGEN_DIR}"
    echo "  ./scripts/build.sh"
    exit 1
fi

# Function to generate and move data
generate_and_move() {
    local scale_factor=$1
    local output_dir="${LDBC_DATA_DIR}/social-network-sf${scale_factor}-bi-parquet"
    
    echo ""
    echo "${SEPARATOR}"
    echo "Generating SF${scale_factor}..."
    echo "${SEPARATOR}"
    echo ""
    
    # Check if already exists
    if [[ -d "${output_dir}" ]]; then
        echo "SF${scale_factor} already exists at: ${output_dir}"
        read -p "Regenerate? (y/N): " REGEN
        if [[ "$REGEN" != "y" ]] && [[ "$REGEN" != "Y" ]]; then
            echo "Skipping SF${scale_factor}"
            return 0
        fi
        rm -rf "${output_dir}"
    fi
    
    # Generate
    echo "Starting generation (this will take a while)..."
    python3 tools/run.py -y -- --format parquet --scale-factor "${scale_factor}" --mode bi
    
    if [[ $? -ne 0 ]]; then
        echo "ERROR: SF${scale_factor} generation failed" >&2
        return 1
    fi
    
    # Move to expected location
    if [[ -d "out/graphs" ]]; then
        mkdir -p "${LDBC_DATA_DIR}"
        mv "out/graphs" "${output_dir}/graphs"
        echo "✓ SF${scale_factor} data moved to: ${output_dir}"
        
        # Show size
        SIZE=$(du -sh "${output_dir}" | cut -f1)
        echo "  Size: ${SIZE}"
    else
        echo "WARNING: Could not find output directory"
    fi
}

# Generate SF10
generate_and_move 10

# Generate SF30
generate_and_move 30

echo ""
echo "${SEPARATOR}"
echo "Generation Complete!"
echo "${SEPARATOR}"
echo ""
echo "Generated datasets:"
echo "  - SF10: ${LDBC_DATA_DIR}/social-network-sf10-bi-parquet"
echo "  - SF30: ${LDBC_DATA_DIR}/social-network-sf30-bi-parquet"
echo ""
echo "To use with benchmarks:"
echo "  export LDBC_SF=10  # or 30"
echo "  cd rustychickpeas-core"
echo "  cargo test --test ldbc_snb_bi_benchmark --release"

