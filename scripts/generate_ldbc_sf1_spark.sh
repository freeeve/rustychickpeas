#!/bin/bash
# Script to generate LDBC SNB SF1 BI dataset using Spark datagen
#
# This script:
# 1. Clones the ldbc_snb_datagen_spark repository
# 2. Builds the project with Maven
# 3. Generates SF1 parquet files
# 4. Moves the output to the expected location
#
# Requirements:
# - Java (JDK 8 or later)
# - Maven
# - Apache Spark (or it will be downloaded)
# - Python 3 (for the run script)
# - Sufficient disk space (~100GB for SF1)

set -e

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
LDBC_DATA_DIR="${LDBC_DATA_DIR:-${PROJECT_ROOT}/ldbc_data}"
DATAGEN_DIR="${PROJECT_ROOT}/ldbc_snb_datagen_spark"
SCALE_FACTOR="${SCALE_FACTOR:-1}"
OUTPUT_DIR="${LDBC_DATA_DIR}/social-network-sf${SCALE_FACTOR}-bi-parquet"

echo "=== LDBC SNB SF${SCALE_FACTOR} Spark Datagen Script ==="
echo ""
echo "This script will generate LDBC SNB SF${SCALE_FACTOR} dataset using Spark datagen."
echo ""
echo "IMPORTANT:"
echo "  - SF${SCALE_FACTOR} requires ~100GB disk space (uncompressed)"
echo "  - Generation may take 1-3 hours depending on hardware"
echo "  - Ensure you have Java, Maven, and Spark installed"
echo ""
echo "Output will be placed at: ${OUTPUT_DIR}"
echo ""

# Set up Java PATH if Homebrew Java is installed
# Prefer Java 11 for compatibility with Scala 2.12 and Spark 3.2.x
if [ -d "/opt/homebrew/opt/openjdk@11" ]; then
    export PATH="/opt/homebrew/opt/openjdk@11/bin:$PATH"
    export JAVA_HOME="/opt/homebrew/opt/openjdk@11"
elif [ -d "/opt/homebrew/opt/openjdk" ]; then
    export PATH="/opt/homebrew/opt/openjdk/bin:$PATH"
    export JAVA_HOME="/opt/homebrew/opt/openjdk"
fi

# Check for required tools
echo "Step 1: Checking for required tools..."

if ! command -v java &> /dev/null; then
    echo "ERROR: Java not found. Please install Java JDK 8 or later."
    echo "  macOS: brew install openjdk"
    echo "  Linux: apt-get install openjdk-11-jdk  (or similar)"
    exit 1
fi

JAVA_VERSION=$(java -version 2>&1 | head -n 1 | cut -d'"' -f2 | sed '/^1\./s///' | cut -d'.' -f1)
if [ "$JAVA_VERSION" -lt 8 ] 2>/dev/null; then
    echo "ERROR: Java 8 or later is required. Found version: $JAVA_VERSION"
    exit 1
fi
echo "✓ Java found: $(java -version 2>&1 | head -n 1)"

if ! command -v sbt &> /dev/null; then
    echo "ERROR: SBT (Scala Build Tool) not found. Please install SBT:"
    echo "  macOS: brew install sbt"
    echo "  Linux: apt-get install sbt  (or follow https://www.scala-sbt.org/download.html)"
    exit 1
fi
echo "✓ SBT found: $(sbt -version 2>&1 | head -n 1)"

if ! command -v python3 &> /dev/null; then
    echo "ERROR: Python 3 not found. Please install Python 3."
    exit 1
fi
echo "✓ Python 3 found: $(python3 --version)"

# Check for Spark (optional, will be downloaded if needed)
if command -v spark-submit &> /dev/null; then
    echo "✓ Spark found: $(spark-submit --version 2>&1 | head -n 1)"
else
    echo "⚠ Spark not found in PATH. The datagen script will download it if needed."
fi

echo ""
echo "Step 2: Setting up LDBC SNB Datagen Spark repository..."

# Clone or update the repository
if [ -d "${DATAGEN_DIR}" ]; then
    echo "Repository already exists at: ${DATAGEN_DIR}"
    if [ -z "${NON_INTERACTIVE:-}" ]; then
        read -p "Update repository? (y/N): " UPDATE_REPO
        if [ "$UPDATE_REPO" = "y" ] || [ "$UPDATE_REPO" = "Y" ]; then
            echo "Updating repository..."
            cd "${DATAGEN_DIR}"
            git pull || echo "Warning: Could not update repository"
        fi
    else
        echo "Skipping update (non-interactive mode)"
    fi
else
    echo "Cloning ldbc_snb_datagen_spark repository..."
    cd "${PROJECT_ROOT}"
    git clone https://github.com/ldbc/ldbc_snb_datagen_spark.git
fi

cd "${DATAGEN_DIR}"

# Check if the repository is valid (this is an SBT project, not Maven)
if [ ! -f "build.sbt" ]; then
    echo "ERROR: Repository does not appear to be valid (build.sbt not found)"
    exit 1
fi

echo ""
echo "Step 3: Installing Python tools dependencies..."
if [ -f "tools/setup.py" ] || [ -f "tools/pyproject.toml" ]; then
    echo "Installing Python dependencies for datagen tools..."
    cd tools
    if [ -f "setup.py" ]; then
        pip3 install . --user 2>&1 | tail -5 || echo "Note: Python tools installation had issues, continuing anyway..."
    elif [ -f "pyproject.toml" ]; then
        pip3 install . --user 2>&1 | tail -5 || echo "Note: Python tools installation had issues, continuing anyway..."
    fi
    cd ..
fi

echo ""
echo "Step 4: Building the project with SBT..."
echo "This may take 10-20 minutes on first run (SBT will download dependencies)..."

# Check if build script exists
if [ -f "scripts/build.sh" ]; then
    echo "Using build.sh script (runs 'sbt assembly')..."
    if [ -d "target" ] && find target -name "*.jar" -path "*/assembly/*" | grep -q .; then
        echo "JAR file already exists. Skipping build."
        read -p "Rebuild? (y/N): " REBUILD
        if [ "$REBUILD" = "y" ] || [ "$REBUILD" = "Y" ]; then
            ./scripts/build.sh
        fi
    else
        ./scripts/build.sh
    fi
else
    # Build directly with SBT
    if [ -d "target" ] && find target -name "*.jar" -path "*/assembly/*" | grep -q .; then
        echo "JAR file already exists. Skipping build."
        read -p "Rebuild? (y/N): " REBUILD
        if [ "$REBUILD" = "y" ] || [ "$REBUILD" = "Y" ]; then
            sbt assembly
        fi
    else
        sbt assembly
    fi
fi

if [ $? -ne 0 ]; then
    echo "ERROR: Build failed"
    exit 1
fi

echo ""
echo "Step 5: Setting up Spark (if needed)..."
# Check if Spark is available, if not the run.py script should handle it
if ! command -v spark-submit &> /dev/null; then
    if [ -f "scripts/get-spark-to-home.sh" ]; then
        echo "Spark not found. The run.py script will download it automatically."
        echo "Alternatively, you can run: ./scripts/get-spark-to-home.sh"
    fi
fi

echo ""
echo "Step 6: Generating SF${SCALE_FACTOR} parquet files..."
echo "This will take 1-3 hours depending on your hardware..."
echo ""

# Create output directory
mkdir -p "${LDBC_DATA_DIR}"

# Check if output already exists
if [ -d "${OUTPUT_DIR}" ]; then
    echo "Output directory already exists: ${OUTPUT_DIR}"
    read -p "Regenerate? This will overwrite existing data. (y/N): " REGENERATE
    if [ "$REGENERATE" != "y" ] && [ "$REGENERATE" != "Y" ]; then
        echo "Skipping generation."
        echo ""
        echo "Dataset location: ${OUTPUT_DIR}"
        echo ""
        echo "You can now run the benchmarks:"
        echo "  cd rustychickpeas-core"
        echo "  LDBC_SF=${SCALE_FACTOR} cargo test --test ldbc_snb_bi_benchmark --release -- --nocapture"
        exit 0
    fi
    echo "Removing existing output directory..."
    rm -rf "${OUTPUT_DIR}"
fi

# Run the datagen
# The run.py script should be in the tools directory
if [ -f "tools/run.py" ]; then
    echo "Running datagen with:"
    echo "  Format: parquet"
    echo "  Scale Factor: ${SCALE_FACTOR}"
    echo "  Mode: bi"
    echo ""
    
    # Run the datagen script
    python3 tools/run.py -- --format parquet --scale-factor "${SCALE_FACTOR}" --mode bi
    
    if [ $? -ne 0 ]; then
        echo "ERROR: Data generation failed"
        exit 1
    fi
    
    # The output should be in the 'out' directory by default
    # Check for the expected structure
    GENERATED_DIR=""
    if [ -d "out/social-network-sf${SCALE_FACTOR}-bi-parquet" ]; then
        GENERATED_DIR="out/social-network-sf${SCALE_FACTOR}-bi-parquet"
    elif [ -d "out" ]; then
        # Check what was actually generated
        echo "Generated output structure:"
        ls -la out/ | head -20
        # Try to find the correct directory
        for dir in out/*; do
            if [ -d "$dir" ] && [[ "$dir" == *"sf${SCALE_FACTOR}"* ]] || [[ "$dir" == *"bi-parquet"* ]]; then
                GENERATED_DIR="$dir"
                break
            fi
        done
        if [ -z "$GENERATED_DIR" ]; then
            # Use the out directory itself if we can't find a better match
            GENERATED_DIR="out"
        fi
    else
        echo "ERROR: Could not find generated output in 'out' directory"
        echo "Please check the datagen output above for errors"
        exit 1
    fi
    
    echo ""
    echo "Step 7: Moving output to expected location..."
    echo "Source: ${GENERATED_DIR}"
    echo "Destination: ${OUTPUT_DIR}"
    
    # Move the generated data to the expected location
    if [ -d "${GENERATED_DIR}" ]; then
        # Remove destination if it exists
        if [ -d "${OUTPUT_DIR}" ]; then
            rm -rf "${OUTPUT_DIR}"
        fi
        mv "${GENERATED_DIR}" "${OUTPUT_DIR}"
        echo "✓ Data moved to: ${OUTPUT_DIR}"
    else
        echo "WARNING: Could not find generated directory structure"
        echo "Please manually check the 'out' directory and move files as needed"
    fi
    
elif [ -f "run.py" ]; then
    # Alternative location for run.py
    python3 run.py -- --format parquet --scale-factor "${SCALE_FACTOR}" --mode bi
    
    if [ $? -ne 0 ]; then
        echo "ERROR: Data generation failed"
        exit 1
    fi
    
    # Handle output similarly
    if [ -d "out/social-network-sf${SCALE_FACTOR}-bi-parquet" ]; then
        mv "out/social-network-sf${SCALE_FACTOR}-bi-parquet" "${OUTPUT_DIR}"
    elif [ -d "out" ]; then
        mv "out" "${OUTPUT_DIR}"
    fi
else
    echo "ERROR: Could not find run.py script in tools/ or root directory"
    echo "Please check the repository structure"
    exit 1
fi

echo ""
echo "Step 8: Verifying output structure..."

# Verify the expected structure exists
EXPECTED_PATH="${OUTPUT_DIR}/graphs/parquet/bi/composite-merged-fk/initial_snapshot"
if [ -d "${EXPECTED_PATH}" ]; then
    echo "✓ Expected directory structure found: ${EXPECTED_PATH}"
    echo ""
    echo "Dataset generated successfully!"
    echo ""
    echo "Dataset location: ${OUTPUT_DIR}"
    echo "Expected path for benchmarks: ${EXPECTED_PATH}"
    echo ""
    echo "You can now run the benchmarks:"
    echo "  cd rustychickpeas-core"
    echo "  LDBC_SF=${SCALE_FACTOR} cargo test --test ldbc_snb_bi_benchmark --release -- --nocapture"
else
    echo "WARNING: Expected directory structure not found at: ${EXPECTED_PATH}"
    echo "Generated structure:"
    find "${OUTPUT_DIR}" -type d -maxdepth 3 | head -20
    echo ""
    echo "Please verify the output structure matches what the benchmarks expect"
fi

echo ""
echo "Done!"

