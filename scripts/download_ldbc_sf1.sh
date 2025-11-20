#!/bin/bash
# Script to download and extract LDBC SNB SF1 BI dataset
#
# The SF1 dataset is available from the LDBC Data Repository:
# https://repository.surfsara.nl/datasets/cwi/ldbc-snb-bi/files/bi-sf1-composite-merged-fk.tar.zst
#
# Note: SF1 is ~1-2GB compressed, ~10-20GB uncompressed
# Ensure you have sufficient disk space

set -e

# Configuration
LDBC_DATA_DIR="${LDBC_DATA_DIR:-../ldbc_data}"
SF1_DIR="${LDBC_DATA_DIR}/social-network-sf1-bi-parquet"
DOWNLOAD_URL="https://repository.surfsara.nl/datasets/cwi/ldbc-snb-bi/files/bi-sf1-composite-merged-fk.tar.zst"
ARCHIVE_NAME="bi-sf1-composite-merged-fk.tar.zst"

echo "=== LDBC SNB SF1 Download Script ==="
echo ""
echo "This script will download and extract the LDBC SNB SF1 dataset."
echo ""
echo "IMPORTANT:"
echo "  - SF1 is ~1-2GB compressed, ~10-20GB uncompressed"
echo "  - Ensure you have sufficient disk space"
echo "  - The dataset may need to be staged from tape (takes 3-5 minutes)"
echo ""
echo "Data will be extracted to: ${SF1_DIR}"
echo ""

# Check for required tools
if ! command -v wget &> /dev/null && ! command -v curl &> /dev/null; then
    echo "ERROR: Neither wget nor curl found. Please install one of them."
    exit 1
fi

if ! command -v zstd &> /dev/null && ! command -v unzstd &> /dev/null; then
    echo "ERROR: zstd or unzstd not found. Please install zstd:"
    echo "  macOS: brew install zstd"
    echo "  Linux: apt-get install zstd  (or yum install zstd)"
    exit 1
fi

# Create data directory
mkdir -p "${LDBC_DATA_DIR}"
cd "${LDBC_DATA_DIR}"

# Check if already downloaded
if [ -f "${ARCHIVE_NAME}" ]; then
    echo "Archive already exists: ${ARCHIVE_NAME}"
    read -p "Re-download? (y/N): " REDOWNLOAD
    if [ "$REDOWNLOAD" != "y" ] && [ "$REDOWNLOAD" != "Y" ]; then
        echo "Skipping download, using existing file."
    else
        rm -f "${ARCHIVE_NAME}"
    fi
fi

# Check if already extracted
if [ -d "social-network-sf1-bi-parquet" ]; then
    echo ""
    echo "Dataset already extracted at: social-network-sf1-bi-parquet"
    read -p "Re-extract? (y/N): " REEXTRACT
    if [ "$REEXTRACT" != "y" ] && [ "$REEXTRACT" != "Y" ]; then
        echo "Skipping extraction."
        echo ""
        echo "Dataset location: ${SF1_DIR}"
        echo ""
        echo "You can now run the benchmarks:"
        echo "  cd rustychickpeas-core"
        echo "  LDBC_SF=1 cargo test --test ldbc_snb_bi_benchmark --release -- --nocapture"
        exit 0
    fi
fi

# Download if needed
if [ ! -f "${ARCHIVE_NAME}" ]; then
    echo "Step 1: Downloading SF1 dataset from SURF repository..."
    echo "URL: ${DOWNLOAD_URL}"
    echo ""
    echo "Note: If the file is stored on tape, it will be automatically staged."
    echo "Staging takes 3-5 minutes. The download will wait and retry."
    echo ""
    
    # Use the LDBC download script approach - it handles staging automatically
    if command -v curl &> /dev/null; then
        echo "Preparing to download ${DOWNLOAD_URL}"
        STAGING_INITIATED=false
        
        # Check if file needs staging and handle it (same logic as official LDBC script)
        while curl --silent --head "${DOWNLOAD_URL}" | grep -q 'HTTP/1.1 409 Conflict'; do
            if [ "$STAGING_INITIATED" = false ]; then
                echo "Data set is not staged, attempting to stage..."
                # Extract staging URL using the same pattern as official script
                STAGING_URL=$(curl --silent "${DOWNLOAD_URL}" | grep -Eo 'https:\\/\\/repository.surfsara.nl\\/api\\/objects\\/cwi\\/[A-Za-z0-9_-]+\\/stage\\/[0-9]+' | sed 's#\\##g' | head -1)
                
                if [ -z "${STAGING_URL}" ]; then
                    echo "Could not retrieve staging URL, exiting..."
                    echo ""
                    echo "Please manually stage the dataset by visiting:"
                    echo "  ${DOWNLOAD_URL}"
                    echo "and clicking 'Request' to stage it, then run this script again."
                    exit 1
                fi
                
                curl "${STAGING_URL}" --data-raw 'share-token=' > /dev/null 2>&1
                echo "Staging initiated through ${STAGING_URL}"
                echo "Staging typically takes 3-5 minutes. Waiting and checking periodically..."
                STAGING_INITIATED=true
            else
                # Already initiated staging, just wait and check
                echo "Still staging... (this can take 3-5 minutes)"
            fi
            
            echo "Waiting 30 seconds before checking again..."
            sleep 30
        done
        
        echo "Downloading data set..."
        curl -L --progress-bar -o "${ARCHIVE_NAME}" "${DOWNLOAD_URL}"
    elif command -v wget &> /dev/null; then
        wget --progress=bar:force -O "${ARCHIVE_NAME}" "${DOWNLOAD_URL}"
    fi
    
    if [ $? -ne 0 ]; then
        echo "ERROR: Download failed"
        echo ""
        echo "If you got a 409 Conflict error, the dataset needs to be staged."
        echo "Please visit: ${DOWNLOAD_URL}"
        echo "and click 'Request' to stage it, then run this script again."
        exit 1
    fi
    
    echo "Download complete!"
fi

echo ""
echo "Step 2: Extract the dataset"
echo "Extracting ${ARCHIVE_NAME}..."
echo "This may take 5-10 minutes depending on disk speed..."

# Extract using zstd
if command -v zstd &> /dev/null; then
    tar -xvf "${ARCHIVE_NAME}" --use-compress-program=zstd
elif command -v unzstd &> /dev/null; then
    tar -xvf "${ARCHIVE_NAME}" --use-compress-program=unzstd
else
    echo "ERROR: Cannot find zstd or unzstd for extraction"
    exit 1
fi

if [ $? -ne 0 ]; then
    echo "ERROR: Extraction failed"
    exit 1
fi

echo ""
echo "Step 3: Verify extraction"
# The extracted directory name may vary, check for common patterns
if [ -d "social-network-sf1-bi-parquet" ]; then
    EXTRACTED_DIR="social-network-sf1-bi-parquet"
elif [ -d "bi-sf1-composite-merged-fk" ]; then
    EXTRACTED_DIR="bi-sf1-composite-merged-fk"
    # Check if we need to look inside for the actual data
    if [ -d "${EXTRACTED_DIR}/graphs" ]; then
        echo "Found graphs directory inside ${EXTRACTED_DIR}"
    fi
else
    # List what was extracted
    echo "Extracted contents:"
    ls -la
    echo ""
    echo "Please check the directory structure and update the script if needed."
    EXTRACTED_DIR=""
fi

if [ -n "$EXTRACTED_DIR" ]; then
    echo "✓ SF1 dataset extracted successfully!"
    echo ""
    echo "Dataset location: ${LDBC_DATA_DIR}/${EXTRACTED_DIR}"
    echo ""
    echo "You can now run the benchmarks:"
    echo "  cd rustychickpeas-core"
    echo "  LDBC_SF=1 cargo test --test ldbc_snb_bi_benchmark --release -- --nocapture"
else
    echo "WARNING: Expected directory structure not found"
    echo "Please check the extraction output above"
fi

echo ""
echo "Done!"
