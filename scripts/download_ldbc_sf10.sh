#!/bin/bash
# Script to download and extract LDBC SNB SF10 BI dataset
#
# The SF10 dataset is available from the LDBC Data Repository:
# https://ldbcouncil.org/data-sets-surf-repository/
#
# Note: SF10 is very large (~20GB compressed, ~1TB uncompressed)
# Ensure you have sufficient disk space and bandwidth

set -e

# Configuration
LDBC_DATA_DIR="${LDBC_DATA_DIR:-../ldbc_data}"
SF10_DIR="${LDBC_DATA_DIR}/social-network-sf10-bi-parquet"
DOWNLOAD_URL=""  # Will be set based on repository link
ARCHIVE_NAME="ldbc_snb_sf10_bi-parquet.tar.zst"

echo "=== LDBC SNB SF10 Download Script ==="
echo ""
echo "This script will help you download the LDBC SNB SF10 dataset."
echo ""
echo "IMPORTANT:"
echo "  - SF10 is ~20GB compressed, ~1TB uncompressed"
echo "  - Ensure you have sufficient disk space"
echo "  - Download may take several hours depending on bandwidth"
echo ""
echo "Data will be extracted to: ${SF10_DIR}"
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

echo "Step 1: Download from LDBC Data Repository"
echo ""
echo "The SF10 dataset must be downloaded from the LDBC Data Repository:"
echo "  https://ldbcouncil.org/data-sets-surf-repository/"
echo ""
echo "Instructions:"
echo "  1. Go to the repository link above"
echo "  2. Navigate to 'SNB Business Intelligence (BI)' section"
echo "  3. Find 'SF10' dataset"
echo "  4. Click 'Request' to stage the dataset (if stored offline)"
echo "  5. Once staged, copy the download URL"
echo ""
read -p "Enter the download URL for SF10 (or press Enter to skip download): " DOWNLOAD_URL

if [ -z "$DOWNLOAD_URL" ]; then
    echo ""
    echo "Skipping download. If you've already downloaded the file, place it as:"
    echo "  ${LDBC_DATA_DIR}/${ARCHIVE_NAME}"
    echo ""
    read -p "Press Enter to continue with extraction (if file exists)..."
else
    echo ""
    echo "Downloading SF10 dataset..."
    echo "This may take a while (20GB download)..."
    
    if command -v wget &> /dev/null; then
        wget --progress=bar:force -O "${ARCHIVE_NAME}" "${DOWNLOAD_URL}"
    else
        curl -L --progress-bar -o "${ARCHIVE_NAME}" "${DOWNLOAD_URL}"
    fi
    
    if [ $? -ne 0 ]; then
        echo "ERROR: Download failed"
        exit 1
    fi
    
    echo "Download complete!"
fi

# Check if archive exists
if [ ! -f "${ARCHIVE_NAME}" ]; then
    echo ""
    echo "ERROR: Archive file not found: ${ARCHIVE_NAME}"
    echo "Please download it first or place it in ${LDBC_DATA_DIR}/"
    exit 1
fi

echo ""
echo "Step 2: Extract the dataset"
echo "Extracting ${ARCHIVE_NAME}..."
echo "This may take 30-60 minutes depending on disk speed..."

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
if [ -d "social-network-sf10-bi-parquet" ]; then
    echo "✓ SF10 dataset extracted successfully!"
    echo ""
    echo "Dataset location: ${SF10_DIR}"
    echo ""
    echo "You can now run the benchmarks:"
    echo "  cd rustychickpeas-core"
    echo "  cargo test --test ldbc_snb_bi_benchmark --release -- --nocapture"
else
    echo "WARNING: Expected directory 'social-network-sf10-bi-parquet' not found"
    echo "Please check the extraction output above"
fi

echo ""
echo "Done!"

