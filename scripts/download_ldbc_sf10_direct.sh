#!/bin/bash
# Direct download script for LDBC SNB SF10
# This script attempts to download SF10 if you provide the URL

set -e

LDBC_DATA_DIR="${LDBC_DATA_DIR:-ldbc_data}"
ARCHIVE_NAME="ldbc_snb_sf10_bi-parquet.tar.zst"
TARGET_DIR="${LDBC_DATA_DIR}/social-network-sf10-bi-parquet"

echo "=== LDBC SNB SF10 Direct Download ==="
echo ""

# Check if already extracted
if [ -d "${TARGET_DIR}" ]; then
    echo "✓ SF10 dataset already exists at: ${TARGET_DIR}"
    echo "Skipping download."
    exit 0
fi

# Check if archive already exists
if [ -f "${LDBC_DATA_DIR}/${ARCHIVE_NAME}" ]; then
    echo "Archive found: ${LDBC_DATA_DIR}/${ARCHIVE_NAME}"
    echo "Extracting..."
    cd "${LDBC_DATA_DIR}"
    tar -xvf "${ARCHIVE_NAME}" --use-compress-program=zstd
    echo "✓ Extraction complete!"
    exit 0
fi

echo "To download SF10:"
echo "1. Visit: https://ldbcouncil.org/data-sets-surf-repository/"
echo "2. Navigate to: SNB Business Intelligence (BI) → SF10"
echo "3. Click 'Request' if needed to stage the dataset"
echo "4. Copy the download URL"
echo ""
echo "Then run:"
echo "  cd ${LDBC_DATA_DIR}"
echo "  curl -L -o ${ARCHIVE_NAME} <DOWNLOAD_URL>"
echo "  tar -xvf ${ARCHIVE_NAME} --use-compress-program=zstd"
echo ""
echo "Or if you have the URL now, provide it:"
read -p "Download URL (or press Enter to skip): " DOWNLOAD_URL

if [ -n "$DOWNLOAD_URL" ]; then
    echo ""
    echo "Downloading SF10 dataset (this may take hours for 20GB)..."
    mkdir -p "${LDBC_DATA_DIR}"
    cd "${LDBC_DATA_DIR}"
    
    curl -L --progress-bar -o "${ARCHIVE_NAME}" "${DOWNLOAD_URL}" || {
        echo "ERROR: Download failed"
        exit 1
    }
    
    echo "Download complete! Extracting..."
    tar -xvf "${ARCHIVE_NAME}" --use-compress-program=zstd || {
        echo "ERROR: Extraction failed"
        exit 1
    }
    
    echo "✓ SF10 dataset ready at: ${TARGET_DIR}"
else
    echo "Skipping download. Run this script again with the URL when ready."
fi
