#!/bin/bash

# Script to create a signed Git tag for a release
# Usage: ./scripts/create_release_tag.sh <version> [message]
# Example: ./scripts/create_release_tag.sh v0.1.0 "Release version 0.1.0"

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check if version is provided
if [ -z "$1" ]; then
    echo -e "${RED}Error: Version is required${NC}"
    echo "Usage: $0 <version> [message]"
    echo "Example: $0 v0.1.0 \"Release version 0.1.0\""
    exit 1
fi

VERSION=$1
MESSAGE=${2:-"Release $VERSION"}

# Validate version format (basic check)
if [[ ! $VERSION =~ ^v[0-9]+\.[0-9]+\.[0-9]+(-[a-zA-Z0-9]+)?$ ]]; then
    echo -e "${YELLOW}Warning: Version '$VERSION' doesn't follow semantic versioning (vMAJOR.MINOR.PATCH)${NC}"
    read -p "Continue anyway? (y/N) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

# Check if tag already exists
if git rev-parse "$VERSION" >/dev/null 2>&1; then
    echo -e "${RED}Error: Tag '$VERSION' already exists${NC}"
    echo "To delete and recreate:"
    echo "  git tag -d $VERSION"
    echo "  git push origin :refs/tags/$VERSION"
    exit 1
fi

# Check if GPG is configured
if ! git config user.signingkey >/dev/null 2>&1; then
    echo -e "${YELLOW}Warning: GPG signing key not configured${NC}"
    echo "Run: git config --global user.signingkey YOUR_KEY_ID"
    read -p "Continue without signing? (y/N) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
    SIGN_FLAG=""
else
    SIGN_FLAG="-s"
    echo -e "${GREEN}GPG signing key found, tag will be signed${NC}"
fi

# Check if we're on the main/master branch
CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
if [[ ! "$CURRENT_BRANCH" =~ ^(main|master)$ ]]; then
    echo -e "${YELLOW}Warning: Not on main/master branch (currently on '$CURRENT_BRANCH')${NC}"
    read -p "Continue anyway? (y/N) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

# Check if working directory is clean
if ! git diff-index --quiet HEAD --; then
    echo -e "${YELLOW}Warning: Working directory has uncommitted changes${NC}"
    git status --short
    read -p "Continue anyway? (y/N) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

# Create the signed tag
echo -e "${GREEN}Creating signed tag '$VERSION'...${NC}"
if git tag $SIGN_FLAG "$VERSION" -m "$MESSAGE"; then
    echo -e "${GREEN}✓ Tag '$VERSION' created successfully${NC}"
else
    echo -e "${RED}✗ Failed to create tag${NC}"
    exit 1
fi

# Verify the tag
if [ -n "$SIGN_FLAG" ]; then
    echo -e "${GREEN}Verifying tag signature...${NC}"
    if git tag -v "$VERSION" >/dev/null 2>&1; then
        echo -e "${GREEN}✓ Tag signature verified${NC}"
    else
        echo -e "${RED}✗ Tag signature verification failed${NC}"
        exit 1
    fi
fi

# Show tag info
echo ""
echo -e "${GREEN}Tag Information:${NC}"
git show "$VERSION" --no-patch --format="%D%n%H%n%an <%ae>%n%ad%n%s" | head -5

# Ask if user wants to push
echo ""
read -p "Push tag to remote? (Y/n) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Nn]$ ]]; then
    echo -e "${GREEN}Pushing tag '$VERSION' to remote...${NC}"
    if git push origin "$VERSION"; then
        echo -e "${GREEN}✓ Tag pushed successfully${NC}"
        echo ""
        echo -e "${GREEN}Next steps:${NC}"
        echo "1. Create a GitHub Release from this tag:"
        echo "   gh release create $VERSION --title \"Release $VERSION\" --notes \"$MESSAGE\""
        echo ""
        echo "2. Or create via GitHub web interface:"
        echo "   https://github.com/$(git config --get remote.origin.url | sed 's/.*github.com[:/]\(.*\)\.git/\1/')/releases/new"
    else
        echo -e "${RED}✗ Failed to push tag${NC}"
        exit 1
    fi
else
    echo -e "${YELLOW}Tag created locally. Push manually with:${NC}"
    echo "  git push origin $VERSION"
fi

