#!/bin/bash
# =============================================================================
# Filovesk Scraper - One-Click Setup & Join Script
# Usage: ./setup.sh <REDIS_HOST> [REDIS_PASSWORD]
# =============================================================================

set -e

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${GREEN}"
echo "=============================================="
echo "  Filovesk Distributed Scraper Setup"
echo "=============================================="
echo -e "${NC}"

# Check arguments
REDIS_HOST="${1:-}"
REDIS_PASSWORD="${2:-}"

if [ -z "$REDIS_HOST" ]; then
    echo -e "${YELLOW}Usage: ./setup.sh <REDIS_HOST> [REDIS_PASSWORD]${NC}"
    echo ""
    echo "Examples:"
    echo "  ./setup.sh 192.168.1.100"
    echo "  ./setup.sh redis.example.com mypassword"
    echo ""
    echo "Running in LOCAL mode (no distributed deduplication)..."
    echo ""
fi

# Check Python
if ! command -v python3 &> /dev/null; then
    echo -e "${RED}Python3 not found. Installing...${NC}"
    if command -v apt &> /dev/null; then
        sudo apt update && sudo apt install -y python3 python3-pip
    elif command -v yum &> /dev/null; then
        sudo yum install -y python3 python3-pip
    else
        echo -e "${RED}Please install Python3 manually${NC}"
        exit 1
    fi
fi

echo -e "${GREEN}✓ Python3 found: $(python3 --version)${NC}"

# Install dependencies
echo -e "\n${YELLOW}Installing dependencies...${NC}"
pip3 install -q -r requirements.txt

echo -e "${GREEN}✓ Dependencies installed${NC}"

# Create data directory
mkdir -p data

# Set environment variables
export REDIS_HOST="$REDIS_HOST"
export REDIS_PASSWORD="$REDIS_PASSWORD"

# Generate node ID if not set
if [ -z "$SCRAPER_NODE_ID" ]; then
    export SCRAPER_NODE_ID="node-$(hostname | cut -c1-8)-$(date +%s | tail -c 5)"
fi

echo -e "\n${GREEN}=============================================="
echo "  Configuration"
echo "=============================================="
echo -e "  Node ID:       $SCRAPER_NODE_ID"
if [ -n "$REDIS_HOST" ]; then
    echo -e "  Redis Host:    $REDIS_HOST"
    echo -e "  Mode:          🌐 Distributed"
else
    echo -e "  Mode:          💻 Local (standalone)"
fi
echo -e "==============================================${NC}"

# Start scraper
echo -e "\n${YELLOW}Starting scraper...${NC}\n"

python3 main.py --target 1000000
