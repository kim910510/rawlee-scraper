#!/bin/bash
# =============================================================================
# Filovesk Scraper - One-Click Bootsrap
# Usage: curl -sL url/bootstrap.sh | bash -s 149.104.78.154
# =============================================================================

set -e

REDIS_HOST="${1:-149.104.78.154}"
REPO_URL="https://github.com/kim910510/rawlee-scraper.git"
DIR_NAME="rawlee-scraper"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${GREEN}🚀 Initializing Filovesk Scraper Node...${NC}"

# 1. Install System Dependencies
echo -e "\n${YELLOW}📦 Installing system packages...${NC}"
if command -v apt &> /dev/null; then
    sudo apt update -qq && sudo apt install -y -qq python3-pip git
elif command -v yum &> /dev/null; then
    sudo yum install -y python3-pip git
fi

# 2. Get Code
echo -e "\n${YELLOW}📥 Fetching code...${NC}"
if [ -d "$DIR_NAME" ]; then
    echo "Updating existing directory..."
    cd "$DIR_NAME"
    pkill -f "python3 main.py" || true
    git pull
else
    git clone "$REPO_URL" "$DIR_NAME"
    cd "$DIR_NAME"
fi

# 3. Install Python Dependencies
echo -e "\n${YELLOW}🐍 Installing Python requirements...${NC}"
pip3 install -q -r requirements.txt

# 4. Set Permission
chmod +x watchdog.sh

# 5. Start Watchdog
echo -e "\n${GREEN}🔥 Starting Scraper (Auto-Restart Mode)...${NC}"
echo "   Redis Host: $REDIS_HOST"
echo "   Node ID:    Auto-generated (IP-based)"

# Run watchdog in background if needed, but for initial setup usually foreground to see output
./watchdog.sh "$REDIS_HOST"
