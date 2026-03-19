#!/bin/bash
# ============================================
# Setup Local Python Virtual Environment
# ============================================
# Creates a venv for local development (IDE autocomplete, linting)
# Execution still happens in Docker containers

set -e  # Exit on error

GREEN='\033[0;32m'
YELLOW='\033[0;33m'
RED='\033[0;31m'
RESET='\033[0m'

echo -e "${GREEN}========================================${RESET}"
echo -e "${GREEN}Setting up local development environment${RESET}"
echo -e "${GREEN}========================================${RESET}"
echo ""

# Check Python version
echo -e "${YELLOW}Checking Python version...${RESET}"
if ! command -v python3 &> /dev/null; then
    echo -e "${RED}✗ Python 3 not found! Please install Python 3.11+${RESET}"
    exit 1
fi

PYTHON_VERSION=$(python3 --version | cut -d' ' -f2 | cut -d'.' -f1,2)
echo -e "${GREEN}✓ Found Python $PYTHON_VERSION${RESET}"

# Create virtual environment
echo ""
echo -e "${YELLOW}Creating virtual environment...${RESET}"
if [ -d ".venv" ]; then
    echo -e "${YELLOW}⚠ .venv already exists. Removing...${RESET}"
    rm -rf .venv
fi

python3 -m venv .venv
echo -e "${GREEN}✓ Virtual environment created${RESET}"

# Activate venv
echo ""
echo -e "${YELLOW}Activating virtual environment...${RESET}"
source .venv/bin/activate
echo -e "${GREEN}✓ Virtual environment activated${RESET}"

# Upgrade pip
echo ""
echo -e "${YELLOW}Upgrading pip...${RESET}"
pip install --upgrade pip setuptools wheel > /dev/null
echo -e "${GREEN}✓ pip upgraded${RESET}"

# Install production dependencies
echo ""
echo -e "${YELLOW}Installing production dependencies...${RESET}"
pip install -r requirements.txt
echo -e "${GREEN}✓ Production dependencies installed${RESET}"

# Install development dependencies
echo ""
echo -e "${YELLOW}Installing development dependencies...${RESET}"
pip install -r requirements-dev.txt
echo -e "${GREEN}✓ Development dependencies installed${RESET}"

# Summary
echo ""
echo -e "${GREEN}========================================${RESET}"
echo -e "${GREEN}Setup complete!${RESET}"
echo -e "${GREEN}========================================${RESET}"
echo ""
echo -e "To activate the virtual environment:"
echo -e "  ${YELLOW}source .venv/bin/activate${RESET}"
echo ""
echo -e "Development workflow:"
echo -e "  1. Activate venv: ${YELLOW}source .venv/bin/activate${RESET}"
echo -e "  2. Open IDE: ${YELLOW}code .${RESET}"
echo -e "  3. Run Airflow: ${YELLOW}make start${RESET} (uses Docker)"
echo -e "  4. Local tests: ${YELLOW}pytest tests/${RESET}"
echo -e "  5. Format code: ${YELLOW}black airflow/${RESET}"
echo ""
echo -e "${GREEN}Note: Airflow execution still happens in Docker containers${RESET}"
echo -e "${GREEN}The venv is only for IDE autocomplete and local linting${RESET}"
