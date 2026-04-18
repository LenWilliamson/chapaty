#!/bin/bash
set -e # Exit immediately if a command exits with a non-zero status.

# Define colors for output visibility (standard ANSI)
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}>>> Starting Local CI Pipeline for Chapaty...${NC}"

# ==============================================================================
# JOB 1: Compliance & Security
# ==============================================================================

echo -e "\n${YELLOW}[1/8] Checking Security (Secrets)...${NC}"
# Check if .cargo/config.toml is tracked by git
if git ls-files --error-unmatch .cargo/config.toml > /dev/null 2>&1; then
    echo -e "${RED}[FAIL] CRITICAL: .cargo/config.toml is being tracked by git! Remove it immediately.${NC}"
    exit 1
fi
echo -e "${GREEN}[OK] No leaked secrets in git index.${NC}"

echo -e "\n${YELLOW}[2/8] Checking Formatting...${NC}"
# Fails if code is not formatted. Remove '--check' to auto-format instead.
cargo fmt -- --check || { echo -e "${RED}[FAIL] Formatting invalid. Run 'cargo fmt' to fix.${NC}"; exit 1; }
echo -e "${GREEN}[OK] Formatting is correct.${NC}"

echo -e "\n${YELLOW}[3/8] Checking Architecture Guardrails...${NC}"
# Prevent circular dependencies via prelude imports within the library
if grep -r "use crate::prelude::" src/; then
    echo -e "${RED}[FAIL] Architecture violation: Internal imports from 'crate::prelude' found.${NC}"
    exit 1
fi
echo -e "${GREEN}[OK] Architecture compliant.${NC}"

# ==============================================================================
# JOB 2: Build, Test & Verify
# ==============================================================================

echo -e "\n${YELLOW}[4/8] Security Audit (Dependencies)...${NC}"
# Check if cargo-audit is installed
if ! command -v cargo-audit &> /dev/null; then
    echo -e "${RED}[FAIL] 'cargo-audit' is not installed.${NC}"
    echo "       Please run: cargo install cargo-audit"
    exit 1
fi
cargo audit
echo -e "${GREEN}[OK] Dependencies audited.${NC}"

echo -e "\n${YELLOW}[5/8] Linting (Clippy)...${NC}"
# Deny warnings to match CI strictness
cargo clippy --all-targets -- -D warnings
echo -e "${GREEN}[OK] Code is clean.${NC}"

echo -e "\n${YELLOW}[6/8] Running Unit Tests...${NC}"
cargo test
echo -e "${GREEN}[OK] All tests passed.${NC}"

echo -e "\n${YELLOW}[7/8] Verifying Documentation...${NC}"
# Ensure documentation builds without warnings (broken links, etc.)
export RUSTDOCFLAGS="-D warnings"
cargo doc --no-deps --document-private-items
echo -e "${GREEN}[OK] Documentation builds successfully.${NC}"

# ==============================================================================
# NEW STEP: Run Examples (excluding grids)
# ==============================================================================

echo -e "\n${YELLOW}[8/8] Running Examples (skipping *grid.rs)...${NC}"

# Iterate over all .rs files in the examples directory
for file in examples/*.rs; do
    # 1. Extract filename (e.g., "news_breakout_grid.rs")
    filename=$(basename "$file")
    
    # 2. Extract example name (remove .rs extension)
    example_name="${filename%.*}"

    # 3. Filter: Check if filename contains "grid.rs"
    if [[ "$filename" == *"grid.rs"* ]]; then
        echo -e "${BLUE}[SKIP] Long-running example: $example_name${NC}"
        continue
    fi

    echo -ne "       Running example: $example_name ... "
    
    # 4. Run the example
    # Redirect stdout to /dev/null to keep terminal clean, but keep stderr for errors.
    if cargo run --example "$example_name" > /dev/null; then
         echo -e "${GREEN}[PASS]${NC}"
    else
         echo -e "${RED}[FAIL]${NC}"
         # Exit immediately if an example fails
         exit 1
    fi
done

echo -e "\n${GREEN}>>> SUCCESS! All checks passed. Ready to push.${NC}"