#!/usr/bin/env bash
set -euo pipefail

# Define colors for output visibility (standard ANSI)
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

cd "$(git rev-parse --show-toplevel)"
echo -e "${BLUE}>>> Starting Local CI Pipeline for Chapaty...${NC}"

# ==============================================================================
# JOB 1: Compliance & Security
# ==============================================================================

echo -e "\n${YELLOW}[1/10] Checking Security (Secrets)...${NC}"
# Check if .cargo/config.toml is tracked by git
if git ls-files --error-unmatch .cargo/config.toml > /dev/null 2>&1; then
    echo -e "${RED}[FAIL] CRITICAL: .cargo/config.toml is being tracked by git! Remove it immediately.${NC}"
    exit 1
fi
echo -e "${GREEN}[OK] No leaked secrets in git index.${NC}"

echo -e "\n${YELLOW}[2/10] Checking Formatting...${NC}"
# Fails if code is not formatted. Remove '--check' to auto-format instead.
cargo fmt -- --check || { echo -e "${RED}[FAIL] Formatting invalid. Run 'cargo fmt' to fix.${NC}"; exit 1; }
echo -e "${GREEN}[OK] Formatting is correct.${NC}"

echo -e "\n${YELLOW}[3/10] Checking Architecture Guardrails...${NC}"
# Prevent circular dependencies via prelude imports within the library
if grep -r "use crate::prelude::" src/; then
    echo -e "${RED}[FAIL] Architecture violation: Internal imports from 'crate::prelude' found.${NC}"
    exit 1
fi
echo -e "${GREEN}[OK] Architecture compliant.${NC}"

# ==============================================================================
# JOB 2: Build, Test & Verify
# ==============================================================================

echo -e "\n${YELLOW}[4/10] Security Audit (Dependencies)...${NC}"
# Check if cargo-audit is installed
if ! command -v cargo-audit &> /dev/null; then
    echo -e "${RED}[FAIL] 'cargo-audit' is not installed.${NC}"
    echo "       Please run: cargo install cargo-audit"
    exit 1
fi
cargo audit
echo -e "${GREEN}[OK] Dependencies audited.${NC}"

echo -e "\n${YELLOW}[5/10] Linting (Clippy)...${NC}"
# Deny warnings to match CI strictness
cargo clippy --all-targets --all-features -- -D warnings
echo -e "${GREEN}[OK] Code is clean.${NC}"

echo -e "\n${YELLOW}[6/10] Building Workspace...${NC}"
cargo build --all-features
echo -e "${GREEN}[OK] Workspace compiled successfully.${NC}"

echo -e "\n${YELLOW}[7/10] Running Unit Tests...${NC}"
cargo test --all-features
echo -e "${GREEN}[OK] All tests passed.${NC}"

echo -e "\n${YELLOW}[8/10] Verifying Documentation...${NC}"
# Ensure documentation builds without warnings (broken links, etc.)
export RUSTDOCFLAGS="-D warnings"
cargo doc --no-deps --document-private-items
echo -e "${GREEN}[OK] Documentation builds successfully.${NC}"

echo -e "\n${YELLOW}[9/10] Verifying Docs.rs Compatibility (Nightly)...${NC}"
# docs.rs strictly uses the nightly compiler. We run a soft-fail check here.
if rustup toolchain list | grep -q nightly; then
    # We suppress stdout to keep it clean, but let stderr show if it fails.
    # We inline RUSTDOCFLAGS="" to override the strict warnings exported in Step 8.
    if env RUSTDOCFLAGS="" cargo +nightly doc --no-deps > /dev/null 2>&1; then
        echo -e "${GREEN}[OK] Nightly docs build successfully.${NC}"
    else
        echo -e "${YELLOW}[WARN] Nightly docs build failed!${NC}"
        echo -e "${YELLOW}       Your code compiles on Stable, but your docs.rs page will likely fail.${NC}"
        echo -e "${YELLOW}       This is usually an upstream dependency breaking on Nightly.${NC}"
        echo -e "${YELLOW}       -> Pipeline continuing because Stable is intact.${NC}"
    fi
else
    echo -e "${BLUE}[SKIP] Nightly toolchain not installed.${NC}"
    echo -e "${BLUE}       Run 'rustup toolchain install nightly' to enable docs.rs dry-runs.${NC}"
fi

# ==============================================================================
# Build & Dry-Run the Quickstart Example
# ==============================================================================

echo -e "\n${YELLOW}[10/10] Building & Dry-Running Quickstart Example...${NC}"
# Compile first so a build error is distinct from a runtime error.
cargo build --example quickstart
# Then run it to verify the full logic path (environment load, eval, export) works.
cargo run --release --example quickstart > /dev/null
echo -e "${GREEN}[OK] Quickstart example ran successfully.${NC}"

echo -e "\n${GREEN}>>> SUCCESS! All checks passed. Ready to push.${NC}"
