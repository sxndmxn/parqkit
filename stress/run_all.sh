#!/bin/bash
# Master stress test runner
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."

echo "=============================================="
echo "       Parqkit Stress Test Suite"
echo "=============================================="
echo ""

# Build release
echo ">>> Building release binary..."
cargo build --release --bin parqkit --bin parqkit-generate
echo ""

# Check if fixtures exist
if [[ ! -d "$SCRIPT_DIR/fixtures" ]] || [[ -z "$(ls -A "$SCRIPT_DIR/fixtures" 2>/dev/null)" ]]; then
    echo ">>> Fixtures not found. Generating..."
    echo ""
    bash "$SCRIPT_DIR/generate.sh"
    echo ""
fi

# Run fast Rust tests (non-ignored)
echo "=============================================="
echo ">>> Running fast Rust tests..."
echo "=============================================="
cargo test --test stress 2>&1 | tail -20
echo ""

# Run ignored (heavy) Rust tests
echo "=============================================="
echo ">>> Running heavy Rust tests (--ignored)..."
echo "=============================================="
cargo test --test stress -- --ignored --test-threads=1 2>&1 | tail -50
echo ""

# Benchmarks
echo "=============================================="
echo ">>> Running benchmarks..."
echo "=============================================="
bash "$SCRIPT_DIR/benchmarks.sh" 2>&1 | tail -100
echo ""

# Memory profiling
echo "=============================================="
echo ">>> Running memory profiling..."
echo "=============================================="
bash "$SCRIPT_DIR/memory_profile.sh" 2>&1
echo ""

# Chaos testing
echo "=============================================="
echo ">>> Running chaos tests (1000 iterations)..."
echo "=============================================="
bash "$SCRIPT_DIR/chaos.sh" 1000
echo ""

echo "=============================================="
echo "       Stress Test Suite Complete"
echo "=============================================="
