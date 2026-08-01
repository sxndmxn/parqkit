#!/bin/bash
# Benchmark Parqkit commands with hyperfine
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FIXTURES_DIR="$SCRIPT_DIR/fixtures"
PARQKIT="./target/release/parqkit"

# Check dependencies
if ! command -v hyperfine &> /dev/null; then
    echo "Error: hyperfine not found. Install with: brew install hyperfine"
    exit 1
fi

if [[ ! -f "$PARQKIT" ]]; then
    echo "Building Parqkit in release mode..."
    cargo build --release
fi

if [[ ! -d "$FIXTURES_DIR" ]]; then
    echo "Error: Fixtures not found. Run ./stress/generate.sh first"
    exit 1
fi

echo "=== Parqkit Benchmark Suite ==="
echo ""

# ============================================================================
echo ">>> COUNT (should use metadata only, instant)"
echo ""

if [[ -f "$FIXTURES_DIR/baseline_100k.parquet" ]]; then
    hyperfine --warmup 5 --runs 20 \
        "$PARQKIT count $FIXTURES_DIR/baseline_100k.parquet" \
        --export-markdown "$FIXTURES_DIR/../results/count_100k.md" 2>/dev/null || \
    hyperfine --warmup 5 --runs 20 "$PARQKIT count $FIXTURES_DIR/baseline_100k.parquet"
fi

if [[ -f "$FIXTURES_DIR/huge_100m.parquet" ]]; then
    echo ""
    hyperfine --warmup 3 --runs 10 \
        "$PARQKIT count $FIXTURES_DIR/huge_100m.parquet"
fi

# ============================================================================
echo ""
echo ">>> HEAD (streaming, should be fast regardless of file size)"
echo ""

if [[ -f "$FIXTURES_DIR/baseline_100k.parquet" ]]; then
    hyperfine --warmup 5 \
        "$PARQKIT head -n 10 $FIXTURES_DIR/baseline_100k.parquet" \
        "$PARQKIT head -n 100 $FIXTURES_DIR/baseline_100k.parquet" \
        "$PARQKIT head -n 1000 $FIXTURES_DIR/baseline_100k.parquet"
fi

if [[ -f "$FIXTURES_DIR/huge_100m.parquet" ]]; then
    echo ""
    echo "Head on 100M row file:"
    hyperfine --warmup 3 --runs 5 \
        "$PARQKIT head -n 10 $FIXTURES_DIR/huge_100m.parquet" \
        "$PARQKIT head -n 1000 $FIXTURES_DIR/huge_100m.parquet" \
        "$PARQKIT head -n 10000 $FIXTURES_DIR/huge_100m.parquet"
fi

# ============================================================================
echo ""
echo ">>> TAIL (full file scan required)"
echo ""

if [[ -f "$FIXTURES_DIR/medium_1m.parquet" ]]; then
    hyperfine --warmup 2 --runs 5 \
        "$PARQKIT tail -n 10 $FIXTURES_DIR/medium_1m.parquet"
fi

if [[ -f "$FIXTURES_DIR/large_10m.parquet" ]]; then
    echo ""
    echo "Tail on 10M row file:"
    hyperfine --warmup 1 --runs 3 \
        "$PARQKIT tail -n 10 $FIXTURES_DIR/large_10m.parquet"
fi

# ============================================================================
echo ""
echo ">>> SCHEMA (metadata only)"
echo ""

if [[ -f "$FIXTURES_DIR/wide_1000col.parquet" ]]; then
    hyperfine --warmup 5 \
        "$PARQKIT schema $FIXTURES_DIR/wide_1000col.parquet"
fi

# ============================================================================
echo ""
echo ">>> STATS (row group iteration)"
echo ""

if [[ -f "$FIXTURES_DIR/medium_1m.parquet" ]]; then
    hyperfine --warmup 2 --runs 5 \
        "$PARQKIT stats $FIXTURES_DIR/medium_1m.parquet"
fi

if [[ -f "$FIXTURES_DIR/large_10m.parquet" ]]; then
    echo ""
    hyperfine --warmup 1 --runs 3 \
        "$PARQKIT stats $FIXTURES_DIR/large_10m.parquet"
fi

# ============================================================================
echo ""
echo ">>> INFO (metadata only)"
echo ""

if [[ -f "$FIXTURES_DIR/huge_100m.parquet" ]]; then
    hyperfine --warmup 5 \
        "$PARQKIT info $FIXTURES_DIR/huge_100m.parquet"
fi

# ============================================================================
echo ""
echo ">>> QUERY (DataFusion SQL engine)"
echo ""

if [[ -f "$FIXTURES_DIR/large_10m.parquet" ]]; then
    hyperfine --warmup 2 --runs 5 \
        "$PARQKIT count $FIXTURES_DIR/large_10m.parquet"

    echo ""
    hyperfine --warmup 2 --runs 3 \
        "$PARQKIT tail -n 1000 $FIXTURES_DIR/large_10m.parquet"
fi

# ============================================================================
echo ""
echo ">>> OUTPUT FORMATS"
echo ""

if [[ -f "$FIXTURES_DIR/baseline_100k.parquet" ]]; then
    hyperfine --warmup 3 \
        "$PARQKIT head -n 10000 $FIXTURES_DIR/baseline_100k.parquet -o table > /dev/null" \
        "$PARQKIT head -n 10000 $FIXTURES_DIR/baseline_100k.parquet -o json > /dev/null" \
        "$PARQKIT head -n 10000 $FIXTURES_DIR/baseline_100k.parquet -o jsonl > /dev/null" \
        "$PARQKIT head -n 10000 $FIXTURES_DIR/baseline_100k.parquet -o csv > /dev/null"
fi

# ============================================================================
echo ""
echo ">>> GLOB PATTERNS"
echo ""

if [[ -d "$FIXTURES_DIR/many" ]]; then
    hyperfine --warmup 2 --runs 5 \
        "$PARQKIT count '$FIXTURES_DIR/many/*.parquet'"
fi

echo ""
echo "=== Benchmark complete ==="
