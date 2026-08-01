#!/bin/bash
# Memory profiling for Parqkit commands
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FIXTURES_DIR="$SCRIPT_DIR/fixtures"
PARQKIT="./target/release/parqkit"
RESULTS_DIR="$SCRIPT_DIR/results"

mkdir -p "$RESULTS_DIR"

if [[ ! -f "$PARQKIT" ]]; then
    echo "Building Parqkit in release mode..."
    cargo build --release
fi

if [[ ! -d "$FIXTURES_DIR" ]]; then
    echo "Error: Fixtures not found. Run ./stress/generate.sh first"
    exit 1
fi

echo "=== Memory Profiling ==="
echo ""

profile_command() {
    local desc="$1"
    shift
    local cmd="$@"

    echo ">>> $desc"
    echo "    Command: $cmd"

    # macOS uses different time format
    if [[ "$(uname)" == "Darwin" ]]; then
        /usr/bin/time -l $cmd > /dev/null 2>&1 || true
        result=$(/usr/bin/time -l $cmd 2>&1 > /dev/null || true)
        peak_mem=$(echo "$result" | grep "maximum resident set size" | awk '{print $1}')
        if [[ -n "$peak_mem" ]]; then
            peak_mb=$((peak_mem / 1048576))
            echo "    Peak RSS: ${peak_mb} MB"
        else
            # Try running again with output capture
            /usr/bin/time -l $cmd 2>&1 | grep -E "(maximum resident|real|user|sys)" || echo "    (timing data unavailable)"
        fi
    else
        # Linux
        /usr/bin/time -v $cmd 2>&1 | grep -E "(Maximum resident|Elapsed)" || echo "    (timing data unavailable)"
    fi
    echo ""
}

# ============================================================================
echo "--- HEAD (streaming, should be low memory) ---"
echo ""

if [[ -f "$FIXTURES_DIR/huge_100m.parquet" ]]; then
    profile_command "Head 10 rows from 100M row file" \
        $PARQKIT head -n 10 "$FIXTURES_DIR/huge_100m.parquet"

    profile_command "Head 10000 rows from 100M row file" \
        $PARQKIT head -n 10000 "$FIXTURES_DIR/huge_100m.parquet"
fi

# ============================================================================
echo "--- TAIL (full scan, higher memory) ---"
echo ""

if [[ -f "$FIXTURES_DIR/medium_1m.parquet" ]]; then
    profile_command "Tail 10 rows from 1M row file" \
        $PARQKIT tail -n 10 "$FIXTURES_DIR/medium_1m.parquet"
fi

if [[ -f "$FIXTURES_DIR/large_10m.parquet" ]]; then
    profile_command "Tail 10 rows from 10M row file" \
        $PARQKIT tail -n 10 "$FIXTURES_DIR/large_10m.parquet"
fi

# ============================================================================
echo "--- COUNT (metadata only, minimal memory) ---"
echo ""

if [[ -f "$FIXTURES_DIR/huge_100m.parquet" ]]; then
    profile_command "Count 100M row file" \
        $PARQKIT count "$FIXTURES_DIR/huge_100m.parquet"
fi

# ============================================================================
echo "--- STATS (row group iteration) ---"
echo ""

if [[ -f "$FIXTURES_DIR/large_10m.parquet" ]]; then
    profile_command "Stats on 10M row file" \
        $PARQKIT stats "$FIXTURES_DIR/large_10m.parquet"
fi

# ============================================================================
echo "--- QUERY (DataFusion memory) ---"
echo ""

if [[ -f "$FIXTURES_DIR/large_10m.parquet" ]]; then
    profile_command "Simple aggregation on 10M rows" \
        $PARQKIT count "$FIXTURES_DIR/large_10m.parquet"

    profile_command "Group by on 10M rows" \
        $PARQKIT tail -n 1000 "$FIXTURES_DIR/large_10m.parquet"
fi

# ============================================================================
echo "--- JSON OUTPUT (string building memory) ---"
echo ""

if [[ -f "$FIXTURES_DIR/medium_1m.parquet" ]]; then
    profile_command "JSON output 100K rows" \
        $PARQKIT head -n 100000 "$FIXTURES_DIR/medium_1m.parquet" -o json
fi

# ============================================================================
echo "--- WIDE SCHEMA ---"
echo ""

if [[ -f "$FIXTURES_DIR/wide_1000col.parquet" ]]; then
    profile_command "Head from 1000-column file" \
        $PARQKIT head -n 100 "$FIXTURES_DIR/wide_1000col.parquet"

    profile_command "Schema of 1000-column file" \
        $PARQKIT schema "$FIXTURES_DIR/wide_1000col.parquet"
fi

echo "=== Memory profiling complete ==="
