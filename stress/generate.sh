#!/bin/bash
# Generate stress test fixtures
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
FIXTURES_DIR="$SCRIPT_DIR/fixtures"
GENERATE=(cargo run --release --locked --features stress-tools --bin parqkit-generate --)

cd "$REPO_ROOT"

mkdir -p "$FIXTURES_DIR"

echo "Building parqkit-generate..."
cargo build --release --locked --features stress-tools --bin parqkit-generate

echo ""
echo "=== Generating stress test fixtures ==="
echo ""

# Baseline (fast)
echo "[1/12] Baseline: 100K rows x 10 cols"
"${GENERATE[@]}" --rows 100000 --cols 10 --profile mixed -o "$FIXTURES_DIR/baseline_100k.parquet"

# Medium
echo "[2/12] Medium: 1M rows x 20 cols"
"${GENERATE[@]}" --rows 1000000 --cols 20 --profile mixed -o "$FIXTURES_DIR/medium_1m.parquet"

# Large
echo "[3/12] Large: 10M rows x 20 cols"
"${GENERATE[@]}" --rows 10000000 --cols 20 --profile mixed -o "$FIXTURES_DIR/large_10m.parquet"

# Huge (extreme)
echo "[4/12] Huge: 100M rows x 20 cols (this will take a while...)"
"${GENERATE[@]}" --rows 100000000 --cols 20 --profile mixed --batch-size 131072 -o "$FIXTURES_DIR/huge_100m.parquet"

# Wide schema
echo "[5/12] Wide: 100K rows x 500 cols"
"${GENERATE[@]}" --rows 100000 --cols 500 --profile mixed -o "$FIXTURES_DIR/wide_500col.parquet"

echo "[6/12] Very Wide: 10K rows x 1000 cols"
"${GENERATE[@]}" --rows 10000 --cols 1000 --profile integers -o "$FIXTURES_DIR/wide_1000col.parquet"

# Sparse (90% nulls)
echo "[7/12] Sparse: 10M rows x 20 cols (90% nulls)"
"${GENERATE[@]}" --rows 10000000 --cols 20 --profile sparse -o "$FIXTURES_DIR/sparse_10m.parquet"

# String-heavy
echo "[8/12] Long Strings: 1M rows x 10 cols (1KB strings)"
"${GENERATE[@]}" --rows 1000000 --cols 10 --profile long-strings -o "$FIXTURES_DIR/long_strings_1m.parquet"

# Unicode
echo "[9/12] Unicode: 100K rows x 10 cols"
"${GENERATE[@]}" --rows 100000 --cols 10 --profile unicode -o "$FIXTURES_DIR/unicode_100k.parquet"

# Edge cases
echo "[10/12] Edge Cases: 100K rows x 10 cols"
"${GENERATE[@]}" --rows 100000 --cols 10 --profile edge-cases -o "$FIXTURES_DIR/edge_cases_100k.parquet"

# Empty file
echo "[11/12] Empty: 0 rows"
"${GENERATE[@]}" --rows 0 --cols 10 --profile empty -o "$FIXTURES_DIR/empty.parquet"

# Many small files for glob testing
echo "[12/12] Many Files: 1000 files x 1K rows each"
mkdir -p "$FIXTURES_DIR/many"
for i in $(seq -w 0 999); do
    "${GENERATE[@]}" --rows 1000 --cols 5 --profile mixed --seed "$i" -o "$FIXTURES_DIR/many/part_$i.parquet" 2>/dev/null &

    # Limit parallelism
    if (( $(jobs -r | wc -l) >= 8 )); then
        wait -n
    fi
done
wait

echo ""
echo "=== Generation complete ==="
echo ""
dust -d 1 "$FIXTURES_DIR"
eza -lh --icons "$FIXTURES_DIR"
