#!/bin/bash
# Chaos testing - random operation bombardment to find crashes
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FIXTURES_DIR="$SCRIPT_DIR/fixtures"
PARQKIT="./target/release/parqkit"
ITERATIONS=${1:-1000}

if [[ ! -f "$PARQKIT" ]]; then
    echo "Building Parqkit in release mode..."
    cargo build --release
fi

if [[ ! -d "$FIXTURES_DIR" ]]; then
    echo "Error: Fixtures not found. Run ./stress/generate.sh first"
    exit 1
fi

echo "=== Chaos Testing ==="
echo "Running $ITERATIONS random operations..."
echo ""

COMMANDS=("head" "tail" "count" "stats" "schema" "info")
FORMATS=("table" "json" "jsonl" "csv")

# Collect fixture files
mapfile -t FILES < <(find "$FIXTURES_DIR" -name "*.parquet" -type f 2>/dev/null | head -50)

if [[ ${#FILES[@]} -eq 0 ]]; then
    echo "Error: No parquet files found in $FIXTURES_DIR"
    exit 1
fi

echo "Found ${#FILES[@]} fixture files"
echo ""

crashes=0
errors=0
successes=0

for i in $(seq 1 $ITERATIONS); do
    # Random command
    cmd_idx=$((RANDOM % ${#COMMANDS[@]}))
    CMD=${COMMANDS[$cmd_idx]}

    # Random file
    file_idx=$((RANDOM % ${#FILES[@]}))
    FILE="${FILES[$file_idx]}"

    # Random format
    fmt_idx=$((RANDOM % ${#FORMATS[@]}))
    FMT=${FORMATS[$fmt_idx]}

    # Random row count for head/tail
    ROWS=$((RANDOM % 1000 + 1))

    # Build command
    case $CMD in
        head|tail)
            FULL_CMD="$PARQKIT $CMD -n $ROWS \"$FILE\" -o $FMT"
            ;;
        *)
            FULL_CMD="$PARQKIT $CMD \"$FILE\" -o $FMT"
            ;;
    esac

    # Progress indicator
    if (( i % 100 == 0 )); then
        echo "[$i/$ITERATIONS] $successes ok, $errors err, $crashes crashes"
    fi

    # Execute with timeout
    set +e
    timeout 30 bash -c "$FULL_CMD" > /dev/null 2>&1
    exit_code=$?
    set -e

    case $exit_code in
        0)
            ((successes++))
            ;;
        124)
            # Timeout - not necessarily a crash
            echo "TIMEOUT: $FULL_CMD"
            ((errors++))
            ;;
        139|134|136|138)
            # SIGSEGV (139), SIGABRT (134), SIGFPE (136), SIGBUS (138)
            echo ""
            echo "!!! CRASH DETECTED !!!"
            echo "Exit code: $exit_code"
            echo "Command: $FULL_CMD"
            echo ""
            ((crashes++))
            ;;
        *)
            # Regular error (expected for some edge cases)
            ((errors++))
            ;;
    esac
done

echo ""
echo "=== Chaos Test Results ==="
echo "Total iterations: $ITERATIONS"
echo "Successes: $successes"
echo "Errors: $errors (expected for edge cases)"
echo "CRASHES: $crashes"
echo ""

if [[ $crashes -gt 0 ]]; then
    echo "FAILED: $crashes crashes detected!"
    exit 1
else
    echo "PASSED: No crashes detected"
    exit 0
fi
