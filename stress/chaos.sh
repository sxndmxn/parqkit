#!/bin/bash
# Chaos testing - random operation bombardment to find crashes
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
FIXTURES_DIR="$SCRIPT_DIR/fixtures"
PARQKIT="$REPO_ROOT/target/release/parqkit"
ITERATIONS=${1:-1000}

cd "$REPO_ROOT"

if [[ ! "$ITERATIONS" =~ ^[1-9][0-9]*$ ]]; then
    echo "Error: iteration count must be a positive integer"
    exit 2
fi

if [[ ! -f "$PARQKIT" ]]; then
    echo "Building Parqkit in release mode..."
    cargo build --release --locked
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
mapfile -t FILES < <(fd --no-ignore --type f --extension parquet . "$FIXTURES_DIR" --max-results 50)

if [[ ${#FILES[@]} -eq 0 ]]; then
    echo "Error: No parquet files found in $FIXTURES_DIR"
    exit 1
fi

echo "Found ${#FILES[@]} fixture files"
echo ""

crashes=0
errors=0
successes=0

for ((i = 1; i <= ITERATIONS; i++)); do
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

    # Build the command as an argument array so fixture names cannot be
    # reinterpreted by a nested shell.
    case $CMD in
        head|tail)
            COMMAND_ARGS=("$PARQKIT" "$CMD" -n "$ROWS" "$FILE" -o "$FMT")
            ;;
        count)
            COMMAND_ARGS=("$PARQKIT" "$CMD" "$FILE")
            ;;
        *)
            COMMAND_ARGS=("$PARQKIT" "$CMD" "$FILE" -o "$FMT")
            ;;
    esac
    printf -v FULL_CMD '%q ' "${COMMAND_ARGS[@]}"

    # Progress indicator
    if (( i % 100 == 0 )); then
        echo "[$i/$ITERATIONS] $successes ok, $errors err, $crashes crashes"
    fi

    # Execute with timeout
    set +e
    timeout 30 "${COMMAND_ARGS[@]}" > /dev/null 2>&1
    exit_code=$?
    set -e

    case $exit_code in
        0)
            ((successes += 1))
            ;;
        124)
            # Timeout - not necessarily a crash
            echo "TIMEOUT: $FULL_CMD"
            ((errors += 1))
            ;;
        139|134|136|138)
            # SIGSEGV (139), SIGABRT (134), SIGFPE (136), SIGBUS (138)
            echo ""
            echo "!!! CRASH DETECTED !!!"
            echo "Exit code: $exit_code"
            echo "Command: $FULL_CMD"
            echo ""
            ((crashes += 1))
            ;;
        *)
            # Regular error (expected for some edge cases)
            ((errors += 1))
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
