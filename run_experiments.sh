#!/bin/bash
# Script to run KTP experiments for varying values of p (0.05 to 0.50 in steps of 0.05)
# Measures:
#   - Total unique data messages generated from the file (>100KB)
#   - Total UDP transmissions made (initial + retransmissions)
#   - Average number of transmissions per message

set -e

echo "=========================================================="
echo "    KTP Automated Test & Experiment Suite (p: 0.05 - 0.50)"
echo "=========================================================="

# Build all binaries
make clean
make all

TEST_FILE="test_input.txt"
OUTPUT_FILE="test_output.txt"
RESULTS_FILE="experiment_results.txt"

# Ensure 128KB test file exists
./user1 "$TEST_FILE" 8081 8082 2>/dev/null || true

echo "Drop Probability (p) | Messages Sent | Total Transmissions | Avg Transmissions / Msg" > "$RESULTS_FILE"
echo "---------------------+---------------+---------------------+------------------------" >> "$RESULTS_FILE"

P_VALUES=(0.05 0.10 0.15 0.20 0.25 0.30 0.35 0.40 0.45 0.50)

for p in "${P_VALUES[@]}"; do
    echo ""
    echo ">>> Running test for p = $p ..."
    
    # Clean any leftover IPC shared memory from previous runs
    ipcrm -M 0x4B545031 2>/dev/null || true
    rm -f "$OUTPUT_FILE"

    # Start initksocket daemon in background with drop probability p
    ./initksocket "$p" > /dev/null 2>&1 &
    INIT_PID=$!
    sleep 1

    # Start user2 (Receiver) in background
    ./user2 "$OUTPUT_FILE" 8082 8081 > user2_out.log 2>&1 &
    USER2_PID=$!
    sleep 1

    # Run user1 (Sender) in foreground and capture log
    ./user1 "$TEST_FILE" 8081 8082 > user1_out.log 2>&1 || true

    # Wait for user2 to finish
    wait "$USER2_PID" 2>/dev/null || true

    # Terminate initksocket daemon
    kill -SIGINT "$INIT_PID" 2>/dev/null || true
    wait "$INIT_PID" 2>/dev/null || true

    # Parse statistics from user1_out.log
    TOTAL_MSGS=$(grep "Total Application Messages Generated:" user1_out.log | awk '{print $NF}' || echo "0")
    TOTAL_TRANS=$(grep "Total UDP Transmissions Made:" user1_out.log | awk '{print $NF}' || echo "0")
    AVG_TRANS=$(grep "Average Transmissions Per Message:" user1_out.log | awk '{print $NF}' || echo "0")

    # Verify integrity
    DIFF_COUNT=$(diff -u "$TEST_FILE" "$OUTPUT_FILE" 2>/dev/null | wc -l || echo "1")
    if [ "$DIFF_COUNT" -eq 0 ]; then
        STATUS="PASSED (Integrity OK)"
    else
        STATUS="FAILED (Mismatch)"
    fi

    printf "  p = %-4s | Msgs: %-4s | Transmissions: %-5s | Avg: %-6s | %s\n" \
           "$p" "$TOTAL_MSGS" "$TOTAL_TRANS" "$AVG_TRANS" "$STATUS"

    printf "%-20s | %-13s | %-19s | %-23s\n" \
           "$p" "$TOTAL_MSGS" "$TOTAL_TRANS" "$AVG_TRANS" >> "$RESULTS_FILE"
done

echo ""
echo "=========================================================="
echo "Experiments completed. Results recorded in $RESULTS_FILE:"
echo "=========================================================="
cat "$RESULTS_FILE"

