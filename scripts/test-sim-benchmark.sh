#!/bin/bash
set -ueo pipefail

# Purpose: This script executes uplink upload and download benchmark tests against storj-sim.
# Setup: Remove any existing uplink configs.
# Usage: from root of storj repo, run
#   $ storj-sim network test bash ./scripts/test-sim-benchmark.sh
# To run and filter out storj-sim logs, run:
#   $ storj-sim -x network test bash ./scripts/test-sim-benchmark.sh | grep -i "test.out"

access=$(storj-sim network env GATEWAY_0_ACCESS)
export ACCESS=$(storj-sim network env GATEWAY_0_ACCESS)
echo "ACCESS:"
echo "$access"

aws_access_key=$(storj-sim network env GATEWAY_0_ACCESS_KEY)
export AWS_ACCESS_KEY_ID=$aws_access_key
echo "AWS_ACCESS_KEY_ID: $aws_access_key"

aws_secret_key=$(storj-sim network env GATEWAY_0_SECRET_KEY)
export AWS_SECRET_ACCESS_KEY=$aws_secret_key
echo "AWS_SECRET_ACCESS_KEY: $aws_secret_key"

# run benchmark tests
echo
echo "Executing benchmark tests with uplink client against storj-sim..."
go test -bench=. -benchmem ./internal/bench/

# run s3-benchmark with uplink
echo
echo "Executing s3-benchmark tests with uplink client against storj-sim..."
s3-benchmark --client=uplink --access="$access"
