#!/usr/bin/env bash
#
# Configures a BigTable instance with the expected tables
#
cd "$(dirname "$0")/"

set -e
# run emulator and then process
$(gcloud beta emulators bigtable env-init)

./init-bigtable-test.sh

for test in bigtable-test execute_evm_bytecode; do
    
    cargo run -p evm-bigtable-context --bin $test
done;
