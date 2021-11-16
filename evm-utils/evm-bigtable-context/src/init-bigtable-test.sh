#!/usr/bin/env bash
#
# Configures a BigTable instance with the expected tables
#

set -e

for instance in insert_and_find_account_bt_first insert_and_find_account_bt_second insert_and_find_account_bt_evm; do
    cbt=(
        cbt
        -instance
        "$instance"
    )
    if [[ -n $BIGTABLE_EMULATOR_HOST ]]; then
        cbt+=(-project emulator)
    fi
    
    for table in evm-accounts evm-account-storage evm-account-code; do
        (
            set -x
            "${cbt[@]}" createtable $table
            "${cbt[@]}" createfamily $table x
            "${cbt[@]}" setgcpolicy $table x maxversions=1
            "${cbt[@]}" setgcpolicy $table x maxage=360d
        )
    done
done