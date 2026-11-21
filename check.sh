#!/bin/sh
# check.sh DATASET: run the fast binary and the reference, and diff their tables.
set -eu
cd "$(dirname "$0")"
D=$1; N=$(basename "$D")
[ -d out/ref-$N ] || target/release/reference "$D" out/ref-$N
rm -rf out/fast-$N
target/release/shopsess "$D" out/fast-$N
.venv/bin/python vendor/starter/starter-duckdb/scripts/compare.py out/fast-$N out/ref-$N | grep -E "rows|MATCH|DIFFER"
