#!/bin/bash
# bench.sh BINARY DATASET [RUNS]: wall time of the whole process, min and median.
B=$1; D=$2; N=${3:-9}; T=()
for i in $(seq $N); do
  rm -rf out/bench; s=$(date +%s%N)
  NCPU=8 SHOPSESS_QUIET=1 taskset -c 0-7 $B $D out/bench 2>/dev/null
  e=$(date +%s%N); T+=($(( (e-s)/1000000 )))
done
S=($(printf '%s\n' "${T[@]}" | sort -n)); echo "min ${S[0]} ms  median ${S[$((N/2))]} ms  (${S[*]})"
