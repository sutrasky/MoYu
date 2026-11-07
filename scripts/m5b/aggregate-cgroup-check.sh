#!/usr/bin/env bash
# Read-only verification of the single M5-B memory boundary. It does not create,
# move or restart any service.
set -euo pipefail

SLICE=${M5B_SLICE:-/sys/fs/cgroup/moyu-m5b.slice}
HARD_BYTES=${M5B_HARD_LIMIT_BYTES:-8589934592}
FAIL=0

if [[ ! -d "$SLICE" ]]; then
  echo "missing slice: $SLICE" >&2
  exit 1
fi

limit=$(cat "$SLICE/memory.max" 2>/dev/null || echo max)
if [[ "$limit" == "max" ]]; then
  echo "slice has no enforceable memory.max" >&2
  FAIL=1
elif (( limit > HARD_BYTES )); then
  echo "slice memory.max=$limit exceeds the $HARD_BYTES byte hard boundary" >&2
  FAIL=1
else
  echo "slice memory.max=$limit (<= $HARD_BYTES)"
fi

current=$(cat "$SLICE/memory.current" 2>/dev/null || echo unknown)
echo "slice memory.current=$current"
echo "slice memory.events:"; cat "$SLICE/memory.events" 2>/dev/null || true
echo "host MemAvailable: $(awk '/MemAvailable/{print $2" "$3}' /proc/meminfo)"

echo "member cgroups:"
find "$SLICE" -name cgroup.procs -print 2>/dev/null | while read -r procs; do
  dir=$(dirname "$procs")
  [[ "$dir" == "$SLICE" ]] && continue
  pids=$(tr '\n' ' ' < "$procs" 2>/dev/null)
  mem=$(cat "$dir/memory.current" 2>/dev/null || echo unknown)
  echo "  ${dir#$SLICE/} pids=[$pids] memory.current=$mem"
done

exit "$FAIL"
