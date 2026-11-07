#!/usr/bin/env bash
# Isolated resource-boundary check for M5-B.
#
# Safety:
# - unique slice/unit names, so no existing slice or drop-in is touched;
# - host MemAvailable must be at least 8 GiB before anything starts;
# - the slice is capped at 8 GiB and the hog scope at 256 MiB with swap 0;
# - the hog refuses to allocate unless its own cgroup limit is <= 256 MiB;
# - RuntimeMaxSec bounds every transient unit; cleanup only touches this run's
#   units and the drop-in this run created.
set -uo pipefail

if [[ ${EUID:-$(id -u)} -ne 0 ]]; then
  echo "must run as root to create the isolated systemd slice" >&2
  exit 2
fi

RUN=$$
OUT=${M5B_OOM_OUT:-docs/evidence/m5b/nonroot-oom.log}
SLICE="m5bprobe${RUN}.slice"
ANCHOR="m5bprobe${RUN}-anchor"
HOG="m5bprobe${RUN}-hog"
DROPIN="/run/systemd/system.control/${SLICE}.d"
HOG_RUN="/tmp/m5bprobe${RUN}-hog.mjs"
dropin_existed=0
[[ -e "$DROPIN" ]] && dropin_existed=1
mkdir -p "$(dirname "$OUT")"

fail() {
  echo "FAIL: $*"
  exit 1
}

cleanup() {
  systemctl stop "$HOG" 2>/dev/null || true
  systemctl reset-failed "$HOG" 2>/dev/null || true
  systemctl stop "$ANCHOR" 2>/dev/null || true
  systemctl stop "$SLICE" 2>/dev/null || true
  if (( dropin_existed == 0 )); then
    rm -rf "$DROPIN"
    systemctl daemon-reload 2>/dev/null || true
  fi
  rm -f "$HOG_RUN"
  if (( dropin_existed == 0 )); then
    systemctl stop "$SLICE" 2>/dev/null || true
  fi
}
trap cleanup EXIT

available_before=$(awk '/MemAvailable/{print $2}' /proc/meminfo)
(( available_before >= 8 * 1024 * 1024 )) || fail "MemAvailable ${available_before} kB is below the 8 GiB safety floor"

{
  echo "date=$(date -Is)"
  echo "slice=$SLICE"
  echo "host_mem_available_before_kb=$available_before"

  systemd-run --unit="$ANCHOR" --slice="$SLICE" \
    --property=RuntimeMaxSec=120 --property=MemoryMax=64M \
    /bin/sleep 120 >/dev/null || fail "could not start the slice anchor"
  systemctl set-property --runtime "$SLICE" MemoryMax=8G MemoryHigh=6G \
    || fail "could not set the slice memory policy"

  slice_cg=$(systemctl show -p ControlGroup --value "$SLICE")
  slice_path="/sys/fs/cgroup${slice_cg}"
  [[ -d "$slice_path" ]] || fail "slice cgroup $slice_path is missing"
  echo "slice_control_group=$slice_cg"
  echo "slice_memory_max=$(cat "$slice_path/memory.max")"
  echo "slice_memory_high=$(cat "$slice_path/memory.high")"
  [[ "$(cat "$slice_path/memory.max")" == "8589934592" ]] || fail "slice memory.max is not 8 GiB"
  [[ "$(cat "$slice_path/memory.high")" == "6442450944" ]] || fail "slice memory.high is not 6 GiB"

  anchor_pid=$(systemctl show -p MainPID --value "$ANCHOR")
  anchor_cg=$(awk -F: '/^0::/{print $3}' "/proc/$anchor_pid/cgroup" 2>/dev/null || true)
  echo "anchor_pid=$anchor_pid anchor_cgroup=$anchor_cg"
  [[ -n "$anchor_cg" && "$anchor_cg" == "$slice_cg"* ]] || fail "anchor is not inside the slice cgroup"

  cp "$PWD/scripts/m5b/memory-hog.mjs" "$HOG_RUN" || fail "could not place the hog fixture"
  chmod 0644 "$HOG_RUN"

  systemd-run --unit="$HOG" --slice="$SLICE" --uid=nobody \
    --property=MemoryMax=256M --property=MemoryHigh=256M --property=MemorySwapMax=0 \
    --property=RuntimeMaxSec=10 --property=TimeoutStopSec=10 \
    /usr/bin/node "$HOG_RUN" >/dev/null || fail "could not start the bounded hog"

  hog_cg=''
  oom_kill=0
  for _ in $(seq 1 100); do
    hog_cg=$(systemctl show -p ControlGroup --value "$HOG" 2>/dev/null || true)
    if [[ -n "$hog_cg" && -r "/sys/fs/cgroup${hog_cg}/memory.events" ]]; then
      oom_kill=$(awk '$1=="oom_kill"{print $2}' "/sys/fs/cgroup${hog_cg}/memory.events")
      oom_kill=${oom_kill:-0}
      (( oom_kill > 0 )) && break
    fi
    systemctl is-active --quiet "$HOG" || break
    sleep 0.2
  done

  hog_result=$(systemctl show -p Result --value "$HOG" 2>/dev/null || true)
  echo "hog_control_group=$hog_cg"
  echo "hog_result=$hog_result"
  echo "hog_oom_kill=$oom_kill"
  if [[ -n "$hog_cg" && -r "/sys/fs/cgroup${hog_cg}/memory.max" ]]; then
    echo "hog_memory_max=$(cat "/sys/fs/cgroup${hog_cg}/memory.max")"
    echo "hog_memory_peak=$(cat "/sys/fs/cgroup${hog_cg}/memory.peak")"
    echo "hog_memory_events=$(tr '\n' ' ' < "/sys/fs/cgroup${hog_cg}/memory.events")"
  fi

  (( oom_kill > 0 )) || fail "no oom_kill was recorded in the hog cgroup"
  [[ "$hog_result" == "oom-kill" ]] || fail "systemd result is $hog_result, not oom-kill"

  available_after=$(awk '/MemAvailable/{print $2}' /proc/meminfo)
  echo "host_mem_available_after_kb=$available_after"
  (( available_after >= 8 * 1024 * 1024 )) || fail "host MemAvailable fell below the safety floor"
  echo "PASS"
} 2>&1 | tee "$OUT"

grep -q '^PASS$' "$OUT"
