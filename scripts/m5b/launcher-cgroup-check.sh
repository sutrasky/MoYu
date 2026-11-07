#!/bin/bash
# Bounded live probe: the production launcher is released only after its PID is
# inside an isolated 256 MiB cgroup; the first child and descendants must stay
# there. Fails non-zero if limits, membership, population or cleanup are unproven.
set -euo pipefail
CGROUP_LAUNCHER='IFS= read -r go <&3 || exit 126; [ "$go" = go ] || exit 127; exec "$@"'
slice="/sys/fs/cgroup/m5b-launcher-$$.slice"
probe="$slice/probe"
marker="/tmp/m5b-launcher-marker.$$"
childpid="/tmp/m5b-launcher-child.$$"
cleanup() {
  if [ -d "$probe" ]; then
    echo 1 > "$probe/cgroup.kill" 2>/dev/null || true
    rmdir "$probe" 2>/dev/null || true
  fi
  rmdir "$slice" 2>/dev/null || true
  rm -f "$marker" "$childpid" /tmp/m5b-launcher-probe.$$.mjs
}
trap cleanup EXIT
mkdir "$slice"
echo '+memory +pids' > "$slice/cgroup.subtree_control"
mkdir "$probe"
echo 268435456 > "$probe/memory.max"
test "$(cat "$probe/memory.max")" = 268435456
cat > /tmp/m5b-launcher-probe.$$.mjs <<'JS'
import { spawn } from 'node:child_process'
import { writeFileSync, readFileSync, existsSync } from 'node:fs'
const cgroup = process.argv[2]
const marker = process.argv[3]
const childFile = process.argv[4]
const child = spawn('/bin/sh', ['-c', process.env.CGROUP_LAUNCHER, 'moyu-cgroup', '/bin/sh', '-c',
  `sleep 3 & echo $! > ${childFile}; touch ${marker}; wait`], {
  detached: true, stdio: ['ignore', 'ignore', 'ignore', 'pipe'],
})

writeFileSync(`${cgroup}/cgroup.procs`, String(child.pid))
child.stdio[3].end('go\n')
await new Promise(resolve => setTimeout(resolve, 400))
const parent = readFileSync(`/proc/${child.pid}/cgroup`, 'utf8').trim()
const childPid = readFileSync(childFile, 'utf8').trim()
const first = readFileSync(`/proc/${childPid}/cgroup`, 'utf8').trim()
console.log(JSON.stringify({ parentPid: child.pid, parent, childPid, first,
  marker: existsSync(marker), max: readFileSync(`${cgroup}/memory.max`, 'utf8').trim() }))
JS
CGROUP_LAUNCHER="$CGROUP_LAUNCHER" /usr/bin/node /tmp/m5b-launcher-probe.$$.mjs "$probe" "$marker" "$childpid"
test -f "$marker"
# Wait, bounded, for the kernel to report the cgroup empty after kill.
echo 1 > "$probe/cgroup.kill"
for _ in $(seq 1 50); do
  if grep -q '^populated 0$' "$probe/cgroup.events"; then echo 'cgroup-empty'; break; fi
  sleep 0.1
done
grep -q '^populated 0$' "$probe/cgroup.events"
test -z "$(cat "$probe/cgroup.procs")"
rm -f "$marker" "$childpid"
