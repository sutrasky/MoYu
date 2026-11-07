# M5-B system boundary (isolated test service)

The M5-B resource model has two layers:

1. **One enforceable cgroup boundary** — `moyu-m5b.slice` with `MemoryMax=8G`
   (and `MemoryHigh=6G`). Platform API, dispatcher, supervisor, the isolated
   PostgreSQL and every runtime instance must be in this slice. Per-instance
   `memory.max` is only the second layer; it does not replace the slice.
2. **Measured admission** — the supervisor samples `memory.current`,
   `memory.peak`, `memory.events` and host `MemAvailable`, then admits a new
   instance only below the scheduling budget or with real host headroom up to
   the hard limit. Sampling alone is not presented as enforcement.

## Test-service layout

systemd nests a slice whose name contains a dash under the shorter parent slice; verify the
real path instead of hard-coding it:

```bash
systemctl show -p ControlGroup moyu-m5b.slice
# example: /moyu.slice/moyu-m5b.slice
```

```text
/sys/fs/cgroup/moyu.slice/moyu-m5b.slice/                     memory.max = 8G (systemd)
/sys/fs/cgroup/moyu.slice/moyu-m5b.slice/platform/            API + dispatcher cgroup
/sys/fs/cgroup/moyu.slice/moyu-m5b.slice/postgres/            isolated test cluster
/sys/fs/cgroup/moyu.slice/moyu-m5b.slice/moyu-supervisor/<profile>/   per-instance cgroups
```

Set these variables for the supervisor:

```dotenv
AGGREGATE_CGROUP=/sys/fs/cgroup/moyu.slice/moyu-m5b.slice
PLATFORM_CGROUP=/sys/fs/cgroup/moyu.slice/moyu-m5b.slice/platform
POSTGRES_CGROUP=/sys/fs/cgroup/moyu.slice/moyu-m5b.slice/postgres
MOYU_CGROUP_ROOT=/sys/fs/cgroup/moyu.slice/moyu-m5b.slice/moyu-supervisor
MEMORY_TARGET_MB=4096
MEMORY_BUDGET_MB=6144
MEMORY_HARD_LIMIT_MB=8192
MEMORY_RESERVE_MB=1024
MEMORY_HYSTERESIS_MB=256
INSTANCE_MEMORY_MAX_MB=3072
INSTANCE_MEMORY_HIGH_MB=2560
```

The supervisor refuses admission when the live slice limit is above
`MEMORY_HARD_LIMIT_MB`, when the aggregate is above the hard limit, when a
managed cgroup or the slice boundary cannot be measured, or when the host has no
measured headroom. An unreadable cgroup is shown as unknown and counted at the
instance estimate; estimated bytes are never written to measured fields.

Verify the effective membership:

```bash
systemctl show -p ControlGroup moyu-m5b-supervisor.service
systemd-cgls --unit moyu-m5b-supervisor.service
```

## Verification and rollback

Read-only check: `bash scripts/m5b/aggregate-cgroup-check.sh`.

Rollback of the test service is `systemctl stop` of the M5-B units followed by
removal of the slice; no production unit or shared cgroup configuration is
changed by this repository.
