import { readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';

const MIB = 1024 * 1024;

export interface MemorySample {
  ok: boolean;
  currentBytes: number | null;
  peakBytes: number | null;
  limitBytes: number | null;
  events: Record<string, number>;
  oomKillCount: number;
  pressure: Record<string, unknown>;
  error: string | null;
}

export interface HostMemory {
  totalBytes: number;
  availableBytes: number;
  swapTotalBytes: number;
  swapFreeBytes: number;
}

export interface AggregateMemory {
  ok: boolean;
  currentBytes: number;
  instanceBytes: number;
  /** Bytes counted from a configured platform/postgres cgroup, when present. */
  platformBytes: number;
  databaseBytes: number;
  supervisorBytes: number;
  /** Measured limit of the single system slice, when one is configured. */
  aggregateLimitBytes: number | null;
  aggregateCurrentBytes: number | null;
  unknownCgroups: string[];
}

export interface ResourceLimits {
  targetBytes: number;
  budgetBytes: number;
  hardLimitBytes: number;
  reserveBytes: number;
  hysteresisBytes: number;
  instanceMaxBytes: number;
  instanceHighBytes: number;
  pressureAvg10: number;
}

export interface AdmissionDecision {
  admitted: boolean;
  state: 'ok' | 'resource_wait' | 'over_budget';
  reason: string;
  projectedBytes: number;
  currentBytes: number;
  hostAvailableBytes: number;
  pressureAvg10: number;
  aggregateOk: boolean;
}

export function limitsFromConfig(config: {
  memoryTargetMb: number;
  memoryBudgetMb: number;
  memoryHardLimitMb: number;
  memoryReserveMb: number;
  memoryHysteresisMb: number;
  instanceMemoryMaxMb: number;
  instanceMemoryHighMb: number;
  memoryPressureAvg10: number;
}): ResourceLimits {
  return {
    targetBytes: config.memoryTargetMb * MIB,
    budgetBytes: config.memoryBudgetMb * MIB,
    hardLimitBytes: config.memoryHardLimitMb * MIB,
    reserveBytes: config.memoryReserveMb * MIB,
    hysteresisBytes: config.memoryHysteresisMb * MIB,
    instanceMaxBytes: config.instanceMemoryMaxMb * MIB,
    instanceHighBytes: config.instanceMemoryHighMb * MIB,
    pressureAvg10: config.memoryPressureAvg10,
  };
}

function readText(path: string): string | null {
  try {
    return readFileSync(path, 'utf8');
  } catch {
    return null;
  }
}

function numberValue(value: string | null | undefined): number | null {
  if (value == null) return null;
  const trimmed = value.trim();
  if (trimmed === 'max' || trimmed === '') return null;
  const parsed = Number(trimmed);
  return Number.isFinite(parsed) ? parsed : null;
}

/**
 * Enables the controllers the supervisor needs in its own subtree. Without this
 * the kernel creates child cgroups without `memory.current`, so memory would be
 * unknown and could not be part of the aggregate.
 */
export function enableSubtreeControllers(path: string): { ok: boolean; error: string | null } {
  const subtree = readText(join(path, 'cgroup.subtree_control'));
  if (subtree == null) return { ok: false, error: `cgroup subtree is not readable: ${path}` };
  const enabled = new Set(subtree.split(/\s+/).filter(Boolean));
  const wanted = ['memory', 'pids'].filter((controller) => !enabled.has(controller));
  if (wanted.length === 0) return { ok: true, error: null };
  try {
    writeFileSync(
      join(path, 'cgroup.subtree_control'),
      wanted.map((controller) => `+${controller}`).join(' '),
    );
    return { ok: true, error: null };
  } catch (error) {
    return {
      ok: false,
      error: error instanceof Error ? error.message : String(error),
    };
  }
}

/** Reads one cgroup's measured memory. A missing file is unknown, never zero. */
export function readMemorySample(path: string | null | undefined): MemorySample {
  if (!path) {
    return {
      ok: false,
      currentBytes: null,
      peakBytes: null,
      limitBytes: null,
      events: {},
      oomKillCount: 0,
      pressure: {},
      error: 'no cgroup path',
    };
  }
  const current = numberValue(readText(join(path, 'memory.current')));
  const peak = numberValue(readText(join(path, 'memory.peak')));
  const limit = numberValue(readText(join(path, 'memory.max')));
  const rawEvents = readText(join(path, 'memory.events'));
  const events: Record<string, number> = {};
  if (rawEvents) {
    for (const line of rawEvents.split('\n')) {
      const [key, value] = line.trim().split(/\s+/);
      if (key && value != null && Number.isFinite(Number(value))) events[key] = Number(value);
    }
  }
  const pressure: Record<string, unknown> = {};
  const rawPressure = readText(join(path, 'memory.pressure'));
  if (rawPressure) {
    for (const line of rawPressure.split('\n')) {
      const [kind, ...rest] = line.trim().split(/\s+/);
      if (!kind) continue;
      const values: Record<string, number> = {};
      for (const entry of rest) {
        const [name, value] = entry.split('=');
        if (name && value != null && Number.isFinite(Number(value))) values[name] = Number(value);
      }
      pressure[kind] = values;
    }
  }
  return {
    ok: current != null,
    currentBytes: current,
    peakBytes: peak,
    limitBytes: limit,
    events,
    oomKillCount: events.oom_kill ?? 0,
    pressure,
    error: current == null ? `memory.current is not readable under ${path}` : null,
  };
}

/** Applies a hard and a soft per-instance cgroup limit. Returns false on a read-only hierarchy. */
export function applyInstanceMemoryLimit(
  path: string,
  limits: ResourceLimits,
): { ok: boolean; error: string | null } {
  try {
    writeFileSync(join(path, 'memory.max'), String(limits.instanceMaxBytes));
    writeFileSync(join(path, 'memory.high'), String(limits.instanceHighBytes));
    return { ok: true, error: null };
  } catch (error) {
    return { ok: false, error: error instanceof Error ? error.message : String(error) };
  }
}

export function readHostMemory(): HostMemory {
  const raw = readText('/proc/meminfo') ?? '';
  const values = new Map<string, number>();
  for (const line of raw.split('\n')) {
    const match = /^([A-Za-z_()]+):\s+(\d+)\s+kB$/.exec(line.trim());
    if (match) values.set(match[1] as string, Number(match[2]) * 1024);
  }
  return {
    totalBytes: values.get('MemTotal') ?? 0,
    availableBytes: values.get('MemAvailable') ?? 0,
    swapTotalBytes: values.get('SwapTotal') ?? 0,
    swapFreeBytes: values.get('SwapFree') ?? 0,
  };
}

/** Path of a cgroup from `/proc/<pid>/cgroup`, or null when it cannot be resolved. */
export function selfCgroupPath(): string | null {
  const raw = readText('/proc/self/cgroup');
  if (!raw) return null;
  for (const line of raw.split('\n')) {
    const match = /^0::(.+)$/.exec(line.trim());
    if (match?.[1]) return `/sys/fs/cgroup${match[1]}`;
  }
  return null;
}

/**
 * Sums the measured memory of the whole system boundary. Any configured cgroup
 * that cannot be read is reported as unknown; the caller counts an unknown
 * instance as its maximum instead of pretending it uses nothing.
 */
export function aggregateMemory(input: {
  instanceCgroups: (string | null | undefined)[];
  instanceEstimateBytes: number;
  platformCgroup?: string;
  postgresCgroup?: string;
  /** The systemd slice that holds platform, DB, supervisor and instances. */
  aggregateCgroup?: string;
}): AggregateMemory {
  let instanceBytes = 0;
  let platformBytes = 0;
  let databaseBytes = 0;
  let supervisorBytes = 0;
  const unknownCgroups: string[] = [];
  for (const cgroup of input.instanceCgroups) {
    if (!cgroup) continue;
    const sample = readMemorySample(cgroup);
    if (sample.currentBytes != null) instanceBytes += sample.currentBytes;
    else {
      unknownCgroups.push(cgroup);
      instanceBytes += input.instanceEstimateBytes;
    }
  }
  const platform = input.platformCgroup ? readMemorySample(input.platformCgroup) : null;
  if (input.platformCgroup && platform?.currentBytes == null) {
    unknownCgroups.push(input.platformCgroup);
  } else if (platform?.currentBytes != null) {
    platformBytes = platform.currentBytes;
  }
  const database = input.postgresCgroup ? readMemorySample(input.postgresCgroup) : null;
  if (input.postgresCgroup && database?.currentBytes == null) {
    unknownCgroups.push(input.postgresCgroup);
  } else if (database?.currentBytes != null) {
    databaseBytes = database.currentBytes;
  }
  // The supervisor's own cgroup is counted when no platform cgroup was given,
  // so a dedicated platform slice is not double counted.
  if (!input.platformCgroup) {
    const own = selfCgroupPath();
    const sample = own ? readMemorySample(own) : null;
    if (sample?.currentBytes != null) supervisorBytes = sample.currentBytes;
  }
  let aggregateLimitBytes: number | null = null;
  let aggregateCurrentBytes: number | null = null;
  if (input.aggregateCgroup) {
    const slice = readMemorySample(input.aggregateCgroup);
    aggregateLimitBytes = slice.limitBytes;
    aggregateCurrentBytes = slice.currentBytes;
    if (slice.limitBytes == null) {
      unknownCgroups.push(`${input.aggregateCgroup}:memory.max`);
    }
  }
  const summed = instanceBytes + platformBytes + databaseBytes + supervisorBytes;
  return {
    ok: unknownCgroups.length === 0,
    currentBytes: aggregateCurrentBytes != null ? Math.max(summed, aggregateCurrentBytes) : summed,
    instanceBytes,
    platformBytes,
    databaseBytes,
    supervisorBytes,
    aggregateLimitBytes,
    aggregateCurrentBytes,
    unknownCgroups,
  };
}

export function pressureAvg10(sample: MemorySample | null | undefined): number {
  const some = sample?.pressure.some;
  return typeof some === 'object' &&
    some !== null &&
    typeof (some as Record<string, unknown>).avg10 === 'number'
    ? ((some as Record<string, number>).avg10 as number)
    : 0;
}

/**
 * Decides whether one more instance may start under the aggregate budget.
 *
 * - projected above the hard limit: refused, no host headroom can override it;
 * - projected above the 6 GB scheduling budget: admitted only while the host
 *   still has real headroom and PSI pressure is below the configured threshold;
 * - otherwise admitted.
 *
 * `blockedBefore` applies the hysteresis: a previously refused repository must
 * fall back below `budget - hysteresis` before it is admitted, so admission does
 * not flap at the boundary.
 */
export function assessAdmission(input: {
  limits: ResourceLimits;
  aggregate: AggregateMemory;
  host: HostMemory;
  estimateBytes: number;
  pressureAvg10: number;
  blockedBefore: boolean;
}): AdmissionDecision {
  const projected = input.aggregate.currentBytes + input.estimateBytes;
  const boundary = input.blockedBefore
    ? input.limits.budgetBytes - input.limits.hysteresisBytes
    : input.limits.budgetBytes;
  const mb = (bytes: number) => `${Math.round(bytes / MIB)} MiB`;
  if (!input.aggregate.ok) {
    return {
      admitted: false,
      state: 'resource_wait',
      reason:
        'The enforced memory boundary or a managed cgroup cannot be measured; refusing admission until it is readable',
      projectedBytes: projected,
      currentBytes: input.aggregate.currentBytes,
      hostAvailableBytes: input.host.availableBytes,
      pressureAvg10: input.pressureAvg10,
      aggregateOk: false,
    };
  }
  if (
    input.aggregate.aggregateLimitBytes != null &&
    input.aggregate.aggregateLimitBytes > input.limits.hardLimitBytes
  ) {
    return {
      admitted: false,
      state: 'over_budget',
      reason: `The enforced system slice limit ${mb(input.aggregate.aggregateLimitBytes)} is above the ${mb(input.limits.hardLimitBytes)} policy; refusing admission until the slice matches the policy`,
      projectedBytes: projected,
      currentBytes: input.aggregate.currentBytes,
      hostAvailableBytes: input.host.availableBytes,
      pressureAvg10: input.pressureAvg10,
      aggregateOk: input.aggregate.ok,
    };
  }
  const effectiveHardLimit =
    input.aggregate.aggregateLimitBytes == null
      ? input.limits.hardLimitBytes
      : Math.min(input.limits.hardLimitBytes, input.aggregate.aggregateLimitBytes);
  if (projected > effectiveHardLimit) {
    return {
      admitted: false,
      state: 'over_budget',
      reason: `Projected aggregate memory ${mb(projected)} exceeds the ${mb(effectiveHardLimit)} enforceable system hard limit`,
      projectedBytes: projected,
      currentBytes: input.aggregate.currentBytes,
      hostAvailableBytes: input.host.availableBytes,
      pressureAvg10: input.pressureAvg10,
      aggregateOk: input.aggregate.ok,
    };
  }
  if (projected > boundary) {
    const excess = projected - input.limits.budgetBytes;
    const headroom = input.host.availableBytes - input.limits.reserveBytes;
    // Above the budget, expansion needs measured host headroom. Between the
    // hysteresis boundary and the budget, a previously refused repository waits
    // until it falls back, so admission does not flap at the edge.
    const aboveBudget = projected > input.limits.budgetBytes;
    const refusesForHysteresis = input.blockedBefore && !aboveBudget;
    if (
      refusesForHysteresis ||
      (aboveBudget &&
        (headroom < Math.max(0, excess) ||
          input.pressureAvg10 > input.limits.pressureAvg10 ||
          !input.aggregate.ok))
    ) {
      return {
        admitted: false,
        state: 'resource_wait',
        reason: refusesForHysteresis
          ? `Projected aggregate memory ${mb(projected)} is still above the ${mb(boundary)} hysteresis boundary`
          : `Projected aggregate memory ${mb(projected)} is above the ${mb(input.limits.budgetBytes)} scheduling budget and host headroom/pressure does not allow expansion`,
        projectedBytes: projected,
        currentBytes: input.aggregate.currentBytes,
        hostAvailableBytes: input.host.availableBytes,
        pressureAvg10: input.pressureAvg10,
        aggregateOk: input.aggregate.ok,
      };
    }
  }
  if (input.host.availableBytes < input.limits.reserveBytes + input.estimateBytes) {
    return {
      admitted: false,
      state: 'resource_wait',
      reason: `Host MemAvailable ${mb(input.host.availableBytes)} leaves less than the ${mb(input.limits.reserveBytes)} reserve plus the new instance`,
      projectedBytes: projected,
      currentBytes: input.aggregate.currentBytes,
      hostAvailableBytes: input.host.availableBytes,
      pressureAvg10: input.pressureAvg10,
      aggregateOk: input.aggregate.ok,
    };
  }
  return {
    admitted: true,
    state: 'ok',
    reason: `Measured aggregate ${mb(input.aggregate.currentBytes)}, projected ${mb(projected)} within ${mb(input.limits.hardLimitBytes)}`,
    projectedBytes: projected,
    currentBytes: input.aggregate.currentBytes,
    hostAvailableBytes: input.host.availableBytes,
    pressureAvg10: input.pressureAvg10,
    aggregateOk: input.aggregate.ok,
  };
}
