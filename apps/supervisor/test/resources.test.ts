import { describe, expect, it } from 'vitest';

import {
  aggregateMemory,
  assessAdmission,
  limitsFromConfig,
  readMemorySample,
} from '../src/resources';

const MIB = 1024 * 1024;

const limits = limitsFromConfig({
  memoryTargetMb: 4096,
  memoryBudgetMb: 6144,
  memoryHardLimitMb: 8192,
  memoryReserveMb: 1024,
  memoryHysteresisMb: 256,
  instanceMemoryMaxMb: 3072,
  instanceMemoryHighMb: 2560,
  memoryPressureAvg10: 10,
});

function aggregate(currentBytes: number, ok = true, extra: Record<string, unknown> = {}) {
  return {
    ok,
    currentBytes,
    instanceBytes: currentBytes,
    platformBytes: 0,
    databaseBytes: 0,
    supervisorBytes: 0,
    aggregateLimitBytes: null,
    aggregateCurrentBytes: null,
    unknownCgroups: ok ? [] : ['unknown'],
    ...extra,
  };
}

describe('aggregate memory policy', () => {
  it('admits a new instance below the scheduling budget', () => {
    const decision = assessAdmission({
      limits,
      aggregate: aggregate(2048 * MIB),
      host: {
        totalBytes: 16_000 * MIB,
        availableBytes: 8000 * MIB,
        swapTotalBytes: 0,
        swapFreeBytes: 0,
      },
      estimateBytes: 2048 * MIB,
      pressureAvg10: 0,
      blockedBefore: false,
    });
    expect(decision.admitted).toBe(true);
    expect(decision.state).toBe('ok');
    expect(decision.projectedBytes).toBe(4096 * MIB);
  });

  it('refuses above the hard limit even when the host is idle', () => {
    const decision = assessAdmission({
      limits,
      aggregate: aggregate(7000 * MIB),
      host: {
        totalBytes: 16_000 * MIB,
        availableBytes: 15_000 * MIB,
        swapTotalBytes: 0,
        swapFreeBytes: 0,
      },
      estimateBytes: 2048 * MIB,
      pressureAvg10: 0,
      blockedBefore: false,
    });
    expect(decision.admitted).toBe(false);
    expect(decision.state).toBe('over_budget');
  });

  it('allows expansion above the budget only with measured host headroom and low pressure', () => {
    const base = {
      limits,
      aggregate: aggregate(5000 * MIB),
      estimateBytes: 2048 * MIB,
      blockedBefore: false,
    };
    const noHeadroom = assessAdmission({
      ...base,
      host: {
        totalBytes: 16_000 * MIB,
        availableBytes: 1024 * MIB,
        swapTotalBytes: 0,
        swapFreeBytes: 0,
      },
      pressureAvg10: 0,
    });
    expect(noHeadroom.admitted).toBe(false);
    expect(noHeadroom.state).toBe('resource_wait');

    const pressured = assessAdmission({
      ...base,
      host: {
        totalBytes: 16_000 * MIB,
        availableBytes: 8000 * MIB,
        swapTotalBytes: 0,
        swapFreeBytes: 0,
      },
      pressureAvg10: 30,
    });
    expect(pressured.admitted).toBe(false);

    const headroom = assessAdmission({
      ...base,
      host: {
        totalBytes: 16_000 * MIB,
        availableBytes: 8000 * MIB,
        swapTotalBytes: 0,
        swapFreeBytes: 0,
      },
      pressureAvg10: 1,
    });
    expect(headroom.admitted).toBe(true);
  });

  it('applies hysteresis to a repository that was just refused', () => {
    const request = {
      limits,
      aggregate: aggregate(6000 * MIB),
      host: {
        totalBytes: 16_000 * MIB,
        availableBytes: 8000 * MIB,
        swapTotalBytes: 0,
        swapFreeBytes: 0,
      },
      estimateBytes: 0,
      pressureAvg10: 0,
    };
    expect(assessAdmission({ ...request, blockedBefore: false }).admitted).toBe(true);
    expect(assessAdmission({ ...request, blockedBefore: true }).admitted).toBe(false);
  });

  it('treats the enforceable slice limit as the real hard boundary', () => {
    const limited = assessAdmission({
      limits,
      aggregate: aggregate(3000 * MIB, true, { aggregateLimitBytes: 4096 * MIB }),
      host: {
        totalBytes: 16_000 * MIB,
        availableBytes: 15_000 * MIB,
        swapTotalBytes: 0,
        swapFreeBytes: 0,
      },
      estimateBytes: 2048 * MIB,
      pressureAvg10: 0,
      blockedBefore: false,
    });
    expect(limited.admitted).toBe(false);
    expect(limited.state).toBe('over_budget');
    expect(limited.reason).toContain('hard limit');

    const abovePolicy = assessAdmission({
      limits,
      aggregate: aggregate(1024 * MIB, true, { aggregateLimitBytes: 10 * 1024 * MIB }),
      host: {
        totalBytes: 16_000 * MIB,
        availableBytes: 15_000 * MIB,
        swapTotalBytes: 0,
        swapFreeBytes: 0,
      },
      estimateBytes: 2048 * MIB,
      pressureAvg10: 0,
      blockedBefore: false,
    });
    expect(abovePolicy.admitted).toBe(false);
    expect(abovePolicy.reason).toContain('above the');
  });

  it('refuses admission when the enforced boundary or a managed cgroup is unknown', () => {
    const decision = assessAdmission({
      limits,
      aggregate: aggregate(1024 * MIB, false),
      host: {
        totalBytes: 16_000 * MIB,
        availableBytes: 15_000 * MIB,
        swapTotalBytes: 0,
        swapFreeBytes: 0,
      },
      estimateBytes: 1024 * MIB,
      pressureAvg10: 0,
      blockedBefore: false,
    });
    expect(decision.admitted).toBe(false);
    expect(decision.state).toBe('resource_wait');
    expect(decision.reason).toContain('cannot be measured');
  });

  it('counts an unreadable instance cgroup as its estimate and reports unknown', () => {
    const aggregateValue = aggregateMemory({
      instanceCgroups: ['/sys/fs/cgroup/does-not-exist-moyu'],
      instanceEstimateBytes: 512 * MIB,
      platformCgroup: undefined,
      postgresCgroup: undefined,
    });
    expect(aggregateValue.ok).toBe(false);
    expect(aggregateValue.unknownCgroups).toHaveLength(1);
    expect(aggregateValue.currentBytes).toBeGreaterThanOrEqual(512 * MIB);
  });

  it('reports a missing cgroup as unknown instead of zero usage', () => {
    const sample = readMemorySample('/sys/fs/cgroup/does-not-exist-moyu');
    expect(sample.ok).toBe(false);
    expect(sample.currentBytes).toBeNull();
    expect(sample.error).toContain('memory.current');
  });
});
