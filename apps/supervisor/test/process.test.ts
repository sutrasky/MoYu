import { spawn, type ChildProcess } from 'node:child_process';
import { join } from 'node:path';
import { mkdirSync, mkdtempSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { createServer, type Server } from 'node:net';

import { afterEach, describe, expect, it, vi } from 'vitest';

import { mergeQueuedWake } from '../src/manager';
import {
  cgroupEmpty,
  cgroupPids,
  cgroupTree,
  childPids,
  collectOwnership,
  descendants,
  ensureCgroup,
  movePidToCgroup,
  removeCgroup,
  identifyProcess,
  isTransientPolling,
  pidAlive,
  portFree,
  retainedDescendants,
  retainedIdentities,
  stopProcessTree,
  waitForPortClosed,
} from '../src/process';

const children: ChildProcess[] = [];

function spawnDetached(command: string, args: string[]): ChildProcess {
  const child = spawn(command, args, { detached: true, stdio: 'ignore' });
  children.push(child);
  return child;
}

async function waitUntil(predicate: () => boolean, timeoutMs = 5000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (predicate()) return;
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
  throw new Error('condition timed out');
}

afterEach(async () => {
  for (const child of children.splice(0)) {
    if (child.pid && pidAlive(child.pid)) {
      await stopProcessTree(child.pid, 3000);
    }
  }
});

describe('supervisor process evidence', () => {
  it('reports a loopback port as free only when nothing listens on it', async () => {
    expect(await portFree(4499)).toBe(true);
    const server: Server = createServer();
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', () => resolve()));
    const address = server.address();
    if (!address || typeof address === 'string') throw new Error('test server did not listen');
    expect(await portFree(address.port)).toBe(false);
    await new Promise<void>((resolve) => server.close(() => resolve()));
    expect(await waitForPortClosed(address.port, 1000)).toBe(true);
  });

  it('lists a real child process and stops its detached process group', async () => {
    const child = spawnDetached('sleep', ['30']);
    if (!child.pid) throw new Error('no pid');
    expect(descendants(process.pid)).toContain(child.pid);
    const result = await stopProcessTree(child.pid, 3000);
    expect(result.stopped).toBe(true);
    expect(pidAlive(child.pid)).toBe(false);
  });

  it('never signals a reused root PID whose start time does not match', async () => {
    const child = spawnDetached('sleep', ['30']);
    if (!child.pid) throw new Error('no pid');
    const current = identifyProcess(child.pid);
    if (!current) throw new Error('no identity');
    const stale = {
      rootPid: child.pid,
      pgid: child.pid,
      members: [{ ...current, startTime: '0', command: 'stale root' }],
    };
    expect(collectOwnership(child.pid, stale).members).toEqual([]);
    const kill = vi.spyOn(process, 'kill');
    const result = await stopProcessTree(child.pid, 1000, stale);
    const signals = kill.mock.calls
      .filter(
        ([pid, signal]) => pid === child.pid && (signal === 'SIGTERM' || signal === 'SIGKILL'),
      )
      .map(([, signal]) => signal);
    kill.mockRestore();
    expect(signals).toEqual([]);
    expect(result.stopped).toBe(false);
    expect(result.unknown).toBe(true);
    expect(pidAlive(child.pid)).toBe(true);
    await stopProcessTree(child.pid, 3000);
  });

  it('does not signal a reused process group after the owned members exited', async () => {
    const victim = spawnDetached('sleep', ['30']);
    if (!victim.pid) throw new Error('no pid');
    const stale = {
      rootPid: victim.pid,
      pgid: victim.pid,
      members: [{ pid: victim.pid, startTime: '0', pgid: victim.pid, sid: victim.pid }],
    };
    expect(collectOwnership(victim.pid, stale).members).toEqual([]);
    const result = await stopProcessTree(victim.pid, 1000, stale);
    expect(result.stopped).toBe(false);
    expect(result.unknown).toBe(true);
    expect(pidAlive(victim.pid)).toBe(true);
    await stopProcessTree(victim.pid, 3000);
  });

  it('finds a setsid child through the per-instance cgroup after the root exits', async () => {
    const cgroup = ensureCgroup(`test-${process.pid}-${Date.now()}`);
    if (!cgroup) {
      // GitHub-hosted runners do not expose a writable cgroup v2 hierarchy.
      // The cgroup path is exercised on the isolated Linux acceptance host.
      return;
    }
    try {
      const parent = spawnDetached('sh', [
        '-c',
        'echo $$ > "$1"; shift; exec sh -c "sleep 30 & exit 0"',
        'sh',
        join(cgroup, 'cgroup.procs'),
      ]);
      if (!parent.pid) throw new Error('no parent pid');
      const rootPid = parent.pid;
      const rootIdentity = identifyProcess(rootPid);
      if (!rootIdentity) throw new Error('no root identity');
      const stored = { rootPid, pgid: rootIdentity.pgid, members: [rootIdentity], cgroup };
      await waitUntil(() => !pidAlive(rootPid), 5000);
      const snapshot = collectOwnership(rootPid, stored);
      expect(snapshot.members.some((member) => member.pid !== rootPid)).toBe(true);
      const result = await stopProcessTree(rootPid, 3000, stored);
      expect(result.stopped).toBe(true);
      expect(cgroupPids(cgroup)).toEqual([]);
    } finally {
      removeCgroup(cgroup);
    }
  });

  it('scans the full cgroup subtree and removes it after stopping nested members', async () => {
    const cgroup = ensureCgroup(`tree-${process.pid}-${Date.now()}`);
    if (!cgroup) return;
    try {
      const nested = join(cgroup, 'nested');
      mkdirSync(nested);
      const root = spawnDetached('sleep', ['30']);
      const child = spawnDetached('sleep', ['30']);
      if (!root.pid || !child.pid) throw new Error('no pid');
      if (!movePidToCgroup(cgroup, root.pid) || !movePidToCgroup(nested, child.pid)) {
        throw new Error('could not move test processes');
      }
      const identity = identifyProcess(root.pid);
      if (!identity) throw new Error('no identity');
      const stored = { rootPid: root.pid, pgid: root.pid, members: [identity], cgroup };
      expect(cgroupTree(cgroup).pids).toEqual(expect.arrayContaining([root.pid, child.pid]));
      const result = await stopProcessTree(root.pid, 3000, stored);
      expect(result.stopped).toBe(true);
      expect(cgroupPids(cgroup)).toEqual([]);
    } finally {
      removeCgroup(cgroup);
    }
  });

  it('treats an unreadable cgroup boundary as unknown instead of empty', () => {
    const dir = mkdtempSync(join(tmpdir(), 'moyu-cgroup-unreadable-'));
    writeFileSync(join(dir, 'cgroup.procs'), '1\n');
    expect(cgroupTree(dir).ok).toBe(false);
    expect(cgroupEmpty(dir)).toEqual({ ok: false, empty: false });
  });

  it('keeps adopted historical descendants unknown even inside a new cgroup', async () => {
    const cgroup = ensureCgroup(`adopt-${process.pid}-${Date.now()}`);
    if (!cgroup) return;
    try {
      const dead = 4000000;
      const stored = {
        rootPid: dead,
        pgid: dead,
        cgroup,
        adoptedUnproven: true,
        members: [{ pid: dead, startTime: '0', pgid: dead, sid: dead, command: 'dead' }],
      };
      const snapshot = collectOwnership(dead, stored);
      expect(snapshot.unknown).toBe(true);
      const result = await stopProcessTree(dead, 1000, stored);
      expect(result.stopped).toBe(false);
      expect(result.unknown).toBe(true);
    } finally {
      removeCgroup(cgroup);
    }
  });

  it('stops a verified root and reports an unproven boundary without a cgroup', async () => {
    const child = spawnDetached('sleep', ['30']);
    if (!child.pid) throw new Error('no pid');
    const current = identifyProcess(child.pid);
    if (!current) throw new Error('no identity');
    const stored = { rootPid: child.pid, pgid: child.pid, members: [current] };
    const result = await stopProcessTree(child.pid, 3000, stored);
    expect(result.stopped).toBe(false);
    expect(result.unknown).toBe(true);
    expect(pidAlive(child.pid)).toBe(false);
  });

  it('finds a child forked after the snapshot even when the root already exited', async () => {
    const flag = `/tmp/moyu-orphan-${process.pid}-${Date.now()}`;
    const parent = spawnDetached('sh', [
      '-c',
      'while [ ! -f "$1" ]; do sleep 0.02; done; sleep 30 & exit 0',
      'sh',
      flag,
    ]);
    if (!parent.pid) throw new Error('no parent pid');
    const rootPid = parent.pid;
    const rootIdentity = identifyProcess(rootPid);
    if (!rootIdentity) throw new Error('no root identity');
    const stored = { rootPid, pgid: rootIdentity.pgid, members: [rootIdentity] };
    writeFileSync(flag, 'go');
    try {
      await waitUntil(() => !pidAlive(rootPid), 5000);
      const snapshot = collectOwnership(rootPid, stored);
      const children = snapshot.members.filter((member) => member.pid !== rootPid);
      expect(children.length).toBeGreaterThan(0);
      const result = await stopProcessTree(rootPid, 3000, stored);
      expect(result.stopped).toBe(false);
      expect(result.unknown).toBe(true);
      await waitUntil(() => collectOwnership(rootPid, stored).members.length === 0, 3000);
    } finally {
      try {
        const { rmSync } = await import('node:fs');
        rmSync(flag, { force: true });
      } catch {
        // Already removed.
      }
    }
  });

  it('does not report stopped when the parent exits but its child survives', async () => {
    const parent = spawnDetached('sh', ['-c', 'sleep 30 & exit 0']);
    if (!parent.pid) throw new Error('no parent pid');
    const rootPid = parent.pid;
    await waitUntil(() => !pidAlive(rootPid), 5000);
    const snapshot = collectOwnership(rootPid);
    expect(snapshot.members.length).toBeGreaterThan(0);
    const result = await stopProcessTree(rootPid, 3000);
    expect(result.stopped).toBe(true);
    expect(result.remaining).toEqual([]);
    await waitUntil(() => collectOwnership(rootPid).members.length === 0, 3000);
  });

  it('does not report stopped when the timeout expires before the child exits', async () => {
    const child = spawnDetached('sh', ['-c', 'trap "" TERM; sleep 30']);
    if (!child.pid) throw new Error('no pid');
    const result = await stopProcessTree(child.pid, 10);
    expect(result.stopped).toBe(false);
    expect(result.remaining.some((member) => member.pid === child.pid)).toBe(true);
    expect(pidAlive(child.pid)).toBe(true);
    await stopProcessTree(child.pid, 3000);
  });

  it('kills a child that ignores SIGTERM before reporting stopped', async () => {
    const child = spawnDetached('sh', ['-c', 'trap "" TERM; sleep 30']);
    if (!child.pid) throw new Error('no pid');
    const result = await stopProcessTree(child.pid, 3000);
    expect(result.stopped).toBe(true);
    expect(pidAlive(child.pid)).toBe(false);
    expect(collectOwnership(child.pid).members).toEqual([]);
  });

  it('retains a newly started service regardless of its age', async () => {
    const parent = spawnDetached('sh', ['-c', 'sleep 30; echo done']);
    if (!parent.pid) throw new Error('no pid');
    await new Promise((resolve) => setTimeout(resolve, 200));
    const childPid = childPids(parent.pid)[0] ?? NaN;
    expect(childPid).toBeGreaterThan(1);
    const retained = retainedDescendants(parent.pid);
    expect(retained.map((member) => member.pid)).toContain(childPid);
    expect(pidAlive(childPid)).toBe(true);
  });

  it('applies the retention policy only to known GitHub polling', () => {
    const members = [
      {
        pid: 2,
        startTime: '1',
        pgid: 2,
        sid: 2,
        command: '/root/.local/bin/gh-agent api repos/x/y',
      },
      { pid: 3, startTime: '2', pgid: 3, sid: 3, command: 'gh api repos/x/y' },
      { pid: 4, startTime: '3', pgid: 4, sid: 4, command: 'sleep 300' },
      { pid: 5, startTime: '4', pgid: 5, sid: 5, command: '/usr/bin/node server.js' },
      { pid: 6, startTime: '5', pgid: 6, sid: 6, command: 'gh-agent-worker --serve' },
      { pid: 7, startTime: '6', pgid: 7, sid: 7, command: 'gh pr create --title x' },
      { pid: 8, startTime: '7', pgid: 8, sid: 8, command: 'gh api -X POST repos/x/y' },
      { pid: 9, startTime: '8', pgid: 9, sid: 9, command: 'gh issue comment 1 --body x' },
    ];
    expect(retainedIdentities(members).map((member) => member.pid)).toEqual([4, 5, 6, 7, 8, 9]);
    expect(isTransientPolling('gh api repos/x/y')).toBe(true);
    expect(isTransientPolling('gh api -X GET repos/x/y')).toBe(true);
    expect(isTransientPolling('gh api --paginate notifications')).toBe(true);
    expect(isTransientPolling('gh issue list --repo x/y')).toBe(true);
    expect(isTransientPolling('gh pr list')).toBe(true);
    expect(isTransientPolling('gh pr create --title x')).toBe(false);
    expect(isTransientPolling('gh api -X POST repos/x/y')).toBe(false);
    expect(isTransientPolling('gh issue comment 1 --body x')).toBe(false);
  });
});

describe('queued wake merge', () => {
  it('keeps the highest priority and the earliest request time in both arrival orders', () => {
    const first = new Date('2026-11-05T00:00:00Z');
    const second = new Date('2026-11-05T00:00:10Z');
    const replyFirst = mergeQueuedWake(
      { queuePriority: 10, wakeRequestedAt: first },
      { priority: 20, at: second },
      true,
    );
    expect(replyFirst).toEqual({ queuePriority: 10, wakeRequestedAt: first });

    const taskFirst = mergeQueuedWake(
      { queuePriority: 20, wakeRequestedAt: first },
      { priority: 10, at: second },
      true,
    );
    expect(taskFirst).toEqual({ queuePriority: 10, wakeRequestedAt: first });
  });

  it('resets the queue for a fresh, non-queued request', () => {
    const at = new Date('2026-11-05T00:01:00Z');
    expect(
      mergeQueuedWake(
        { queuePriority: 10, wakeRequestedAt: new Date(0) },
        { priority: 20, at },
        false,
      ),
    ).toEqual({ queuePriority: 20, wakeRequestedAt: at });
  });
});

describe('pre-exec cgroup handshake', () => {
  it('does not run the command before the supervisor releases it', async () => {
    const { spawn } = await import('node:child_process');
    const { mkdtempSync, existsSync, rmSync } = await import('node:fs');
    const { tmpdir } = await import('node:os');
    const { join } = await import('node:path');
    const dir = mkdtempSync(join(tmpdir(), 'm5b-handshake-'));
    const marker = join(dir, 'started');
    const child = spawn(
      '/bin/sh',
      ['-c', 'read go <&3; exec /bin/sh -c "touch $1"', 'moyu-cgroup', marker],
      {
        detached: true,
        stdio: ['ignore', 'ignore', 'ignore', 'pipe'],
      },
    );
    try {
      await new Promise((resolve) => setTimeout(resolve, 150));
      expect(existsSync(marker)).toBe(false);
      const release = child.stdio[3] as import('node:stream').Writable | null;
      expect(release && 'end' in release).toBe(true);
      release!.end('go\n');
      await new Promise<void>((resolve, reject) => {
        child.once('exit', (code) => (code === 0 ? resolve() : reject(new Error(`exit ${code}`))));
        child.once('error', reject);
      });
      expect(existsSync(marker)).toBe(true);
    } finally {
      try {
        process.kill(-child.pid!, 'SIGKILL');
      } catch {
        /* already exited */
      }
      rmSync(dir, { recursive: true, force: true });
    }
  });
});

describe('cgroup launcher only accepts a positive release', () => {
  const run = async (release: 'eof' | 'wrong' | 'go', marker: string) => {
    const { spawn } = await import('node:child_process');
    const { CGROUP_LAUNCHER } = await import('../src/process');
    const child = spawn(
      '/bin/sh',
      ['-c', CGROUP_LAUNCHER, 'moyu-cgroup', '/bin/sh', '-c', `touch ${marker}`],
      {
        detached: true,
        stdio: ['ignore', 'ignore', 'ignore', 'pipe'],
      },
    );
    const pipe = child.stdio[3] as import('node:stream').Writable | null;
    if (release === 'eof') pipe!.end();
    else pipe!.end(release === 'go' ? 'go\n' : 'nope\n');
    const code = await new Promise<number | null>((resolve, reject) => {
      child.once('exit', resolve);
      child.once('error', reject);
    });
    try {
      process.kill(-child.pid!, 'SIGKILL');
    } catch {
      /* already exited */
    }
    return code;
  };

  it('does not execute on EOF or a wrong release, and does on the exact release', async () => {
    const { mkdtempSync, existsSync, rmSync } = await import('node:fs');
    const { tmpdir } = await import('node:os');
    const { join } = await import('node:path');
    const dir = mkdtempSync(join(tmpdir(), 'm5b-launcher-'));
    try {
      const eofMarker = join(dir, 'eof'),
        wrongMarker = join(dir, 'wrong'),
        goMarker = join(dir, 'go');
      expect(await run('eof', eofMarker)).toBe(126);
      expect(existsSync(eofMarker)).toBe(false);
      expect(await run('wrong', wrongMarker)).toBe(127);
      expect(existsSync(wrongMarker)).toBe(false);
      expect(await run('go', goMarker)).toBe(0);
      expect(existsSync(goMarker)).toBe(true);
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it('exits without executing when the parent dies before release', async () => {
    const { spawn } = await import('node:child_process');
    const { mkdtempSync, existsSync, rmSync } = await import('node:fs');
    const { tmpdir } = await import('node:os');
    const { join } = await import('node:path');
    const { CGROUP_LAUNCHER } = await import('../src/process');
    const dir = mkdtempSync(join(tmpdir(), 'm5b-parent-kill-'));
    const marker = join(dir, 'executed');
    const script = [
      "const {spawn}=require('node:child_process')",
      "const c=spawn('/bin/sh',['-c',process.env.LAUNCHER,'moyu-cgroup','/bin/sh','-c',`touch ${process.env.MARKER}`],{detached:true,stdio:['ignore','ignore','ignore','pipe']})",
      'console.log(c.pid)',
      'setInterval(()=>{},1000)',
    ].join(';');
    const parent = spawn(process.execPath, ['-e', script], {
      env: { ...process.env, LAUNCHER: CGROUP_LAUNCHER, MARKER: marker },
      stdio: ['ignore', 'pipe', 'ignore'],
    });
    try {
      await new Promise<void>((resolve) => parent.stdout!.once('data', () => resolve()));
      process.kill(parent.pid!, 'SIGKILL');
      await new Promise((resolve) => setTimeout(resolve, 500));
      expect(existsSync(marker)).toBe(false);
    } finally {
      try {
        process.kill(parent.pid!, 'SIGKILL');
      } catch {
        /* already exited */
      }
      rmSync(dir, { recursive: true, force: true });
    }
  });
});

describe('cgroup release pipe', () => {
  it('does not crash the supervisor when the child dies before release', async () => {
    const { spawn } = await import('node:child_process');
    const { CGROUP_LAUNCHER } = await import('../src/process');
    const child = spawn(
      '/bin/sh',
      ['-c', CGROUP_LAUNCHER, 'moyu-cgroup', '/bin/sh', '-c', 'sleep 5'],
      {
        detached: true,
        stdio: ['ignore', 'ignore', 'ignore', 'pipe'],
      },
    );
    const pipe = child.stdio[3] as import('node:stream').Writable | null;
    const errors: Error[] = [];
    pipe!.on('error', (error) => errors.push(error));
    process.kill(child.pid!, 'SIGKILL');
    pipe!.end('go\n');
    await new Promise((resolve) => setTimeout(resolve, 200));
    expect(errors.length).toBeLessThanOrEqual(1);
    try {
      process.kill(-child.pid!, 'SIGKILL');
    } catch {
      /* already exited */
    }
  });
});
