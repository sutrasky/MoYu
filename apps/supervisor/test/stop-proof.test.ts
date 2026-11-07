import { spawn } from 'node:child_process';
import { mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';
import { bindStopProof, verifyStopProof, type StopProofIdentity } from '../src/stop-proof';

const base = `/sys/fs/cgroup/m5b-stop-proof-${process.pid}`;
const state = await mkdtemp(join(tmpdir(), 'm5b-stop-proof-'));
const children: number[] = [];
const identity = (suffix: string): StopProofIdentity => ({
  runtimeId: `r-${suffix}`,
  generation: `g-${suffix}`,
  ticketId: `t-${suffix}`,
  assignmentId: `a-${suffix}`,
  leaseId: `l-${suffix}`,
  fencingToken: '1',
});
const rootOnly = process.getuid?.() === 0;

beforeAll(async () => {
  if (rootOnly) await mkdir(base, { recursive: true });
});
afterEach(async () => {
  for (const pid of children.splice(0)) {
    try {
      process.kill(pid, 'SIGKILL');
    } catch {
      /* gone */
    }
  }
  for (const name of ['a', 'b']) {
    await rm(join(base, name), { recursive: true, force: true }).catch(() => {});
  }
});
afterAll(async () => {
  await rm(base, { recursive: true, force: true }).catch(() => {});
  await rm(state, { recursive: true, force: true }).catch(() => {});
});

describe.skipIf(!rootOnly)('supervisor cgroup stop proof', () => {
  it('refuses a different cgroup object for the same complete identity', async () => {
    const a = join(base, 'a'),
      b = join(base, 'b');
    await mkdir(a);
    await mkdir(b);
    const child = spawn('sleep', ['10'], { stdio: 'ignore' });
    children.push(child.pid!);
    await writeFile(join(a, 'cgroup.procs'), String(child.pid));
    const id = identity('a');
    await bindStopProof(state, { ...id, cgroup: a });
    expect(await verifyStopProof(state, id)).toMatchObject({
      proven: false,
      reason: expect.stringMatching(/populated/),
    });
    await expect(bindStopProof(state, { ...id, cgroup: b })).rejects.toThrow(
      /cgroup object conflicts/,
    );
    const live = await verifyStopProof(state, id);
    expect(live.proven).toBe(false);
    child.kill('SIGKILL');
    for (let i = 0; i < 50; i++) {
      if (/^populated 0$/m.test(await readFile(join(a, 'cgroup.events'), 'utf8'))) break;
      await new Promise((resolve) => setTimeout(resolve, 50));
    }
    expect(await verifyStopProof(state, id)).toEqual({ proven: true });
  });

  it('serializes concurrent first bindings so only one identity wins', async () => {
    const path = join(base, 'b');
    await mkdir(path);
    const results = await Promise.allSettled(
      Array.from({ length: 10 }, (_, i) =>
        bindStopProof(state, {
          ...identity(`c${i}`),
          runtimeId: 'race',
          generation: 'g1',
          cgroup: path,
        }),
      ),
    );
    expect(results.filter((r) => r.status === 'fulfilled')).toHaveLength(1);
    expect(results.filter((r) => r.status === 'rejected')).toHaveLength(9);
  });

  it('serializes independent processes with the kernel lock', async () => {
    const path = join(base, 'b');
    await mkdir(path);
    const script = `import { bindStopProof } from ${JSON.stringify(join(process.cwd(), 'src/stop-proof.ts'))};
await bindStopProof(process.env.BIND_ROOT, { runtimeId: 'proc', generation: 'g1', cgroup: process.env.BIND_CGROUP,
  ticketId: process.env.BIND_TICKET, assignmentId: 'a1', leaseId: 'l1', fencingToken: '1' });`;
    const run = (ticket: string) =>
      new Promise<number>((resolve) => {
        const child = spawn(
          process.execPath,
          ['--experimental-strip-types', '--input-type=module', '-e', script],
          {
            env: { ...process.env, BIND_ROOT: state, BIND_CGROUP: path, BIND_TICKET: ticket },
            stdio: 'ignore',
          },
        );
        child.once('exit', (code) => resolve(code ?? 1));
      });
    const codes = await Promise.all([run('t1'), run('t2')]);
    expect(codes.sort()).toEqual([0, 1]);
  });

  it('rejects malformed records, non-cgroup paths and missing bindings', async () => {
    await mkdir(join(base, 'b'));
    await writeFile(join(state, 'stop-proof', 'bad.json'), '{}');
    await expect(
      bindStopProof(state, { ...identity('bad'), runtimeId: 'bad', cgroup: join(base, 'b') }),
    ).rejects.toThrow(/unreadable/);
    await expect(bindStopProof(state, { ...identity('tmp'), cgroup: state })).rejects.toThrow(
      /live cgroup v2/,
    );
    expect(await verifyStopProof(state, identity('missing'))).toMatchObject({
      proven: false,
      reason: expect.stringMatching(/missing/),
    });
  });
});
