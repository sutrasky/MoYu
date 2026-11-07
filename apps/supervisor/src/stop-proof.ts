import { mkdir, open, readFile, realpath, rename, stat, statfs, unlink } from 'node:fs/promises';
import { randomUUID } from 'node:crypto';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { join } from 'node:path';

export interface StopProof {
  version: 1;
  runtimeId: string;
  generation: string;
  cgroup: string;
  cgroupInode: number;
  ticketId: string;
  assignmentId: string;
  leaseId: string;
  fencingToken: string;
  state: 'unproven' | 'proven';
  at: string;
}

export interface StopProofIdentity {
  runtimeId: string;
  generation: string;
  ticketId: string;
  assignmentId: string;
  leaseId: string;
  fencingToken: string;
}
export interface StopProofInput extends StopProofIdentity {
  cgroup: string;
}
export type VerifyResult = { proven: true } | { proven: false; reason: string };

const file = (root: string, runtimeId: string) =>
  join(root, 'stop-proof', `${encodeURIComponent(runtimeId)}.json`);
const LOCK = '.lock';
const CGROUP2_SUPER_MAGIC = 0x63677270n;
const sameIdentity = (a: StopProofIdentity, b: StopProofIdentity) =>
  a.runtimeId === b.runtimeId &&
  a.generation === b.generation &&
  a.ticketId === b.ticketId &&
  a.assignmentId === b.assignmentId &&
  a.leaseId === b.leaseId &&
  a.fencingToken === b.fencingToken;
async function cgroupPath(path: string): Promise<string | undefined> {
  let resolved: string;
  try {
    resolved = await realpath(path);
  } catch {
    return undefined;
  }
  if (!resolved.startsWith('/sys/fs/cgroup/') || resolved === '/sys/fs/cgroup') return undefined;
  try {
    const fs = await statfs(resolved, { bigint: true });
    if (fs.type !== CGROUP2_SUPER_MAGIC) return undefined;
  } catch {
    return undefined;
  }
  return resolved;
}

function valid(value: unknown): value is StopProof {
  if (!value || typeof value !== 'object') return false;
  const p = value as Record<string, unknown>;
  return (
    p.version === 1 &&
    typeof p.runtimeId === 'string' &&
    typeof p.generation === 'string' &&
    typeof p.cgroup === 'string' &&
    typeof p.cgroupInode === 'number' &&
    typeof p.ticketId === 'string' &&
    typeof p.assignmentId === 'string' &&
    typeof p.leaseId === 'string' &&
    typeof p.fencingToken === 'string' &&
    (p.state === 'unproven' || p.state === 'proven') &&
    typeof p.at === 'string'
  );
}

async function readProof(
  root: string,
  runtimeId: string,
): Promise<StopProof | undefined | 'unreadable'> {
  try {
    const parsed: unknown = JSON.parse(await readFile(file(root, runtimeId), 'utf8'));
    return valid(parsed) ? parsed : 'unreadable';
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'ENOENT') return undefined;
    return 'unreadable';
  }
}
async function writeAtomic(temporary: string, target: string, text: string): Promise<void> {
  const handle = await open(temporary, 'wx', 0o600);
  try {
    await handle.writeFile(text);
    await handle.sync();
  } finally {
    await handle.close();
  }
  await rename(temporary, target);
  await unlink(temporary).catch(() => {});
}

/** Kernel-held mutex: util-linux flock owns the lock; if this process dies or
 * closes the pipe, the holder exits and the kernel releases it. */
async function withLock<T>(root: string, work: () => Promise<T>): Promise<T> {
  const dir = join(root, 'stop-proof');
  await mkdir(dir, { recursive: true });
  const holder = spawn('/usr/bin/flock', ['-x', join(dir, LOCK), '-c', 'printf "locked\n"; cat'], {
    stdio: ['pipe', 'pipe', 'ignore'],
  });
  const locked = new Promise<void>((resolve, reject) => {
    let output = '';
    const timer = setTimeout(
      () => reject(new Error('timed out waiting for the stop-proof lock')),
      10_000,
    );
    holder.once('error', (error) => {
      clearTimeout(timer);
      reject(error);
    });
    holder.stdout!.on('data', (chunk) => {
      output += String(chunk);
      if (output.includes('locked')) {
        clearTimeout(timer);
        resolve();
      }
    });
    holder.once('exit', (code) => {
      if (!output.includes('locked')) {
        clearTimeout(timer);
        reject(new Error(`flock exited before acquiring (${code})`));
      }
    });
  });
  try {
    await locked;
    return await work();
  } finally {
    holder.stdin?.end();
    await Promise.race([
      once(holder, 'exit'),
      new Promise((resolve) => setTimeout(resolve, 2000)),
    ]).catch(() => {});
  }
}

/** Bind before stopping. Every dimension is immutable while unproven. */
export async function bindStopProof(root: string, input: StopProofInput): Promise<StopProof> {
  const cgroup = await cgroupPath(input.cgroup);
  if (!cgroup) throw new Error('cgroup must be a live cgroup v2 object');
  return withLock(root, async () => {
    const existing = await readProof(root, input.runtimeId);
    if (existing === 'unreadable')
      throw new Error('existing stop proof is unreadable; refusing to overwrite');
    if (existing) {
      if (sameIdentity(existing, input)) {
        if (existing.cgroup !== input.cgroup)
          throw new Error('cgroup object conflicts with the bound generation');
        if (existing.cgroupInode !== (await stat(input.cgroup)).ino)
          throw new Error('cgroup object inode changed');
        return existing;
      }
      if (existing.state === 'unproven')
        throw new Error('another unproven stop proof is bound to this runtime');
    }
    const proof: StopProof = {
      version: 1,
      ...input,
      cgroupInode: (await stat(input.cgroup)).ino,
      state: 'unproven',
      at: new Date().toISOString(),
    };
    await writeAtomic(
      `${file(root, input.runtimeId)}.${randomUUID()}.tmp`,
      file(root, input.runtimeId),
      JSON.stringify(proof),
    );
    return proof;
  });
}

/** Prove the exact old generation and complete identity is empty in the same cgroup object. */
export async function verifyStopProof(
  root: string,
  identity: StopProofIdentity,
): Promise<VerifyResult> {
  return withLock(root, async () => {
    const proof = await readProof(root, identity.runtimeId);
    if (proof === 'unreadable')
      return { proven: false, reason: 'stop proof is unreadable or malformed' };
    if (!proof) return { proven: false, reason: 'stop proof is missing' };
    if (!sameIdentity(proof, identity))
      return { proven: false, reason: 'stop proof binding does not match' };
    const cgroup = await cgroupPath(proof.cgroup);
    if (!cgroup || cgroup !== proof.cgroup)
      return { proven: false, reason: 'cgroup path is not the original cgroup v2 object' };
    let events: string;
    try {
      const handle = await open(cgroup, 'r');
      try {
        if ((await handle.stat()).ino !== proof.cgroupInode)
          return { proven: false, reason: 'cgroup object was replaced' };
        events = await readFile(`/proc/self/fd/${handle.fd}/cgroup.events`, 'utf8');
      } finally {
        await handle.close();
      }
    } catch {
      return { proven: false, reason: 'cgroup.events is unreadable or permission unknown' };
    }
    if (!/^populated 0$/m.test(events))
      return { proven: false, reason: 'cgroup still has populated descendants' };
    await writeAtomic(
      `${file(root, identity.runtimeId)}.${randomUUID()}.tmp`,
      file(root, identity.runtimeId),
      JSON.stringify({ ...proof, state: 'proven', at: new Date().toISOString() }),
    );
    return { proven: true };
  });
}
