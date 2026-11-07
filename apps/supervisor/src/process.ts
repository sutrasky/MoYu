import { createConnection } from 'node:net';
import {
  lstatSync,
  mkdirSync,
  readFileSync,
  readdirSync,
  rmdirSync,
  statSync,
  writeFileSync,
} from 'node:fs';
import { basename, join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';

import { enableSubtreeControllers, readMemorySample } from './resources';

export async function portFree(port: number, host = '127.0.0.1'): Promise<boolean> {
  return new Promise((resolve) => {
    const socket = createConnection({ host, port });
    const finish = (free: boolean) => {
      socket.removeAllListeners();
      socket.destroy();
      resolve(free);
    };
    socket.setTimeout(600);
    socket.once('connect', () => finish(false));
    socket.once('timeout', () => finish(true));
    socket.once('error', () => finish(true));
  });
}

export function pidAlive(pid: number): boolean {
  if (!Number.isInteger(pid) || pid <= 1) return false;
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

export interface ProcessIdentity {
  pid: number;
  /** Kernel start time in clock ticks; identifies a PID even after reuse. */
  startTime: string;
  pgid: number;
  sid: number;
  command: string;
}

export interface ProcessOwnershipSnapshot {
  rootPid: number | null;
  pgid: number | null;
  members: ProcessIdentity[];
  /** Ownership cannot be proven for at least one live process boundary. */
  unknown: boolean;
  /** Live members that cannot be tied to the saved identity. */
  unknownMembers: ProcessIdentity[];
}

export interface StoredOwnership {
  execution?: {
    ticketId: string;
    assignmentId: string;
    leaseId: string;
    fencingToken: string;
  } | null;
  rootPid?: number | null;
  pgid?: number | null;
  cgroup?: string | null;
  /** Discovery-only callers without a persisted boundary may accept best-effort. */
  allowUnproven?: boolean;
  /** Adopted outside a pre-existing cgroup; historical descendants are unproven. */
  adoptedUnproven?: boolean;
  members?: Array<Partial<ProcessIdentity> & { pid?: number; startTime?: string }>;
}

export interface StopProcessResult {
  stopped: boolean;
  rootPid: number;
  pgid: number | null;
  remaining: ProcessIdentity[];
  /** The saved group still has live members we cannot prove are ours. */
  unknown: boolean;
}

interface ProcStat {
  startTime: string;
  pgid: number;
  sid: number;
}

function readProcStat(pid: number): ProcStat | null {
  try {
    const raw = readFileSync(`/proc/${pid}/stat`, 'utf8');
    // comm may contain spaces and parentheses; fields after the last ')' are stable.
    const tail = raw.slice(raw.lastIndexOf(')') + 2).split(' ');
    const startTime = tail[19];
    const pgid = Number(tail[2]);
    const sid = Number(tail[3]);
    if (!startTime) return null;
    return {
      startTime,
      pgid: Number.isFinite(pgid) ? pgid : pid,
      sid: Number.isFinite(sid) ? sid : pid,
    };
  } catch {
    return null;
  }
}

function readCommand(pid: number): string {
  try {
    const raw = readFileSync(`/proc/${pid}/cmdline`, 'utf8');
    return raw.replaceAll('\0', ' ').trim();
  } catch {
    return '';
  }
}

export function identifyProcess(pid: number): ProcessIdentity | null {
  if (!pidAlive(pid)) return null;
  const stat = readProcStat(pid);
  if (!stat) return null;
  return {
    pid,
    startTime: stat.startTime,
    pgid: stat.pgid,
    sid: stat.sid,
    command: readCommand(pid),
  };
}

export function processStartTime(pid: number): string | null {
  return readProcStat(pid)?.startTime ?? null;
}

export function processGroupOf(pid: number): number | null {
  return readProcStat(pid)?.pgid ?? null;
}

/** Reads direct children from /proc; DSH's own workers are sessions, not child processes. */
export function childPids(pid: number): number[] {
  const children: number[] = [];
  try {
    for (const task of readdirSync(`/proc/${pid}/task`)) {
      try {
        const raw = readFileSync(join('/proc', String(pid), 'task', task, 'children'), 'utf8');
        for (const value of raw.trim().split(/\s+/)) {
          if (value) children.push(Number(value));
        }
      } catch {
        // A task exited between readdir and read.
      }
    }
  } catch {
    return [];
  }
  return [...new Set(children)].filter((child) => Number.isInteger(child) && child > 1);
}

function identityKey(identity: Pick<ProcessIdentity, 'pid' | 'startTime'>): string {
  return `${identity.pid}:${identity.startTime}`;
}

function identityAlive(identity: Pick<ProcessIdentity, 'pid' | 'startTime'>): boolean {
  if (!pidAlive(identity.pid)) return false;
  const current = processStartTime(identity.pid);
  return current !== null && current === identity.startTime;
}

/** All live processes in a process group, including a child that outlived its parent. */
export function groupMembers(pgid: number): ProcessIdentity[] {
  if (!Number.isInteger(pgid) || pgid <= 1) return [];
  const members: ProcessIdentity[] = [];
  let entries: string[];
  try {
    entries = readdirSync('/proc');
  } catch {
    return [];
  }
  for (const entry of entries) {
    if (!/^\d+$/.test(entry)) continue;
    const pid = Number(entry);
    const identity = identifyProcess(pid);
    if (identity && identity.pgid === pgid) members.push(identity);
  }
  return members;
}

function normalizeStored(stored: StoredOwnership | undefined): ProcessIdentity[] {
  const result: ProcessIdentity[] = [];
  for (const member of stored?.members ?? []) {
    if (!member || typeof member.pid !== 'number' || typeof member.startTime !== 'string') {
      continue;
    }
    if (!identityAlive({ pid: member.pid, startTime: member.startTime })) continue;
    const current = identifyProcess(member.pid);
    if (current) result.push(current);
  }
  return result;
}

/**
 * Accepts a root only when its saved start time still matches. A live PID with
 * a different start time is a reused PID and must never be signalled.
 */
function verifiedRoot(rootPid: number, stored?: StoredOwnership): ProcessIdentity | null {
  const current = identifyProcess(rootPid);
  if (!current) return null;
  if (!stored) return current;
  const saved = (stored.members ?? []).find(
    (member) => member?.pid === rootPid && typeof member.startTime === 'string',
  );
  if (!saved || saved.startTime !== current.startTime) return null;
  return current;
}

export const CGROUP_ROOT = process.env.MOYU_CGROUP_ROOT ?? '/sys/fs/cgroup/moyu-supervisor';

/** Creates a per-instance cgroup. It is the durable boundary for setsid/setpgid children. */
export function ensureCgroup(profile: string): string | null {
  const path = join(CGROUP_ROOT, profile.replaceAll(/[^a-zA-Z0-9_.-]/g, '-'));
  try {
    // The memory controller must be delegated before the child cgroup exists,
    // otherwise memory.current is absent and the instance cannot be metered.
    enableSubtreeControllers(CGROUP_ROOT);
    mkdirSync(path, { recursive: true, mode: 0o755 });
    // The kernel creates cgroup.procs; reading proves the hierarchy is usable.
    readFileSync(join(path, 'cgroup.procs'), 'utf8');
    return path;
  } catch {
    return null;
  }
}

/** The launcher accepts only the parent's exact positive release; EOF and errors exit before exec. */
export const CGROUP_LAUNCHER =
  'IFS= read -r go <&3 || exit 126; [ "$go" = go ] || exit 127; exec "$@"';

/** True when the cgroup's measured `memory.events` report an OOM kill. */
export function cgroupOomKilled(path: string | null | undefined): boolean {
  return (readMemorySample(path).events.oom_kill ?? 0) > 0;
}

export function movePidToCgroup(path: string, pid: number): boolean {
  try {
    writeFileSync(join(path, 'cgroup.procs'), String(pid));
    return true;
  } catch {
    return false;
  }
}

export function cgroupTree(path: string): { ok: boolean; pids: number[]; populated: boolean } {
  const pids: number[] = [];
  const stack = [path];
  let ok = true;
  let populated = false;
  while (stack.length > 0) {
    const dir = stack.pop() as string;
    try {
      const raw = readFileSync(join(dir, 'cgroup.procs'), 'utf8');
      for (const value of raw.split(/\s+/)) {
        const pid = Number(value);
        if (Number.isInteger(pid) && pid > 1) pids.push(pid);
      }
      try {
        const events = readFileSync(join(dir, 'cgroup.events'), 'utf8');
        if (/^populated\s+1$/m.test(events)) populated = true;
      } catch {
        ok = false;
      }
      for (const entry of readdirSync(dir)) {
        const child = join(dir, entry);
        try {
          if (lstatSync(child).isDirectory()) stack.push(child);
        } catch {
          ok = false;
        }
      }
    } catch {
      ok = false;
    }
  }
  const unique = [...new Set(pids)];
  // `populated` covers nested descendants; an unreadable member is not empty.
  if (populated && unique.length === 0) ok = false;
  return { ok, pids: unique, populated };
}

export function cgroupPids(path: string): number[] {
  return cgroupTree(path).pids;
}

export function cgroupEmpty(path: string): { ok: boolean; empty: boolean } {
  const tree = cgroupTree(path);
  return { ok: tree.ok, empty: tree.ok && !tree.populated && tree.pids.length === 0 };
}

export function removeCgroup(path: string | null | undefined): void {
  if (!path) return;
  try {
    for (const entry of readdirSync(path)) {
      const child = join(path, entry);
      try {
        if (statSync(child).isDirectory()) removeCgroup(child);
      } catch {
        // Pseudo-files are expected here.
      }
    }
    if (cgroupPids(path).length === 0) rmdirSync(path);
  } catch {
    // A non-empty or partially removed cgroup is kept for a later retry.
  }
}

/** Processes in the saved session, even when they changed process group with setpgid. */
export function sessionMembers(sid: number): ProcessIdentity[] {
  if (!Number.isInteger(sid) || sid <= 1) return [];
  const members: ProcessIdentity[] = [];
  let entries: string[];
  try {
    entries = readdirSync('/proc');
  } catch {
    return [];
  }
  for (const entry of entries) {
    if (!/^\d+$/.test(entry)) continue;
    const identity = identifyProcess(Number(entry));
    if (identity && identity.sid === sid) members.push(identity);
  }
  return members;
}

/**
 * Captures the root process, its owned process groups, and every descendant.
 * Start times are compared before a PID or PGID is accepted. When the root has
 * already died, a saved root identity lets us find children that were created
 * between snapshots and inherited its session; group members that cannot be
 * tied to that session are reported as unknown instead of being signalled.
 */
export function collectOwnership(
  rootPid: number,
  stored?: StoredOwnership,
): ProcessOwnershipSnapshot {
  const byKey = new Map<string, ProcessIdentity>();
  const add = (identity: ProcessIdentity | null | undefined) => {
    if (!identity) return;
    byKey.set(identityKey(identity), identity);
  };

  const root = verifiedRoot(rootPid, stored);
  const savedRoot = (stored?.members ?? []).find(
    (member) => member?.pid === rootPid && typeof member.startTime === 'string',
  );
  add(root);
  const normalized = normalizeStored(stored);
  for (const member of normalized) add(member);

  const unknownMembers: ProcessIdentity[] = [];
  const cgroup = stored?.cgroup ?? null;
  let cgroupReadable = true;
  if (cgroup) {
    const tree = cgroupTree(cgroup);
    cgroupReadable = tree.ok;
    for (const pid of tree.pids) add(identifyProcess(pid));
  }
  const ownedGroups = new Set<number>();
  if (root) ownedGroups.add(root.pgid);
  for (const member of normalized) ownedGroups.add(member.pgid);
  const rootReused = !root && pidAlive(rootPid);
  if (!root && !rootReused && savedRoot && typeof savedRoot.pgid === 'number') {
    // The root may have forked a child after the last snapshot and then died.
    // Scan its old group, but only accept members in the root's session; a
    // reused PGID from an unrelated service must not be killed.
    ownedGroups.add(savedRoot.pgid);
  }
  if (rootReused && savedRoot && typeof savedRoot.pgid === 'number') {
    // The root PID has been reused. We cannot prove the old group is ours, so
    // report its members as unknown and never signal them.
    for (const member of groupMembers(savedRoot.pgid)) unknownMembers.push(member);
  }
  if (!stored && rootPid > 1) {
    // Discovery-only calls (tests, descendants()) have no persisted identity.
    ownedGroups.add(rootPid);
  }
  if (savedRoot && typeof savedRoot.sid === 'number' && (!root || !pidAlive(rootPid))) {
    for (const member of sessionMembers(savedRoot.sid)) {
      if (member.pid !== rootPid) add(member);
    }
  }
  if (
    !cgroup &&
    !root &&
    savedRoot &&
    typeof savedRoot.sid === 'number' &&
    pidAlive(rootPid) === false
  ) {
    // Without a cgroup, a setsid child can escape every group/session scan.
    // This is not proof of exit; keep it unknown until the owner clears it.
    unknownMembers.push(
      ...sessionMembers(savedRoot.sid).filter((member) => member.pid !== rootPid),
    );
  }
  const pgid = [...ownedGroups][0] ?? null;
  for (const group of ownedGroups) {
    for (const member of groupMembers(group)) {
      if (!rootReused && (!stored || root || normalized.some((item) => item.pid === member.pid))) {
        add(member);
      } else if (savedRoot && typeof savedRoot.sid === 'number' && member.sid === savedRoot.sid) {
        add(member);
      } else {
        unknownMembers.push(member);
      }
    }
  }

  const visit = (parent: number) => {
    for (const child of childPids(parent)) {
      const identity = identifyProcess(child);
      if (!identity) continue;
      add(identity);
      visit(child);
    }
  };
  if (root) visit(root.pid);
  for (const member of normalized) visit(member.pid);

  return {
    rootPid: root?.pid ?? null,
    pgid,
    members: [...byKey.values()].filter(identityAlive),
    unknown:
      !cgroupReadable ||
      stored?.adoptedUnproven === true ||
      unknownMembers.filter(identityAlive).length > 0 ||
      (!cgroup &&
        !stored?.allowUnproven &&
        !root &&
        !!savedRoot &&
        typeof savedRoot.sid === 'number'),
    unknownMembers: unknownMembers.filter(identityAlive),
  };
}

/**
 * A GitHub Source polls by spawning a short-lived `gh` child. That child is the
 * only process class we treat as disposable; every other descendant, including
 * a service started <5s ago, must be preserved until it exits by itself.
 */
export function isTransientPolling(command: string): boolean {
  const normalized = command.trim().replaceAll(/\s+/g, ' ');
  if (!normalized) return false;
  const tokens = normalized.split(' ');
  const binary = basename(tokens[0] ?? '');
  if (binary !== 'gh' && binary !== 'gh-agent') return false;

  const args = tokens.slice(1);
  const methodIndex = args.findIndex((arg) => arg === '-X' || arg === '--method');
  if (methodIndex >= 0) {
    const method = (args[methodIndex + 1] ?? '').toUpperCase();
    if (method !== 'GET') return false;
  }
  if (
    args.some(
      (arg) => arg === '-f' || arg === '--field' || arg === '--raw-field' || arg === '--input',
    )
  ) {
    return false;
  }
  if (args[0] === 'api') {
    // The Source polls with GET requests; pagination/formatting flags are
    // read-only. Any explicit non-GET method or field was rejected above; an
    // unknown flag keeps the process because its effects are not proven.
    const readOnlyFlags = new Set([
      '--paginate',
      '--slurp',
      '--jq',
      '-q',
      '--template',
      '-t',
      '--include',
      '-i',
      '--cache',
      '--hostname',
    ]);
    const apiArgs = args
      .slice(1)
      .filter((_, index) => index !== methodIndex - 1 && index !== methodIndex);
    const flags = apiArgs.filter((arg) => arg.startsWith('-'));
    return flags.every((flag) => readOnlyFlags.has(flag));
  }
  if (args[0] === 'issue' || args[0] === 'pr') {
    const action = args[1] ?? '';
    return (
      action === 'list' ||
      action === 'view' ||
      action === 'status' ||
      action === 'diff' ||
      action === 'checks'
    );
  }
  if (args[0] === 'search') return true;
  if (args[0] === 'repo') return args[1] === 'view';
  if (args[0] === 'auth') return args[1] === 'status';
  // Anything else, including gh pr create / gh api -X POST, may write or have
  // unknown effects and must be retained.
  return false;
}

/** Applies the retention policy to a captured set; only known polling is disposable. */
export function retainedIdentities(members: ProcessIdentity[]): ProcessIdentity[] {
  return members.filter((member) => !isTransientPolling(member.command));
}

/** Descendants that are work worth preserving; only known polling is excluded. */
export function retainedDescendants(pid: number, stored?: StoredOwnership): ProcessIdentity[] {
  return retainedIdentities(
    collectOwnership(pid, stored).members.filter((member) => member.pid !== pid),
  );
}

/** Recursively lists the live descendants of a harness process. */
export function descendants(pid: number): number[] {
  return collectOwnership(pid)
    .members.filter((member) => member.pid !== pid)
    .map((member) => member.pid);
}

function signalPid(pid: number, signal: NodeJS.Signals): void {
  try {
    process.kill(pid, signal);
  } catch {
    // Already gone.
  }
}

/**
 * Stops a detached runtime and all of its descendants.
 *
 * The result is only `stopped: true` after every tracked identity (root,
 * stored members, process-group members and newly discovered children) is gone.
 * If a child ignores SIGTERM, SIGKILL is sent and the wait continues; a parent
 * exiting first does not release the stop. Every caller must keep the instance
 * in `stopping` and its capacity occupied when this returns false.
 */
export async function stopProcessTree(
  rootPid: number,
  timeoutMs: number,
  stored?: StoredOwnership,
): Promise<StopProcessResult> {
  if (!stored) {
    const initial = collectOwnership(rootPid);
    stored = { rootPid, pgid: initial.pgid, members: initial.members, allowUnproven: true };
  }
  const merged = new Map<string, ProcessIdentity>();
  let unknown = false;
  const merge = (identity: ProcessIdentity | null | undefined) => {
    if (identity && identityAlive(identity)) merged.set(identityKey(identity), identity);
  };
  const refresh = () => {
    const snapshot = collectOwnership(rootPid, {
      rootPid: stored?.rootPid ?? rootPid,
      pgid: stored?.pgid ?? rootPid,
      cgroup: stored?.cgroup ?? null,
      allowUnproven: stored?.allowUnproven,
      adoptedUnproven: stored?.adoptedUnproven,
      members: [...(stored?.members ?? []), ...merged.values()],
    });
    for (const member of snapshot.members) merge(member);
    if (stored?.cgroup) {
      const tree = cgroupTree(stored.cgroup);
      for (const pid of tree.pids) merge(identifyProcess(pid));
      if (!tree.ok) unknown = true;
    }
    unknown = unknown || snapshot.unknown;
    return snapshot.pgid;
  };

  let pgid = refresh();
  if (!pidAlive(rootPid) && merged.size === 0) {
    return { stopped: !unknown, rootPid, pgid, remaining: [], unknown };
  }

  const startedAt = Date.now();
  const termSent = new Set<string>();
  let killSent = false;
  const send = (signal: NodeJS.Signals) => {
    for (const member of [...merged.values()]) {
      const key = identityKey(member);
      if (signal === 'SIGTERM' && termSent.has(key)) continue;
      // Re-verify immediately before every signal: a process may have exited
      // and its PID/PGID reused since it was recorded. Never signal a group
      // here; an unrelated member could share a reused PGID.
      const current = identifyProcess(member.pid);
      if (!current || current.startTime !== member.startTime) {
        merged.delete(key);
        continue;
      }
      if (signal === 'SIGTERM') termSent.add(key);
      signalPid(member.pid, signal);
    }
  };

  send('SIGTERM');
  const killAt = startedAt + Math.max(500, Math.floor(timeoutMs / 2));
  const deadline = startedAt + timeoutMs;

  for (;;) {
    await delay(100);
    pgid = refresh();
    if (!killSent && Date.now() >= killAt) {
      killSent = true;
      send('SIGKILL');
    }
    const alive = [...merged.values()].filter(identityAlive);
    if (alive.length === 0) {
      // One final scan closes the race where a child forked just before the
      // parent exited and was not observed in the previous iteration.
      refresh();
      const finalAlive = [...merged.values()].filter(identityAlive);
      const cgroupState = stored?.cgroup ? cgroupEmpty(stored.cgroup) : { ok: true, empty: true };
      if (finalAlive.length === 0 && cgroupState.ok && cgroupState.empty) {
        // An empty, readable cgroup is the execution boundary's own proof.
        // `unknown` can be a PID/PGID reused outside it; do not keep capacity
        // occupied for members that cannot be inside the metered cgroup. A
        // caller without a cgroup still needs the unknown boundary to block.
        const boundaryProven = Boolean(stored?.cgroup) && cgroupState.ok && cgroupState.empty;
        removeCgroup(stored?.cgroup);
        return { stopped: boundaryProven || !unknown, rootPid, pgid, remaining: [], unknown };
      }
      merged.clear();
      for (const member of finalAlive) merged.set(identityKey(member), member);
    }
    if (Date.now() >= deadline) {
      return {
        stopped: false,
        rootPid,
        pgid,
        remaining: [...merged.values()].filter(identityAlive),
        unknown,
      };
    }
  }
}

export async function waitForExit(pid: number, timeoutMs: number): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (!pidAlive(pid)) return true;
    await delay(150);
  }
  return !pidAlive(pid);
}

export async function waitForPortClosed(port: number, timeoutMs: number): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await portFree(port)) return true;
    await delay(150);
  }
  return portFree(port);
}
