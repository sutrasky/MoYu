#!/usr/bin/env node
/* global Buffer */
/**
 * Bounded memory-hog fixture for the M5-B cgroup acceptance test.
 *
 * It refuses to allocate unless its own cgroup has a small enforced memory.max,
 * so a delegation mistake cannot turn into an unbounded host allocation. Run it
 * through systemd-run inside the isolated M5-B slice, never directly.
 */
import { readFileSync } from 'node:fs';

const pidCgroup = readFileSync('/proc/self/cgroup', 'utf8');
const match = /^0::(.+)$/m.exec(pidCgroup);
if (!match) {
  console.error('cannot resolve own cgroup');
  process.exit(2);
}
const path = `/sys/fs/cgroup${match[1]}`;
let limit;
try {
  limit = readFileSync(`${path}/memory.max`, 'utf8').trim();
} catch {
  console.error(`cannot read ${path}/memory.max; refusing to allocate`);
  process.exit(2);
}
if (limit === 'max' || !/^\d+$/.test(limit) || Number(limit) > 256 * 1024 * 1024) {
  console.error(`own cgroup limit is ${limit}; refusing an unbounded allocation`);
  process.exit(2);
}
console.log(`allocating inside ${path} with memory.max=${limit}`);
const blocks = [];
for (;;) blocks.push(Buffer.alloc(8 * 1024 * 1024, 1));
