import { createHash, randomBytes } from 'node:crypto';
import {
  chmodSync,
  cpSync,
  existsSync,
  mkdirSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from 'node:fs';
import { join } from 'node:path';

import type { SupervisorConfig } from './config';
import type { RepositoryRow, SupervisorStore } from './store';
import { portFree } from './process';

export interface PreparedInstance {
  repository: RepositoryRow;
  profile: string;
  dshPort: number;
  bridgePort: number;
  envFile: string;
  credentialsFile: string;
  bridgeToken: string;
  callbackToken: string;
}

function ensurePrivateDir(path: string): void {
  mkdirSync(path, { recursive: true, mode: 0o700 });
  chmodSync(path, 0o700);
}

function writePrivate(path: string, content: string): void {
  writeFileSync(path, content, { mode: 0o600 });
  chmodSync(path, 0o600);
}

/** The managed instance environment written under `INSTANCE_ENV_DIR`. */
export function instanceEnvironment(input: {
  platformUrl: string;
  platformToken: string;
  bridgeToken: string;
  bridgePort: number;
}): string {
  return [
    `MOYU_PLATFORM_URL=${input.platformUrl}`,
    `MOYU_PLATFORM_TOKEN=${input.platformToken}`,
    `MOYU_BRIDGE_TOKEN=${input.bridgeToken}`,
    'MOYU_BRIDGE_HOST=127.0.0.1',
    `MOYU_BRIDGE_PORT=${input.bridgePort}`,
    // Managed instances always enforce execution admission; the capability
    // is advertised only when this flag is on.
    'MOYU_REQUIRE_ADMISSION=1',
    '',
  ].join('\n');
}

export function parseEnvFile(path: string): Record<string, string> {
  const values: Record<string, string> = {};
  for (const line of readFileSync(path, 'utf8').split('\n')) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith('#')) continue;
    const index = trimmed.indexOf('=');
    if (index <= 0) throw new Error(`Malformed instance env line in ${path}`);
    values[trimmed.slice(0, index).trim()] = trimmed.slice(index + 1).trim();
  }
  return values;
}

function sha256(value: string): string {
  return createHash('sha256').update(value, 'utf8').digest('hex');
}

/**
 * Allocates a fixed profile/port pair. A pinned mapping is the normal deployment
 * path; the sequential allocator only supports adding a repository on demand
 * from the same fixed template.
 */
async function allocate(
  config: SupervisorConfig,
  store: SupervisorStore,
  repository: RepositoryRow,
): Promise<{ profile: string; dshPort: number; bridgePort: number }> {
  const pinned = config.pinned.get(repository.fullName);
  if (pinned) {
    return { profile: pinned.profile, dshPort: pinned.dshPort, bridgePort: pinned.bridgePort };
  }
  const existing = await store.getInstance(repository.id);
  if (existing?.profile && existing.port) {
    return {
      profile: existing.profile,
      dshPort: existing.port,
      bridgePort: existing.port + 1,
    };
  }
  if (!config.profileTemplateDir) {
    throw new Error(
      'No pinned instance mapping and no PROFILE_TEMPLATE_DIR; cannot prepare this repository',
    );
  }
  const instances = await store.listInstances();
  const used = new Set(
    instances.map((row) => row.profile).filter((value): value is string => !!value),
  );
  // A sleeping instance still owns its profile and both ports. Reserving only
  // the profile would allow another repository to take a port it still needs
  // (for example a swapped dsh/bridge pair) and overwrite its Source state.
  const usedPorts = new Set<number>();
  for (const row of instances) {
    if (row.port) usedPorts.add(row.port);
    if (row.bridgePort) usedPorts.add(row.bridgePort);
  }
  for (const instance of config.pinned.values()) {
    used.add(instance.profile);
    usedPorts.add(instance.dshPort);
    usedPorts.add(instance.bridgePort);
  }
  const pairCount = Math.floor((config.portRange.end - config.portRange.start) / 2);
  for (let index = 0; index < pairCount; index += 1) {
    const profile = `${config.profilePrefix}-${index + 1}`;
    const dshPort = config.portRange.start + index * 2;
    const bridgePort = dshPort + 1;
    if (used.has(profile)) continue;
    if (usedPorts.has(dshPort) || usedPorts.has(bridgePort)) continue;
    if (!(await portFree(dshPort)) || !(await portFree(bridgePort))) continue;
    return { profile, dshPort, bridgePort };
  }
  throw new Error('No free supervisor profile/port pair in the configured range');
}

function installProfile(config: SupervisorConfig, profile: string, dshPort: number): string {
  const profileDir = join(config.dshHome, 'profiles', profile);
  if (!existsSync(profileDir)) {
    if (!config.profileTemplateDir || !existsSync(config.profileTemplateDir)) {
      throw new Error(`Profile ${profile} does not exist and the template is unavailable`);
    }
    cpSync(config.profileTemplateDir, profileDir, { recursive: true });
    const patchPath = join(profileDir, 'cordis.patch.yml');
    if (!existsSync(patchPath)) {
      rmSync(profileDir, { recursive: true, force: true });
      throw new Error(`Profile template has no cordis.patch.yml: ${config.profileTemplateDir}`);
    }
    const patch = readFileSync(patchPath, 'utf8')
      .replaceAll('__DSH_PORT__', String(dshPort))
      .replaceAll('__STATE_DIR__', join(config.dshHome, 'state', profile));
    writePrivate(patchPath, patch);
    chmodSync(profileDir, 0o700);
  }
  return profileDir;
}

/**
 * Prepares the credential boundary and the profile for one repository. Idempotent:
 * a repeated add request reuses the same token file and profile, so a concurrent
 * wake cannot create a second instance or rotate a token under a running worker.
 */
export async function prepareInstance(
  config: SupervisorConfig,
  store: SupervisorStore,
  repositoryId: string,
): Promise<PreparedInstance> {
  const repository = await store.getRepository(repositoryId);
  if (!repository) throw new Error('Repository not found');

  ensurePrivateDir(config.credentialsDir);
  ensurePrivateDir(config.instanceEnvDir);
  ensurePrivateDir(config.logDir);
  ensurePrivateDir(join(config.worktreeRoot));

  const { profile, dshPort, bridgePort } = await allocate(config, store, repository);
  const profileDir = installProfile(config, profile, dshPort);
  if (!existsSync(join(profileDir, 'node_modules', 'dsh-superagent'))) {
    throw new Error(`Profile ${profile} has no dsh-superagent installation`);
  }

  const credentialsFile = join(config.credentialsDir, `${repository.id}.json`);
  const envFile = join(config.instanceEnvDir, `${repository.id}.env`);
  let credentials: { bridgeToken: string; callbackToken: string } | null = null;
  if (existsSync(credentialsFile)) {
    const parsed = JSON.parse(readFileSync(credentialsFile, 'utf8')) as {
      bridgeToken?: string;
      callbackToken?: string;
    };
    if (parsed.bridgeToken && parsed.callbackToken) {
      credentials = { bridgeToken: parsed.bridgeToken, callbackToken: parsed.callbackToken };
    }
  }
  if (!credentials) {
    credentials = {
      bridgeToken: randomBytes(32).toString('base64url'),
      callbackToken: randomBytes(32).toString('base64url'),
    };
  }

  const bridgeUrl = `http://127.0.0.1:${bridgePort}`;
  writePrivate(credentialsFile, `${JSON.stringify({ bridgeUrl, ...credentials }, null, 2)}\n`);
  writePrivate(
    envFile,
    instanceEnvironment({
      platformUrl: config.platformUrl,
      platformToken: credentials.callbackToken,
      bridgeToken: credentials.bridgeToken,
      bridgePort,
    }),
  );

  await store.setBridgeBinding(repository.id, {
    bridgeUrl,
    credentialRef: `file:${repository.id}.json`,
    callbackTokenHash: sha256(credentials.callbackToken),
  });
  await store.createInstance(repository.id, 'sleeping', 'sleeping', 'never_started', {
    profile,
    port: dshPort,
    bridgePort,
  });
  await store.updateInstance(repository.id, {
    profile,
    port: dshPort,
    bridgePort,
    reasonCode: 'sleeping',
    reasonDetail: null,
    waitingFor: null,
  });

  return {
    repository,
    profile,
    dshPort,
    bridgePort,
    envFile,
    credentialsFile,
    ...credentials,
  };
}

export function instanceLogPath(config: SupervisorConfig, profile: string): string {
  return join(config.logDir, `${profile}.log`);
}
