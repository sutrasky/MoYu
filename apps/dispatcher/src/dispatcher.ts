import { setTimeout as delay } from 'node:timers/promises';

import type pg from 'pg';

import { createPool } from '@moyu/db';
import type { RuntimeBridgeClient } from '@moyu/runtime-client';

import type { DispatcherConfig } from './config';
import { admitImportedOnce } from './admissions';
import { deliverOnce } from './delivery';
import { createConsoleLogger, type DispatcherLogger } from './logger';
import { maintainLeases, operationsOnce, reconcileTasks } from './operations';
import { captureReportsOnce } from './reports';
import { duplicatesOnce } from './duplicates';
import { bridgeClient, listActiveRepositories, type RepositoryBinding } from './runtime';
import { syncOnce } from './sync';

export interface RunDispatcherOptions {
  signal: AbortSignal;
  logger?: DispatcherLogger;
}

async function runLoop(
  name: string,
  intervalMs: number,
  signal: AbortSignal,
  logger: DispatcherLogger,
  work: () => Promise<boolean>,
): Promise<void> {
  while (!signal.aborted) {
    let busy = false;
    try {
      busy = await work();
    } catch (error) {
      logger.error(
        {
          event: `dispatcher.${name}_error`,
          error: error instanceof Error ? error.message : String(error),
        },
        `${name} loop failed; retrying`,
      );
    }
    if (!busy) {
      await delay(intervalMs, undefined, { signal }).catch(() => undefined);
    }
  }
}

/**
 * One independent loop set per online repository. Each has its own Bridge
 * client, credential and scoped cursor; a stopped repository's loops are
 * aborted, so its messages can never be delivered to another runtime.
 */
async function runRepositoryWorker(
  pool: pg.Pool,
  config: DispatcherConfig,
  repository: RepositoryBinding,
  signal: AbortSignal,
  logger: DispatcherLogger,
): Promise<void> {
  let bridge: RuntimeBridgeClient;
  try {
    bridge = bridgeClient(config, repository);
  } catch (error) {
    logger.error(
      {
        event: 'dispatcher.bridge_unconfigured',
        repository: repository.fullName,
        error: error instanceof Error ? error.message : String(error),
      },
      'refusing to dispatch a repository without its own verified Bridge credentials',
    );
    return;
  }

  let admissionSupported = false;
  try {
    const capabilities = await bridge.capabilities();
    admissionSupported = capabilities.capabilities.admission === true;
  } catch (error) {
    logger.warn(
      {
        event: 'dispatcher.capabilities_unavailable',
        repository: repository.fullName,
        error: error instanceof Error ? error.message : String(error),
      },
      'runtime capabilities are unknown; execution admission stays on the platform control plane',
    );
  }
  logger.info(
    { event: 'dispatcher.repository', repository: repository.fullName, admissionSupported },
    'dispatching for repository runtime',
  );

  if (config.reconcileOnStart) {
    try {
      await reconcileTasks(pool, repository, bridge, logger);
    } catch (error) {
      logger.error(
        {
          event: 'dispatcher.reconcile_error',
          repository: repository.fullName,
          error: error instanceof Error ? error.message : String(error),
        },
        'startup reconciliation failed; loops continue with the stored state',
      );
    }
  }

  await Promise.allSettled([
    runLoop('delivery', config.deliveryIntervalMs, signal, logger, () =>
      deliverOnce(pool, config, repository, bridge, logger, admissionSupported),
    ),
    runLoop('event_sync', config.eventSyncIntervalMs, signal, logger, () =>
      syncOnce(pool, repository, bridge, logger),
    ),
    runLoop('operations', config.operationIntervalMs, signal, logger, () =>
      operationsOnce(pool, config, repository, bridge, logger, admissionSupported),
    ),
    runLoop('leases', 5000, signal, logger, async () => {
      await maintainLeases(pool, config, repository, bridge, logger);
      return false;
    }),
    runLoop('admissions', 2000, signal, logger, () =>
      admitImportedOnce(pool, config, repository, bridge, admissionSupported, logger),
    ),
    runLoop('reports', config.reportCaptureIntervalMs, signal, logger, () =>
      captureReportsOnce(pool, repository, bridge, logger),
    ),
    runLoop('duplicates', config.duplicateIntervalMs, signal, logger, () =>
      duplicatesOnce(pool, config, repository, bridge, logger),
    ),
  ]);
}

interface WorkerHandle {
  key: string;
  controller: AbortController;
  task: Promise<void>;
}

function bindingKey(repository: RepositoryBinding): string {
  return `${repository.bridgeUrl ?? ''}|${repository.bridgeCredentialRef ?? ''}`;
}

/**
 * M5 dispatcher: discovers online repository instances from the supervisor
 * projection and runs an independent loop set for each. Restarting the
 * dispatcher resumes every repository from its own persisted Bridge cursor.
 */
export async function runDispatcher(
  config: DispatcherConfig,
  options: RunDispatcherOptions,
): Promise<void> {
  const logger = options.logger ?? createConsoleLogger('moyu-dispatcher');
  const pool = createPool(config.databaseUrl, { max: 8 });
  const workers = new Map<string, WorkerHandle>();

  try {
    await pool.query('select 1');
    logger.info(
      { event: 'dispatcher.ready', nodeEnv: config.nodeEnv },
      'dispatcher connected to PostgreSQL',
    );

    const stopWorkers = async () => {
      for (const worker of workers.values()) worker.controller.abort();
      await Promise.allSettled([...workers.values()].map((worker) => worker.task));
      workers.clear();
    };

    while (!options.signal.aborted) {
      try {
        const repositories = await listActiveRepositories(pool, config);
        const seen = new Set(repositories.map((repository) => repository.id));
        for (const [id, worker] of workers) {
          if (!seen.has(id)) {
            logger.info(
              { event: 'dispatcher.repository_offline', repositoryId: id },
              'repository runtime is no longer online; stopping its loops',
            );
            worker.controller.abort();
            workers.delete(id);
          }
        }
        for (const repository of repositories) {
          const existing = workers.get(repository.id);
          if (existing && existing.key === bindingKey(repository)) continue;
          if (existing) {
            existing.controller.abort();
            workers.delete(repository.id);
          }
          const controller = new AbortController();
          const task = runRepositoryWorker(
            pool,
            config,
            repository,
            controller.signal,
            logger,
          ).catch((error) => {
            logger.error(
              {
                event: 'dispatcher.worker_failed',
                repository: repository.fullName,
                error: error instanceof Error ? error.message : String(error),
              },
              'repository dispatcher worker stopped unexpectedly',
            );
          });
          workers.set(repository.id, {
            key: bindingKey(repository),
            controller,
            task,
          });
        }
      } catch (error) {
        logger.error(
          {
            event: 'dispatcher.discovery_error',
            error: error instanceof Error ? error.message : String(error),
          },
          'could not read online repository instances; retrying',
        );
      }
      await delay(2000, undefined, { signal: options.signal }).catch(() => undefined);
    }

    await stopWorkers();
    logger.info({ event: 'dispatcher.stopped' }, 'dispatcher stopped');
  } finally {
    await pool.end();
  }
}
