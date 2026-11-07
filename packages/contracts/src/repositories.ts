import { z } from 'zod';

import { isoTimestampSchema, uuidSchema } from './common';
import { executionModeSchema, modelSelectionSchema } from './tasks';

export const repositoryStatusSchema = z.enum(['provisioning', 'active', 'sleeping', 'error']);

export type RepositoryStatus = z.infer<typeof repositoryStatusSchema>;

/** Lifecycle state stored by the host supervisor for one repository runtime. */
export const runtimeInstanceStateSchema = z.enum([
  'sleeping',
  'starting',
  'online',
  'stopping',
  'fault',
]);

export type RuntimeInstanceState = z.infer<typeof runtimeInstanceStateSchema>;

export const runtimeDesiredStateSchema = z.enum(['sleeping', 'online']);

export type RuntimeDesiredState = z.infer<typeof runtimeDesiredStateSchema>;

/** One allowed owner operation on a repository instance. */
export const repositoryRuntimeActionSchema = z.object({
  action: z.enum(['wake', 'sleep', 'retry', 'retry_stop']),
  label: z.string().min(1),
  enabled: z.boolean(),
  unavailableReason: z.string().nullable(),
});

export type RepositoryRuntimeAction = z.infer<typeof repositoryRuntimeActionSchema>;

/** Measured cgroup memory for one instance; never an estimate labelled as a measurement. */
export const runtimeResourceSchema = z.object({
  state: z.enum(['ok', 'resource_wait', 'over_budget', 'oom', 'unknown']),
  detail: z.string().nullable(),
  memoryCurrentBytes: z.number().int().nonnegative().nullable(),
  memoryPeakBytes: z.number().int().nonnegative().nullable(),
  memoryLimitBytes: z.number().int().nonnegative().nullable(),
  oomKillCount: z.number().int().nonnegative(),
  checkedAt: isoTimestampSchema.nullable(),
});

export type RuntimeResource = z.infer<typeof runtimeResourceSchema>;

/** The repository execution lease generation visible to the owner. */
export const runtimeLeaseSchema = z.object({
  /** Decimal string because the fencing token is a bigint. */
  fencingToken: z.string().regex(/^(0|[1-9][0-9]*)$/),
  recoveryState: z.string().min(1),
  waitingFor: z.string().nullable(),
  detail: z.string().nullable(),
  stopProvenAt: isoTimestampSchema.nullable(),
});

export type RuntimeLeaseProjection = z.infer<typeof runtimeLeaseSchema>;

/**
 * The repository runtime projection. `reason` is already sanitised for display;
 * `reasonCode`/`waitingFor` stay machine-readable so the UI does not infer a
 * cause or invent an ETA. Unknown or stale facts are represented as unknown,
 * never as completion.
 */
export const repositoryRuntimeSchema = z.object({
  state: runtimeInstanceStateSchema,
  desiredState: runtimeDesiredStateSchema,
  reasonCode: z.string().min(1),
  reason: z.string().min(1),
  waitingFor: z.string().nullable(),
  /** Whether the instance's GitHub listener is active; paused while sleeping. */
  listener: z.enum(['active', 'paused', 'unknown']),
  /** The repository's single execution slot, independent of online capacity. */
  executionSlot: z.enum(['free', 'busy', 'unknown']),
  lastProgressAt: isoTimestampSchema.nullable(),
  lastActivityAt: isoTimestampSchema.nullable(),
  lastStartedAt: isoTimestampSchema.nullable(),
  lastStoppedAt: isoTimestampSchema.nullable(),
  lastHealthAt: isoTimestampSchema.nullable(),
  /** Measured memory of this instance, when the supervisor has a cgroup sample. */
  resource: runtimeResourceSchema.nullable(),
  /** Current execution lease generation, when one exists for this repository. */
  lease: runtimeLeaseSchema.nullable(),
  actions: z.array(repositoryRuntimeActionSchema),
});

export type RepositoryRuntime = z.infer<typeof repositoryRuntimeSchema>;

export const repositorySummarySchema = z.object({
  id: uuidSchema,
  fullName: z.string().min(1),
  githubRepositoryId: z.number().int().nullable(),
  defaultBranch: z.string().min(1),
  status: repositoryStatusSchema,
  /** Present on M5 responses; older responses may omit it. */
  runtime: repositoryRuntimeSchema.nullable().optional(),
  createdAt: isoTimestampSchema,
  updatedAt: isoTimestampSchema,
});

export type RepositorySummary = z.infer<typeof repositorySummarySchema>;

export const repositoryListDataSchema = z.object({
  repositories: z.array(repositorySummarySchema),
});

/**
 * Per-repository task defaults. A saved preference never changes a task that is
 * already frozen: the conversation and task copy the chosen values at creation.
 */
export const repositoryPreferencesSchema = z.object({
  executionMode: executionModeSchema,
  baseBranch: z.string().trim().min(1).max(255),
  model: modelSelectionSchema.nullable(),
});

export type RepositoryPreferences = z.infer<typeof repositoryPreferencesSchema>;

export const repositoryPreferencesInputSchema = repositoryPreferencesSchema.strict();

export type RepositoryPreferencesInput = z.infer<typeof repositoryPreferencesInputSchema>;

export const repositoryPreferencesDataSchema = z.object({
  repositoryId: uuidSchema,
  /** The stored preference, or null when the repository defaults are in use. */
  preferences: repositoryPreferencesSchema.nullable(),
  /** The values the next task would actually use, including repository defaults. */
  effective: repositoryPreferencesSchema.extend({
    source: z.enum(['saved', 'repository']),
  }),
  updatedAt: isoTimestampSchema.nullable(),
});

export type RepositoryPreferencesData = z.infer<typeof repositoryPreferencesDataSchema>;

export const repositoryBranchSchema = z.object({
  name: z.string().min(1),
  commit: z.string().nullable(),
});

export type RepositoryBranch = z.infer<typeof repositoryBranchSchema>;

/**
 * Branch availability as reported by the repository runtime. `unsupported`
 * means the runtime cannot list branches at all, so the UI must not claim that
 * a branch is missing; it can only show that availability is unverified.
 */
export const repositoryBranchListDataSchema = z.object({
  repositoryId: uuidSchema,
  status: z.enum(['ready', 'unsupported']),
  branches: z.array(repositoryBranchSchema),
  hasMore: z.boolean(),
  checkedAt: isoTimestampSchema,
});

export type RepositoryBranchListData = z.infer<typeof repositoryBranchListDataSchema>;

/** A GitHub repository the dedicated account can access and may be added. */
export const githubRepositorySummarySchema = z.object({
  fullName: z.string().min(1),
  defaultBranch: z.string().min(1),
  private: z.boolean(),
  permissions: z.object({
    push: z.boolean(),
    admin: z.boolean(),
  }),
});

export type GithubRepositorySummary = z.infer<typeof githubRepositorySummarySchema>;

export const githubRepositoryListQuerySchema = z.object({
  q: z.string().trim().max(140).optional(),
  limit: z.coerce.number().int().min(1).max(100).optional(),
});

export const githubRepositoryListDataSchema = z.object({
  repositories: z.array(githubRepositorySummarySchema),
  hasMore: z.boolean(),
});

export type GithubRepositoryListData = z.infer<typeof githubRepositoryListDataSchema>;

/** Adds one existing GitHub repository by full name after a permission check. */
export const createRepositoryInputSchema = z
  .object({
    fullName: z
      .string()
      .trim()
      .regex(/^[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+$/, 'must be owner/name'),
  })
  .strict();

export type CreateRepositoryInput = z.infer<typeof createRepositoryInputSchema>;

export const repositoryRuntimeDataSchema = z.object({
  repositoryId: uuidSchema,
  runtime: repositoryRuntimeSchema,
});

export type RepositoryRuntimeData = z.infer<typeof repositoryRuntimeDataSchema>;

/** Result of a wake/sleep request; a duplicate request resolves to the same lifecycle. */
export const repositoryActionDataSchema = z.object({
  repositoryId: uuidSchema,
  accepted: z.boolean(),
  duplicate: z.boolean(),
  runtime: repositoryRuntimeSchema,
});

export type RepositoryActionData = z.infer<typeof repositoryActionDataSchema>;
