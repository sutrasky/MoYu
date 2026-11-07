import { z } from 'zod';

import { isoTimestampSchema, uuidSchema } from './common';
import { taskSummarySchema } from './tasks';

/**
 * Machine callback from the repository's private Bridge to the platform. Only
 * the Bridge's workspace Channel uses it in M3: acknowledgements, questions,
 * results and notices that must appear in the conversation.
 */
export const channelMessageEventSchema = z
  .object({
    sourceEventId: z.string().min(1).max(256),
    type: z.literal('channel.message'),
    conversationId: uuidSchema,
    ticketId: z.string().min(1).nullable(),
    triggerId: z.string().min(1).nullable(),
    messageKind: z.enum(['acknowledgement', 'question', 'reply', 'result', 'notice']),
    text: z.string().min(1).max(20_000),
    at: isoTimestampSchema,
  })
  .strict();

export const runtimeEventInputSchema = z.discriminatedUnion('type', [channelMessageEventSchema]);

export type RuntimeEventInput = z.infer<typeof runtimeEventInputSchema>;

export const runtimeEventDataSchema = z.object({
  messageId: uuidSchema,
  duplicate: z.boolean(),
  task: taskSummarySchema.nullable(),
});

export type RuntimeEventData = z.infer<typeof runtimeEventDataSchema>;

/** Capability document served by the runtime Bridge. */
export const bridgeCapabilitiesSchema = z.object({
  protocolVersion: z.string().min(1),
  upstream: z.object({
    package: z.string().optional(),
    version: z.string().optional(),
    commit: z.string().nullable().optional(),
  }),
  capabilities: z.object({
    trigger: z.boolean(),
    events: z.boolean(),
    tickets: z.boolean(),
    models: z.boolean(),
    pause: z.boolean(),
    resume: z.boolean(),
    retry: z.boolean(),
    readOnlyDiscussion: z.boolean(),
    /** M4: discussion tickets can be started into execution; absent on an M3 runtime. */
    start: z.boolean().optional(),
    /** M4-UX: the runtime can list repository branches; absent on runtimes without the patch. */
    branches: z.boolean().optional(),
    /** M5-B: execution requests carry a lease admission and un-admitted Source tickets wait. */
    admission: z.boolean().optional(),
  }),
  source: z.object({ kind: z.string(), enabled: z.boolean() }),
});

export type BridgeCapabilities = z.infer<typeof bridgeCapabilitiesSchema>;

/** One branch as reported by the repository runtime's read-only listing. */
export const bridgeBranchSchema = z.object({
  name: z.string().min(1),
  /** Branch head commit, or an empty string when the runtime could not report it. */
  commit: z.string().nullable().optional(),
});

export const bridgeBranchListSchema = z.object({
  branches: z.array(bridgeBranchSchema),
  hasMore: z.boolean().optional(),
});

export type BridgeBranchList = z.infer<typeof bridgeBranchListSchema>;

export const bridgeModelSchema = z.object({
  provider: z.string().min(1),
  model: z.string().min(1),
  displayName: z.string().min(1).nullable().optional(),
  reasoningEfforts: z.array(z.string()).optional(),
});

export const bridgeModelListSchema = z.object({ models: z.array(bridgeModelSchema) });

export type BridgeModelList = z.infer<typeof bridgeModelListSchema>;

export const bridgeAdmissionSchema = z
  .object({
    leaseId: uuidSchema,
    fencingToken: z.string().regex(/^(0|[1-9][0-9]*)$/),
  })
  .strict();

export type BridgeAdmission = z.infer<typeof bridgeAdmissionSchema>;

export const bridgeTriggerInputSchema = z
  .object({
    triggerId: z.string().min(1).max(256),
    conversationId: uuidSchema,
    messageId: uuidSchema,
    replyToMessageId: uuidSchema.nullable(),
    actor: z.object({ id: z.string().min(1), name: z.string().optional() }),
    text: z.string().min(1).max(20_000),
    at: isoTimestampSchema,
    /**
     * Continues an existing ticket (an imported GitHub conversation) instead of
     * opening the message in a new one. The Bridge uses that ticket's origin
     * thread and frozen model/base, and replies go back to this workspace Channel.
     */
    targetTicketId: z.string().min(1).max(200).optional(),
    admission: bridgeAdmissionSchema.optional(),
    executionSpec: z.object({
      repository: z.string().regex(/^[^/\s]+\/[^/\s]+$/),
      baseBranch: z.string().min(1),
      baseCommit: z
        .string()
        .regex(/^[0-9a-f]{7,64}$/)
        .nullable(),
      model: z
        .object({
          provider: z.string().min(1),
          model: z.string().min(1),
          reasoningEffort: z.string().min(1).nullable().optional(),
        })
        .nullable(),
      mode: z.enum(['execute', 'discuss']),
    }),
  })
  .strict();

export type BridgeTriggerInput = z.infer<typeof bridgeTriggerInputSchema>;

export const bridgeTriggerResultSchema = z.object({
  result: z.enum(['stored', 'duplicate', 'filtered']),
  triggerId: z.string().min(1),
  baseCommit: z.string().nullable(),
  ticketId: z.string().nullable(),
});

export type BridgeTriggerResult = z.infer<typeof bridgeTriggerResultSchema>;

export const bridgeEventSchema = z.object({
  seq: z.number().int().nonnegative(),
  id: z.string().min(1),
  type: z.string().min(1),
  at: isoTimestampSchema,
  data: z.record(z.string(), z.unknown()),
});

export const bridgeEventPageSchema = z.object({
  events: z.array(bridgeEventSchema),
  next: z.number().int().nonnegative(),
  hasMore: z.boolean(),
});

export type BridgeEvent = z.infer<typeof bridgeEventSchema>;
export type BridgeEventPage = z.infer<typeof bridgeEventPageSchema>;

/** Frozen model of a runtime ticket snapshot. */
export const bridgeTicketModelSchema = z.object({
  provider: z.string().min(1),
  model: z.string().min(1),
  reasoningEffort: z.string().nullable().optional(),
});

/** Repository and starting commit frozen on a runtime ticket. */
export const bridgeTicketBaseSchema = z.object({
  repository: z.string().optional(),
  branch: z.string(),
  commit: z.string(),
});

export const bridgeTicketReportSchema = z.object({
  state: z.string().min(1),
  summary: z.string(),
  at: isoTimestampSchema,
});

/** A recoverable human pause, as the runtime persists it. */
export const bridgeTicketPausedSchema = z.object({
  at: isoTimestampSchema,
  by: z.literal('human'),
  operationId: z.string().min(1).optional(),
  stoppedAt: isoTimestampSchema.optional(),
  evidence: z.record(z.string(), z.unknown()).optional(),
  error: z.string().nullable().optional(),
});

export const bridgeTicketExecutionSchema = z.object({
  name: z.string(),
  directory: z.string(),
  assignment: z.string(),
});

/**
 * Authoritative runtime ticket snapshot. M3 fields stay; M4 adds mode,
 * planRevision, paused, execution. Fields an M3 runtime does not send are
 * optional so the same client can read both.
 */
export const bridgeTicketSnapshotSchema = z.object({
  ticketId: z.string().min(1),
  title: z.string(),
  state: z.string().min(1),
  question: z.string().nullable().optional(),
  attentionId: z.string().nullable().optional(),
  model: bridgeTicketModelSchema.nullable().optional(),
  base: bridgeTicketBaseSchema.nullable().optional(),
  assignmentId: z.string().nullable().optional(),
  report: bridgeTicketReportSchema.nullable().optional(),
  pr: z.object({ number: z.number().int().positive(), url: z.string() }).optional(),
  ended: z
    .object({
      at: isoTimestampSchema,
      by: z.string(),
      outcome: z.string(),
      reason: z.string().optional(),
    })
    .nullable()
    .optional(),
  mode: z.enum(['execute', 'discuss']).optional(),
  planRevision: z.number().int().positive().nullable().optional(),
  paused: bridgeTicketPausedSchema.nullable().optional(),
  /** M5-B: a Source ticket is held until the platform admits it. */
  admissionRequired: z.boolean().optional(),
  admission: bridgeAdmissionSchema.nullable().optional(),
  execution: bridgeTicketExecutionSchema.nullable().optional(),
});

export type BridgeTicketSnapshot = z.infer<typeof bridgeTicketSnapshotSchema>;

export const bridgeOperationKindSchema = z.enum(['start', 'pause', 'resume', 'retry']);

export type BridgeOperationKind = z.infer<typeof bridgeOperationKindSchema>;

export const bridgeOperationStateSchema = z.enum(['queued', 'running', 'succeeded', 'failed']);

export type BridgeOperationState = z.infer<typeof bridgeOperationStateSchema>;

export const bridgeOperationSchema = z.object({
  operationId: z.string().min(1),
  ticketId: z.string().min(1),
  kind: bridgeOperationKindSchema,
  state: bridgeOperationStateSchema,
  error: z.string().nullable().optional(),
  evidence: z.record(z.string(), z.unknown()).optional(),
  updatedAt: isoTimestampSchema,
});

export type BridgeOperation = z.infer<typeof bridgeOperationSchema>;

export const bridgeOperationAcceptedSchema = z.object({
  operationId: z.string().min(1),
  ticketId: z.string().min(1),
  kind: bridgeOperationKindSchema,
  state: bridgeOperationStateSchema,
});

export type BridgeOperationAccepted = z.infer<typeof bridgeOperationAcceptedSchema>;

export const bridgePauseInputSchema = z
  .object({ operationId: z.string().min(1).max(200), reason: z.string().max(1000).optional() })
  .strict();

export type BridgePauseInput = z.infer<typeof bridgePauseInputSchema>;

export const bridgeResumeInputSchema = z
  .object({
    operationId: z.string().min(1).max(200),
    admission: z
      .object({
        leaseId: uuidSchema,
        /** Decimal string because the fencing token is a bigint. */
        fencingToken: z.string().regex(/^(0|[1-9][0-9]*)$/),
      })
      .strict(),
  })
  .strict();

export type BridgeResumeInput = z.infer<typeof bridgeResumeInputSchema>;

export const bridgeStartInputSchema = z
  .object({
    operationId: z.string().min(1).max(200),
    planRevision: z.number().int().positive(),
    admission: bridgeAdmissionSchema.optional(),
  })
  .strict();

export type BridgeStartInput = z.infer<typeof bridgeStartInputSchema>;

export const bridgeRetryInputSchema = z
  .object({ operationId: z.string().min(1).max(200), admission: bridgeAdmissionSchema.optional() })
  .strict();

export type BridgeRetryInput = z.infer<typeof bridgeRetryInputSchema>;

export const bridgeAdmitInputSchema = z
  .object({
    admission: bridgeAdmissionSchema,
    assignmentId: z.string().min(1).max(200).optional(),
  })
  .strict();

export type BridgeAdmitInput = z.infer<typeof bridgeAdmitInputSchema>;

export const bridgeAdmitResultSchema = z.object({
  ticketId: z.string().min(1),
  admitted: z.literal(true),
});

export type BridgeAdmitResult = z.infer<typeof bridgeAdmitResultSchema>;
