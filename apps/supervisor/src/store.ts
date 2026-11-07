import type pg from 'pg';

export interface RepositoryRow {
  id: string;
  ownerId: string;
  fullName: string;
  defaultBranch: string;
  bridgeUrl: string | null;
  bridgeCredentialRef: string | null;
  status: 'provisioning' | 'active' | 'sleeping' | 'error';
}

export interface InstanceRow {
  repositoryId: string;
  fullName: string;
  defaultBranch: string;
  state: 'sleeping' | 'starting' | 'online' | 'stopping' | 'fault';
  desiredState: 'sleeping' | 'online';
  reasonCode: string;
  reasonDetail: string | null;
  waitingFor: string | null;
  queuePriority: number;
  wakeRequestedAt: Date | null;
  lastActivityAt: Date | null;
  lastProgressAt: Date | null;
  lastStartedAt: Date | null;
  lastStoppedAt: Date | null;
  lastHealthAt: Date | null;
  pid: number | null;
  port: number | null;
  bridgePort: number | null;
  profile: string | null;
  ownership: Record<string, unknown>;
  attemptCount: number;
  errorCode: string | null;
  errorDetail: string | null;
  memoryCurrentBytes: number | null;
  memoryPeakBytes: number | null;
  memoryLimitBytes: number | null;
  memoryEvents: Record<string, number>;
  resourceState: string;
  resourceDetail: string | null;
  resourceCheckedAt: Date | null;
  updatedAt: Date;
}

export interface TaskGate {
  active: number;
  waiting: number;
  pendingOperations: number;
}

const INSTANCE_COLUMNS = `
  i.repository_id as "repositoryId", r.full_name as "fullName", r.default_branch as "defaultBranch",
  i.state, i.desired_state as "desiredState", i.reason_code as "reasonCode",
  i.reason_detail as "reasonDetail", i.waiting_for as "waitingFor", i.queue_priority as "queuePriority",
  i.wake_requested_at as "wakeRequestedAt", i.last_activity_at as "lastActivityAt",
  i.last_progress_at as "lastProgressAt", i.last_started_at as "lastStartedAt",
  i.last_stopped_at as "lastStoppedAt", i.last_health_at as "lastHealthAt",
  i.pid, i.port, i.bridge_port as "bridgePort", i.profile, i.ownership, i.attempt_count as "attemptCount", i.error_code as "errorCode",
  i.error_detail as "errorDetail",
  i.memory_current_bytes as "memoryCurrentBytes", i.memory_peak_bytes as "memoryPeakBytes",
  i.memory_limit_bytes as "memoryLimitBytes", i.memory_events as "memoryEvents",
  i.resource_state as "resourceState", i.resource_detail as "resourceDetail",
  i.resource_checked_at as "resourceCheckedAt", i.updated_at as "updatedAt"
`;

/** Durable supervisor state. The platform API reads the same rows. */
export class SupervisorStore {
  constructor(private readonly pool: pg.Pool) {}

  async getRepository(repositoryId: string): Promise<RepositoryRow | null> {
    const result = await this.pool.query<{
      id: string;
      owner_id: string;
      full_name: string;
      default_branch: string;
      bridge_url: string | null;
      bridge_credential_ref: string | null;
      status: RepositoryRow['status'];
    }>(
      `select id, owner_id, full_name, default_branch, bridge_url, bridge_credential_ref, status
         from repositories where id = $1 limit 1`,
      [repositoryId],
    );
    const row = result.rows[0];
    return row
      ? {
          id: row.id,
          ownerId: row.owner_id,
          fullName: row.full_name,
          defaultBranch: row.default_branch,
          bridgeUrl: row.bridge_url,
          bridgeCredentialRef: row.bridge_credential_ref,
          status: row.status,
        }
      : null;
  }

  async getRepositoryByFullName(fullName: string): Promise<RepositoryRow | null> {
    const result = await this.pool.query<{ id: string }>(
      `select id from repositories where full_name = $1 limit 1`,
      [fullName],
    );
    const id = result.rows[0]?.id;
    return id ? this.getRepository(id) : null;
  }

  async firstOwnerId(): Promise<string | null> {
    const result = await this.pool.query<{ id: string }>(
      `select id from owners order by created_at limit 1`,
    );
    return result.rows[0]?.id ?? null;
  }

  async getInstance(repositoryId: string): Promise<InstanceRow | null> {
    const result = await this.pool.query<InstanceRow>(
      `select ${INSTANCE_COLUMNS}
         from repository_instances i join repositories r on r.id = i.repository_id
        where i.repository_id = $1 limit 1`,
      [repositoryId],
    );
    return result.rows[0] ?? null;
  }

  async listInstances(): Promise<InstanceRow[]> {
    const result = await this.pool.query<InstanceRow>(
      `select ${INSTANCE_COLUMNS}
         from repository_instances i join repositories r on r.id = i.repository_id
        order by i.updated_at`,
    );
    return result.rows;
  }

  async createInstance(
    repositoryId: string,
    state: InstanceRow['state'],
    desiredState: InstanceRow['desiredState'],
    reasonCode: string,
    fields: { profile?: string | null; port?: number | null; bridgePort?: number | null } = {},
  ): Promise<void> {
    await this.pool.query(
      `insert into repository_instances
         (repository_id, state, desired_state, reason_code, profile, port, bridge_port)
       values ($1, $2, $3, $4, $5, $6, $7)
       on conflict (repository_id) do nothing`,
      [
        repositoryId,
        state,
        desiredState,
        reasonCode,
        fields.profile ?? null,
        fields.port ?? null,
        fields.bridgePort ?? null,
      ],
    );
  }

  async updateInstance(
    repositoryId: string,
    fields: Partial<{
      state: InstanceRow['state'];
      desiredState: InstanceRow['desiredState'];
      reasonCode: string;
      reasonDetail: string | null;
      waitingFor: string | null;
      queuePriority: number;
      wakeRequestedAt: Date | null;
      lastActivityAt: Date | null;
      lastProgressAt: Date | null;
      lastStartedAt: Date | null;
      lastStoppedAt: Date | null;
      lastHealthAt: Date | null;
      pid: number | null;
      port: number | null;
      bridgePort: number | null;
      profile: string | null;
      ownership: Record<string, unknown>;
      attemptCount: number;
      errorCode: string | null;
      errorDetail: string | null;
      memoryCurrentBytes: number | null;
      memoryPeakBytes: number | null;
      memoryLimitBytes: number | null;
      memoryEvents: Record<string, number>;
      resourceState: string;
      resourceDetail: string | null;
      resourceCheckedAt: Date | null;
    }>,
  ): Promise<void> {
    const column: Record<string, string> = {
      state: 'state',
      desiredState: 'desired_state',
      reasonCode: 'reason_code',
      reasonDetail: 'reason_detail',
      waitingFor: 'waiting_for',
      queuePriority: 'queue_priority',
      wakeRequestedAt: 'wake_requested_at',
      lastActivityAt: 'last_activity_at',
      lastProgressAt: 'last_progress_at',
      lastStartedAt: 'last_started_at',
      lastStoppedAt: 'last_stopped_at',
      lastHealthAt: 'last_health_at',
      pid: 'pid',
      port: 'port',
      bridgePort: 'bridge_port',
      profile: 'profile',
      ownership: 'ownership',
      attemptCount: 'attempt_count',
      errorCode: 'error_code',
      errorDetail: 'error_detail',
      memoryCurrentBytes: 'memory_current_bytes',
      memoryPeakBytes: 'memory_peak_bytes',
      memoryLimitBytes: 'memory_limit_bytes',
      memoryEvents: 'memory_events',
      resourceState: 'resource_state',
      resourceDetail: 'resource_detail',
      resourceCheckedAt: 'resource_checked_at',
    };
    const entries = Object.entries(fields).filter(([, value]) => value !== undefined);
    if (entries.length === 0) return;
    const assignments = entries.map(([key], index) => `${column[key]} = $${index + 2}`);
    await this.pool.query(
      `update repository_instances set ${assignments.join(', ')}, updated_at = now()
        where repository_id = $1`,
      [repositoryId, ...entries.map(([, value]) => value)],
    );
  }

  async recordResourceSample(input: {
    scope: 'host' | 'aggregate' | 'platform' | 'instance';
    repositoryId?: string | null;
    memoryCurrentBytes?: number | null;
    memoryPeakBytes?: number | null;
    memoryLimitBytes?: number | null;
    memoryEvents?: Record<string, number>;
    oomKillCount?: number;
    hostMemAvailableBytes?: number | null;
    pressure?: Record<string, unknown>;
    detail?: Record<string, unknown>;
  }): Promise<void> {
    await this.pool.query(
      `insert into runtime_resource_samples
         (scope, repository_id, memory_current_bytes, memory_peak_bytes, memory_limit_bytes,
          memory_events, oom_kill_count, host_mem_available_bytes, pressure, detail)
       values ($1,$2,$3,$4,$5,$6::jsonb,$7,$8,$9::jsonb,$10::jsonb)`,
      [
        input.scope,
        input.repositoryId ?? null,
        input.memoryCurrentBytes ?? null,
        input.memoryPeakBytes ?? null,
        input.memoryLimitBytes ?? null,
        JSON.stringify(input.memoryEvents ?? {}),
        input.oomKillCount ?? 0,
        input.hostMemAvailableBytes ?? null,
        JSON.stringify(input.pressure ?? {}),
        JSON.stringify(input.detail ?? {}),
      ],
    );
  }

  /** Keeps the legacy repository status column consistent with the supervisor projection. */
  async syncRepositoryStatus(repositoryId: string, state: InstanceRow['state']): Promise<void> {
    const status =
      state === 'online' || state === 'starting' || state === 'stopping'
        ? 'active'
        : state === 'fault'
          ? 'error'
          : 'sleeping';
    await this.pool.query(
      `update repositories set status = $2, updated_at = now()
        where id = $1 and status is distinct from $2`,
      [repositoryId, status],
    );
  }

  async recordProgress(repositoryId: string): Promise<Date | null> {
    const result = await this.pool.query<{ at: Date | null; activity: Date | null }>(
      `select (select max(updated_at) from task_links where repository_id = $1) as at,
              (select max(updated_at) from conversations where repository_id = $1) as activity`,
      [repositoryId],
    );
    const at = result.rows[0]?.at ?? null;
    const activity = result.rows[0]?.activity ?? null;
    if (at) {
      await this.pool.query(
        `update repository_instances
            set last_progress_at = greatest(coalesce(last_progress_at, $2), $2),
                last_activity_at = greatest(coalesce(last_activity_at, $3), $3),
                updated_at = now()
          where repository_id = $1`,
        [repositoryId, at, activity ?? at],
      );
    }
    return at;
  }

  async taskGate(repositoryId: string): Promise<TaskGate> {
    const result = await this.pool.query<{
      active: string;
      waiting: string;
      ops: string;
    }>(
      `select
         (select count(*) from task_links
           where repository_id = $1 and state::text in
             ('pending_admission','queued','discussing','running','stopping')) as active,
         (select count(*) from task_links
           where repository_id = $1 and state::text in
             ('waiting_input','awaiting_review','stopped','blocked','failed')) as waiting,
         (select count(*) from task_operations
           where repository_id = $1 and state in ('queued','running')) as ops`,
      [repositoryId],
    );
    const row = result.rows[0];
    return {
      active: Number(row?.active ?? 0),
      waiting: Number(row?.waiting ?? 0),
      pendingOperations: Number(row?.ops ?? 0),
    };
  }

  async pendingWakeInstances(): Promise<InstanceRow[]> {
    const result = await this.pool.query<InstanceRow>(
      `select ${INSTANCE_COLUMNS}
         from repository_instances i join repositories r on r.id = i.repository_id
        where i.desired_state = 'online'
          and i.state in ('sleeping','fault')
        order by i.queue_priority, i.wake_requested_at nulls last, i.last_activity_at nulls last`,
    );
    return result.rows;
  }

  async onlineInstances(): Promise<InstanceRow[]> {
    const result = await this.pool.query<InstanceRow>(
      `select ${INSTANCE_COLUMNS}
         from repository_instances i join repositories r on r.id = i.repository_id
        where i.state in ('starting','online','stopping')
        order by i.last_activity_at nulls last, i.updated_at`,
    );
    return result.rows;
  }

  async setBridgeBinding(
    repositoryId: string,
    input: { bridgeUrl: string; credentialRef: string; callbackTokenHash: string },
  ): Promise<void> {
    await this.pool.query(
      `update repositories
          set bridge_url = $2, bridge_credential_ref = $3,
              bridge_callback_token_hash = $4, updated_at = now()
        where id = $1`,
      [repositoryId, input.bridgeUrl, input.credentialRef, input.callbackTokenHash],
    );
  }

  async setLastActivity(repositoryId: string, at: Date): Promise<void> {
    await this.pool.query(
      `update repository_instances
          set last_activity_at = greatest(coalesce(last_activity_at, $2), $2), updated_at = now()
        where repository_id = $1`,
      [repositoryId, at],
    );
  }
}
