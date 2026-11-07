import type pg from 'pg';

export interface ExecutionReservation {
  ticketId: string;
  assignmentId: string;
  leaseId: string;
  fencingToken: string;
}
export interface ReserveInput extends ExecutionReservation {
  repositoryId: string;
  requestKey: string;
  taskId?: string | null;
  deliveryId?: string | null;
}

/** Persist one execution identity per idempotency key and current lease. */
export async function reserveExecution(
  pool: pg.Pool,
  input: ReserveInput,
): Promise<{
  id: string;
  state: string;
  ticketId: string;
  assignmentId: string;
}> {
  const client = await pool.connect();
  try {
    await client.query('begin');
    const lease = await client.query<{
      id: string;
      repository_id: string;
      fencing_token: string;
      state: string;
    }>(
      `select id, repository_id, fencing_token, state from runtime_leases where id = $1 for update`,
      [input.leaseId],
    );
    const current = lease.rows[0];
    if (
      !current ||
      current.repository_id !== input.repositoryId ||
      current.state !== 'active' ||
      String(current.fencing_token) !== input.fencingToken
    ) {
      throw new Error(
        'execution reservation requires the current active lease generation for this repository',
      );
    }
    const inserted = await client.query<{
      id: string;
      state: string;
      ticket_id: string;
      assignment_id: string;
    }>(
      `insert into execution_reservations
         (repository_id, task_id, delivery_id, ticket_id, assignment_id, lease_id, fencing_token, request_key)
       values ($1, $2, $3, $4, $5, $6, $7, $8)
       on conflict (repository_id, request_key) do nothing
       returning id, state, ticket_id, assignment_id`,
      [
        input.repositoryId,
        input.taskId ?? null,
        input.deliveryId ?? null,
        input.ticketId,
        input.assignmentId,
        input.leaseId,
        input.fencingToken,
        input.requestKey,
      ],
    );
    const row =
      inserted.rows[0] ??
      (
        await client.query<{ id: string; state: string; ticket_id: string; assignment_id: string }>(
          `select id, state, ticket_id, assignment_id from execution_reservations
        where repository_id = $1 and request_key = $2 for update`,
          [input.repositoryId, input.requestKey],
        )
      ).rows[0];
    if (!row || row.ticket_id !== input.ticketId || row.assignment_id !== input.assignmentId) {
      throw new Error('execution reservation conflicts with a different identity');
    }
    const stored = await client.query<{ lease_id: string; fencing_token: string }>(
      `select lease_id, fencing_token from execution_reservations where id = $1`,
      [row.id],
    );
    if (
      String(stored.rows[0]?.lease_id) !== input.leaseId ||
      String(stored.rows[0]?.fencing_token) !== input.fencingToken
    ) {
      throw new Error('execution reservation is bound to a different lease generation');
    }
    await client.query('commit');
    return {
      id: row.id,
      state: row.state,
      ticketId: row.ticket_id,
      assignmentId: row.assignment_id,
    };
  } catch (error) {
    await client.query('rollback').catch(() => undefined);
    throw error;
  } finally {
    client.release();
  }
}

/** Read the authoritative identity for an idempotency key, if one exists. */
export async function readReservation(
  pool: pg.Pool,
  repositoryId: string,
  requestKey: string,
): Promise<{
  ticketId: string;
  assignmentId: string;
  leaseId: string;
  fencingToken: string;
  state: string;
} | null> {
  const result = await pool.query<{
    ticket_id: string;
    assignment_id: string;
    lease_id: string;
    fencing_token: string;
    state: string;
  }>(
    `select ticket_id, assignment_id, lease_id, fencing_token, state
       from execution_reservations where repository_id = $1 and request_key = $2`,
    [repositoryId, requestKey],
  );
  const row = result.rows[0];
  if (!row) return null;
  return {
    ticketId: row.ticket_id,
    assignmentId: row.assignment_id,
    leaseId: row.lease_id,
    fencingToken: String(row.fencing_token),
    state: row.state,
  };
}

/** Claim a reservation exactly once; replays return the already claimed row. */
export async function claimExecution(
  pool: pg.Pool,
  input: ReserveInput,
): Promise<{ claimed: boolean; id: string }> {
  const client = await pool.connect();
  try {
    await client.query('begin');
    const lease = await client.query<{
      repository_id: string;
      fencing_token: string;
      state: string;
    }>(`select repository_id, fencing_token, state from runtime_leases where id = $1 for update`, [
      input.leaseId,
    ]);
    const current = lease.rows[0];
    if (
      !current ||
      current.repository_id !== input.repositoryId ||
      current.state !== 'active' ||
      String(current.fencing_token) !== input.fencingToken
    ) {
      throw new Error(
        'execution claim requires the current active lease generation for this repository',
      );
    }
    const updated = await client.query<{ id: string }>(
      `update execution_reservations set state = 'claimed', updated_at = now()
        where repository_id = $1 and request_key = $2 and ticket_id = $3 and assignment_id = $4
          and lease_id = $5 and fencing_token = $6 and state = 'reserved'
        returning id`,
      [
        input.repositoryId,
        input.requestKey,
        input.ticketId,
        input.assignmentId,
        input.leaseId,
        input.fencingToken,
      ],
    );
    if (updated.rows[0]) {
      await client.query('commit');
      return { claimed: true, id: updated.rows[0].id };
    }
    const existing = await client.query<{
      id: string;
      state: string;
      ticket_id: string;
      assignment_id: string;
      lease_id: string;
      fencing_token: string;
    }>(
      `select id, state, ticket_id, assignment_id, lease_id, fencing_token
         from execution_reservations where repository_id = $1 and request_key = $2`,
      [input.repositoryId, input.requestKey],
    );
    await client.query('commit');
    const row = existing.rows[0];
    if (!row) throw new Error('execution reservation is missing');
    if (
      row.state !== 'claimed' ||
      row.ticket_id !== input.ticketId ||
      row.assignment_id !== input.assignmentId ||
      row.lease_id !== input.leaseId ||
      String(row.fencing_token) !== input.fencingToken
    ) {
      throw new Error('execution reservation identity or generation does not match');
    }
    return { claimed: false, id: row.id };
  } catch (error) {
    await client.query('rollback').catch(() => undefined);
    throw error;
  } finally {
    client.release();
  }
}

/**
 * Claim a still-reserved identity from an event that names the Assignment.
 * Lock order matches reserve/claim (lease then reservation); both the event
 * identity and the active generation must match, or nothing is claimed.
 */
export async function reconcileExecutionClaimOn(
  client: pg.PoolClient,
  repositoryId: string,
  requestKey: string,
  ticketId: string,
  assignmentId: string | null | undefined,
  leaseId: string | null | undefined,
  fencingToken: string | null | undefined,
): Promise<boolean> {
  if (!assignmentId || !leaseId || !fencingToken) return false;
  const initial = await client.query<{
    id: string;
    assignment_id: string;
    lease_id: string;
    fencing_token: string;
    state: string;
  }>(
    `select id, assignment_id, lease_id, fencing_token, state
       from execution_reservations
      where repository_id = $1 and request_key = $2 and ticket_id = $3`,
    [repositoryId, requestKey, ticketId],
  );
  const candidate = initial.rows[0];
  if (
    !candidate ||
    candidate.state !== 'reserved' ||
    candidate.assignment_id !== assignmentId ||
    candidate.lease_id !== leaseId ||
    String(candidate.fencing_token) !== fencingToken
  ) {
    return false;
  }
  const lease = await client.query<{ repository_id: string; fencing_token: string; state: string }>(
    `select repository_id, fencing_token, state from runtime_leases where id = $1 for update`,
    [candidate.lease_id],
  );
  const current = lease.rows[0];
  if (
    !current ||
    current.repository_id !== repositoryId ||
    current.state !== 'active' ||
    String(current.fencing_token) !== candidate.fencing_token
  ) {
    return false;
  }
  const locked = await client.query<{
    id: string;
    assignment_id: string;
    fencing_token: string;
    state: string;
  }>(
    `select id, assignment_id, fencing_token, state from execution_reservations
      where id = $1 for update`,
    [candidate.id],
  );
  const reservation = locked.rows[0];
  if (
    !reservation ||
    reservation.state !== 'reserved' ||
    reservation.assignment_id !== assignmentId ||
    String(reservation.fencing_token) !== fencingToken
  ) {
    return false;
  }
  const updated = await client.query(
    `update execution_reservations set state = 'claimed', updated_at = now()
      where id = $1 and state = 'reserved'`,
    [reservation.id],
  );
  return (updated.rowCount ?? 0) === 1;
}
