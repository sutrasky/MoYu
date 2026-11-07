import { describe, expect, it, vi } from 'vitest';
import { buildSupervisorServer } from '../src/server';

describe('supervisor wake execution binding', () => {
  it('passes the full execution generation and returns 409 when refused', async () => {
    const wake = vi.fn(async () => ({
      accepted: false,
      duplicate: false,
      refused: 'bound to another generation',
      instance: {},
    }));
    const app = buildSupervisorServer({
      config: { token: 'secret' } as never,
      manager: { wake } as never,
      logger: { info: vi.fn(), warn: vi.fn(), error: vi.fn() } as never,
    });
    const execution = { ticketId: 't1', assignmentId: 'a1', leaseId: 'l1', fencingToken: '7' };
    const response = await app.inject({
      method: 'POST',
      url: '/control/v1/instances/00000000-0000-4000-8000-000000000001/wake',
      headers: { authorization: 'Bearer secret' },
      payload: { execution },
    });
    expect(response.statusCode).toBe(409);
    expect(wake).toHaveBeenCalledWith(
      '00000000-0000-4000-8000-000000000001',
      expect.objectContaining({ execution }),
    );
    await app.close();
  });
});
