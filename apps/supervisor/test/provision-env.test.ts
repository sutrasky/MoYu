import { describe, expect, it } from 'vitest';
import { instanceEnvironment, parseEnvFile } from '../src/provision';
import { mkdtempSync, writeFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

describe('managed instance environment', () => {
  it('writes the admission enforcement flag by default', () => {
    const dir = mkdtempSync(join(tmpdir(), 'm5b-env-'));
    try {
      const path = join(dir, 'instance.env');
      writeFileSync(
        path,
        instanceEnvironment({
          platformUrl: 'http://127.0.0.1:1',
          platformToken: 'callback',
          bridgeToken: 'bridge',
          bridgePort: 3901,
        }),
      );
      expect(parseEnvFile(path)).toMatchObject({
        MOYU_REQUIRE_ADMISSION: '1',
        MOYU_BRIDGE_PORT: '3901',
      });
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
});
