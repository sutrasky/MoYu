-- Reverse 0013: lease generations/recovery proof, command and delivery
-- admission columns, and resource samples. Only runs when 0013 is the latest
-- applied migration and the caller supplies the exact sha256 of
-- 0013_loving_quentin_quire.sql.
\set ON_ERROR_STOP on
\if :{?hash}
\else
  \set hash __MOYU_0013_HASH_MISSING__
\endif
BEGIN;
SET LOCAL moyu.rollback_hash = :'hash';
DO $$
DECLARE
  expected constant text := 'b06e32aebd8dfa96845f74dc916613de7f4d775935c7ce27f5875fcef13f8196';
  supplied text := current_setting('moyu.rollback_hash', true);
  matching integer;
  latest text;
BEGIN
  IF supplied IS NULL OR supplied = '' OR supplied = '__MOYU_0013_HASH_MISSING__' THEN
    RAISE EXCEPTION '0013 rollback refused: pass -v hash=<sha256 of 0013_loving_quentin_quire.sql>';
  END IF;
  IF supplied <> expected THEN RAISE EXCEPTION '0013 rollback refused: hash mismatch'; END IF;
  SELECT count(*) INTO matching FROM drizzle.__drizzle_migrations WHERE hash = expected;
  IF matching <> 1 THEN RAISE EXCEPTION '0013 rollback refused: expected one applied 0013 migration, found %', matching; END IF;
  SELECT hash INTO latest FROM drizzle.__drizzle_migrations ORDER BY id DESC LIMIT 1;
  IF latest <> expected THEN RAISE EXCEPTION '0013 rollback refused: 0013 is not the latest applied migration'; END IF;
END $$;
DROP TABLE IF EXISTS "runtime_resource_samples";
DROP TABLE IF EXISTS "runtime_lease_events";
ALTER TABLE "deliveries" DROP CONSTRAINT IF EXISTS "deliveries_runtime_lease_id_runtime_leases_id_fk";
ALTER TABLE "task_operations" DROP CONSTRAINT IF EXISTS "task_operations_runtime_lease_id_runtime_leases_id_fk";
ALTER TABLE "runtime_leases" DROP COLUMN IF EXISTS "recovery_state";
ALTER TABLE "runtime_leases" DROP COLUMN IF EXISTS "recovery_reason";
ALTER TABLE "runtime_leases" DROP COLUMN IF EXISTS "waiting_for";
ALTER TABLE "runtime_leases" DROP COLUMN IF EXISTS "stop_proof";
ALTER TABLE "runtime_leases" DROP COLUMN IF EXISTS "stop_proven_at";
ALTER TABLE "runtime_leases" DROP COLUMN IF EXISTS "stale_receipt_count";
ALTER TABLE "runtime_leases" DROP COLUMN IF EXISTS "last_error";
ALTER TABLE "task_operations" DROP COLUMN IF EXISTS "runtime_lease_id";
ALTER TABLE "task_operations" DROP COLUMN IF EXISTS "fencing_token";
ALTER TABLE "task_operations" DROP COLUMN IF EXISTS "stale_at";
ALTER TABLE "task_operations" DROP COLUMN IF EXISTS "stale_reason";
ALTER TABLE "deliveries" DROP COLUMN IF EXISTS "runtime_lease_id";
ALTER TABLE "deliveries" DROP COLUMN IF EXISTS "fencing_token";
ALTER TABLE "repository_instances" DROP COLUMN IF EXISTS "memory_current_bytes";
ALTER TABLE "repository_instances" DROP COLUMN IF EXISTS "memory_peak_bytes";
ALTER TABLE "repository_instances" DROP COLUMN IF EXISTS "memory_limit_bytes";
ALTER TABLE "repository_instances" DROP COLUMN IF EXISTS "memory_events";
ALTER TABLE "repository_instances" DROP COLUMN IF EXISTS "resource_state";
ALTER TABLE "repository_instances" DROP COLUMN IF EXISTS "resource_detail";
ALTER TABLE "repository_instances" DROP COLUMN IF EXISTS "resource_checked_at";
DELETE FROM drizzle.__drizzle_migrations WHERE hash = 'b06e32aebd8dfa96845f74dc916613de7f4d775935c7ce27f5875fcef13f8196';
COMMIT;
