ALTER TABLE "execution_reservations" DROP CONSTRAINT IF EXISTS "execution_reservations_delivery_fk";
ALTER TABLE "execution_reservations" DROP CONSTRAINT IF EXISTS "execution_reservations_task_fk";
ALTER TABLE "execution_reservations" DROP CONSTRAINT IF EXISTS "execution_reservations_repository_fk";
DROP TABLE IF EXISTS "execution_reservations";
