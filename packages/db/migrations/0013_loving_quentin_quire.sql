CREATE TABLE "runtime_lease_events" (
	"id" uuid PRIMARY KEY DEFAULT gen_random_uuid() NOT NULL,
	"lease_id" uuid,
	"repository_id" uuid NOT NULL,
	"fencing_token" bigint,
	"type" text NOT NULL,
	"detail" jsonb DEFAULT '{}'::jsonb NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE TABLE "runtime_resource_samples" (
	"id" uuid PRIMARY KEY DEFAULT gen_random_uuid() NOT NULL,
	"scope" text NOT NULL,
	"repository_id" uuid,
	"memory_current_bytes" bigint,
	"memory_peak_bytes" bigint,
	"memory_limit_bytes" bigint,
	"memory_events" jsonb DEFAULT '{}'::jsonb NOT NULL,
	"oom_kill_count" integer DEFAULT 0 NOT NULL,
	"host_mem_available_bytes" bigint,
	"pressure" jsonb DEFAULT '{}'::jsonb NOT NULL,
	"detail" jsonb DEFAULT '{}'::jsonb NOT NULL,
	"sampled_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
ALTER TABLE "deliveries" ADD COLUMN "runtime_lease_id" uuid;--> statement-breakpoint
ALTER TABLE "deliveries" ADD COLUMN "fencing_token" bigint;--> statement-breakpoint
ALTER TABLE "repository_instances" ADD COLUMN "memory_current_bytes" bigint;--> statement-breakpoint
ALTER TABLE "repository_instances" ADD COLUMN "memory_peak_bytes" bigint;--> statement-breakpoint
ALTER TABLE "repository_instances" ADD COLUMN "memory_limit_bytes" bigint;--> statement-breakpoint
ALTER TABLE "repository_instances" ADD COLUMN "memory_events" jsonb DEFAULT '{}'::jsonb NOT NULL;--> statement-breakpoint
ALTER TABLE "repository_instances" ADD COLUMN "resource_state" text DEFAULT 'unknown' NOT NULL;--> statement-breakpoint
ALTER TABLE "repository_instances" ADD COLUMN "resource_detail" text;--> statement-breakpoint
ALTER TABLE "repository_instances" ADD COLUMN "resource_checked_at" timestamp with time zone;--> statement-breakpoint
ALTER TABLE "runtime_leases" ADD COLUMN "recovery_state" text DEFAULT 'none' NOT NULL;--> statement-breakpoint
ALTER TABLE "runtime_leases" ADD COLUMN "recovery_reason" text;--> statement-breakpoint
ALTER TABLE "runtime_leases" ADD COLUMN "waiting_for" text;--> statement-breakpoint
ALTER TABLE "runtime_leases" ADD COLUMN "stop_proof" jsonb DEFAULT '{}'::jsonb NOT NULL;--> statement-breakpoint
ALTER TABLE "runtime_leases" ADD COLUMN "stop_proven_at" timestamp with time zone;--> statement-breakpoint
ALTER TABLE "runtime_leases" ADD COLUMN "stale_receipt_count" bigint DEFAULT 0 NOT NULL;--> statement-breakpoint
ALTER TABLE "runtime_leases" ADD COLUMN "last_error" text;--> statement-breakpoint
ALTER TABLE "task_operations" ADD COLUMN "runtime_lease_id" uuid;--> statement-breakpoint
ALTER TABLE "task_operations" ADD COLUMN "fencing_token" bigint;--> statement-breakpoint
ALTER TABLE "task_operations" ADD COLUMN "stale_at" timestamp with time zone;--> statement-breakpoint
ALTER TABLE "task_operations" ADD COLUMN "stale_reason" text;--> statement-breakpoint
ALTER TABLE "runtime_lease_events" ADD CONSTRAINT "runtime_lease_events_lease_id_runtime_leases_id_fk" FOREIGN KEY ("lease_id") REFERENCES "public"."runtime_leases"("id") ON DELETE set null ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "runtime_lease_events" ADD CONSTRAINT "runtime_lease_events_repository_id_repositories_id_fk" FOREIGN KEY ("repository_id") REFERENCES "public"."repositories"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "runtime_resource_samples" ADD CONSTRAINT "runtime_resource_samples_repository_id_repositories_id_fk" FOREIGN KEY ("repository_id") REFERENCES "public"."repositories"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
CREATE INDEX "runtime_lease_events_repository_idx" ON "runtime_lease_events" USING btree ("repository_id","created_at");--> statement-breakpoint
CREATE INDEX "runtime_lease_events_lease_idx" ON "runtime_lease_events" USING btree ("lease_id","created_at");--> statement-breakpoint
CREATE INDEX "runtime_resource_samples_scope_idx" ON "runtime_resource_samples" USING btree ("scope","sampled_at");--> statement-breakpoint
CREATE INDEX "runtime_resource_samples_repository_idx" ON "runtime_resource_samples" USING btree ("repository_id","sampled_at");--> statement-breakpoint
ALTER TABLE "deliveries" ADD CONSTRAINT "deliveries_runtime_lease_id_runtime_leases_id_fk" FOREIGN KEY ("runtime_lease_id") REFERENCES "public"."runtime_leases"("id") ON DELETE set null ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "task_operations" ADD CONSTRAINT "task_operations_runtime_lease_id_runtime_leases_id_fk" FOREIGN KEY ("runtime_lease_id") REFERENCES "public"."runtime_leases"("id") ON DELETE set null ON UPDATE no action;--> statement-breakpoint
CREATE INDEX "runtime_leases_recovery_idx" ON "runtime_leases" USING btree ("state","recovery_state");