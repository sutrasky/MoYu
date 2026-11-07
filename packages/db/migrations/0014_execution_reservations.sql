CREATE TABLE "execution_reservations" (
	"id" uuid PRIMARY KEY DEFAULT gen_random_uuid() NOT NULL,
	"repository_id" uuid NOT NULL,
	"task_id" uuid,
	"delivery_id" uuid,
	"ticket_id" text NOT NULL,
	"assignment_id" text NOT NULL,
	"lease_id" uuid NOT NULL,
	"fencing_token" bigint NOT NULL,
	"request_key" text NOT NULL,
	"state" text DEFAULT 'reserved' NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "execution_reservations_request_uq" UNIQUE("repository_id","request_key"),
	CONSTRAINT "execution_reservations_state_ck" CHECK ("state" IN ('reserved','claimed','abandoned'))
);
--> statement-breakpoint
ALTER TABLE "execution_reservations" ADD CONSTRAINT "execution_reservations_repository_fk" FOREIGN KEY ("repository_id") REFERENCES "repositories"("id") ON DELETE cascade ON UPDATE no action;
--> statement-breakpoint
ALTER TABLE "execution_reservations" ADD CONSTRAINT "execution_reservations_task_fk" FOREIGN KEY ("task_id") REFERENCES "task_links"("id") ON DELETE set null ON UPDATE no action;
--> statement-breakpoint
ALTER TABLE "execution_reservations" ADD CONSTRAINT "execution_reservations_delivery_fk" FOREIGN KEY ("delivery_id") REFERENCES "deliveries"("id") ON DELETE set null ON UPDATE no action;
