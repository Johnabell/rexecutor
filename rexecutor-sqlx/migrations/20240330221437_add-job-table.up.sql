BEGIN;

DO $$ BEGIN
  CREATE TYPE rexecutor_job_state AS ENUM (
      'scheduled',
      'executing',
      'retryable',
      'complete',
      'cancelled',
      'discarded'
  );
EXCEPTION
  WHEN duplicate_object THEN null;
END $$;

CREATE TABLE IF NOT EXISTS rexecutor_jobs (
  id SERIAL PRIMARY KEY,
  status  rexecutor_job_state NOT null DEFAULT 'scheduled'::rexecutor_job_state,
  executor VARCHAR NOT null,
  data JSONB NOT null DEFAULT '{}'::jsonb,
  errors JSONB[] NOT null DEFAULT ARRAY[]::jsonb[],
  attempt INTEGER NOT null DEFAULT 0,
  max_attempts INTEGER NOT null DEFAULT 5,
  attempted_by VARCHAR[],
  tags VARCHAR[] DEFAULT ARRAY[]::character varying[],
  metadata JSONB DEFAULT '{}'::jsonb,
  queue VARCHAR DEFAULT 'default',
  priority INTEGER NOT null DEFAULT 0,
  inserted_at TIMESTAMP WITH TIME ZONE NOT null DEFAULT timezone('UTC'::text, now()),
  scheduled_at TIMESTAMP WITH TIME ZONE NOT null DEFAULT timezone('UTC'::text, now()),
  attempted_at TIMESTAMP WITH TIME ZONE,
  completed_at TIMESTAMP WITH TIME ZONE,
  cancelled_at TIMESTAMP WITH TIME ZONE,
  discarded_at TIMESTAMP WITH TIME ZONE
);

CREATE INDEX IF NOT EXISTS rexecutor_job_data_index ON public.rexecutor_jobs USING gin (data);
CREATE INDEX IF NOT EXISTS rexecutor_job_meta_index ON public.rexecutor_jobs USING gin (metadata);

COMMIT;
