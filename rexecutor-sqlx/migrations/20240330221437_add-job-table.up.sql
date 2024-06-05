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
  tags VARCHAR[] NOT null DEFAULT ARRAY[]::character varying[],
  metadata JSONB,
  priority INTEGER NOT null DEFAULT 0,
  uniqueness_key BIGINT,
  inserted_at TIMESTAMP WITH TIME ZONE NOT null DEFAULT timezone('UTC'::text, now()),
  scheduled_at TIMESTAMP WITH TIME ZONE NOT null DEFAULT timezone('UTC'::text, now()),
  attempted_at TIMESTAMP WITH TIME ZONE,
  completed_at TIMESTAMP WITH TIME ZONE,
  cancelled_at TIMESTAMP WITH TIME ZONE,
  discarded_at TIMESTAMP WITH TIME ZONE
);

CREATE INDEX IF NOT EXISTS rexecutor_job_data_index ON public.rexecutor_jobs USING gin (data);
CREATE INDEX IF NOT EXISTS rexecutor_job_meta_index ON public.rexecutor_jobs USING gin (metadata);
CREATE INDEX IF NOT EXISTS rexecutor_job_uniqueness_index ON public.rexecutor_jobs (status, uniqueness_key, executor, scheduled_at);
CREATE INDEX IF NOT EXISTS rexecutor_job_inserted_at_index ON public.rexecutor_jobs (scheduled_at);

CREATE OR REPLACE
FUNCTION public.rexecutor_new_job_notify()
  RETURNS trigger
  LANGUAGE plpgsql
AS $function$
DECLARE
  channel text;
  notice json;
BEGIN
  IF NEW.status = 'scheduled' OR NEW.status = 'retryable' THEN
    channel = 'public.rexecutor_scheduled';
    notice = json_build_object('executor', NEW.executor, 'scheduled_at', NEW.scheduled_at);

    PERFORM pg_notify(channel, notice::text);
  END IF;

  RETURN NULL;
END;
$function$
;

CREATE TRIGGER rexecutor_insert
AFTER INSERT ON public.rexecutor_jobs
FOR each ROW
EXECUTE FUNCTION public.rexecutor_new_job_notify();

CREATE TRIGGER rexecutor_update
AFTER UPDATE ON public.rexecutor_jobs
FOR each ROW
EXECUTE FUNCTION public.rexecutor_new_job_notify();

COMMIT;
