BEGIN;

DROP TRIGGER IF EXISTS rexecutor_insert;
DROP TRIGGER IF EXISTS rexecutor_update;
DROP FUNCTION IF EXISTS public.rexecutor_new_job_notify();
DROP INDEX IF EXISTS rexecutor_job_data_index;
DROP INDEX IF EXISTS rexecutor_job_meta_index;
DROP INDEX IF EXISTS rexecutor_job_uniqueness_index;

DROP TABLE IF EXISTS rexecutor_jobs;

DROP TYPE IF EXISTS rexecutor_job_state;

COMMIT;
