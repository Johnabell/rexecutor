BEGIN;

DROP INDEX IF EXISTS rexecutor_job_data_index;
DROP INDEX IF EXISTS rexecutor_job_meta_index;

DROP TABLE IF EXISTS rexecutor_jobs;

DROP TYPE IF EXISTS rexecutor_job_state;

COMMIT;