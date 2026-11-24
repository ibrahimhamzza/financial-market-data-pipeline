-- Migration 001: pipeline_runs gets explicit start_time / end_time.
-- Only needed for databases created with the OLD schema.sql (column run_date).
-- Safe to run more than once. Fresh installs already get the new layout from schema.sql.
--   psql -U postgres -d financial_db -f database/migrations/001_pipeline_runs_start_end_time.sql

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.columns
               WHERE table_name = 'pipeline_runs' AND column_name = 'run_date') THEN
        ALTER TABLE pipeline_runs RENAME COLUMN run_date TO start_time;
    END IF;
END $$;

ALTER TABLE pipeline_runs ADD COLUMN IF NOT EXISTS end_time TIMESTAMP;

-- Backfill finished runs from the stored duration.
UPDATE pipeline_runs
SET end_time = start_time + (duration_seconds * INTERVAL '1 second')
WHERE end_time IS NULL
  AND status <> 'running'
  AND duration_seconds IS NOT NULL;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_run_times') THEN
        ALTER TABLE pipeline_runs
            ADD CONSTRAINT chk_run_times CHECK (end_time IS NULL OR end_time >= start_time);
    END IF;
END $$;

DROP INDEX IF EXISTS idx_pipeline_runs_date;
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_date ON pipeline_runs (start_time DESC);
