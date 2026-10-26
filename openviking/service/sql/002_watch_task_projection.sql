-- Upgrade an existing ov_watch_tasks table created by 001_schedule_provider.sql
-- before the request-payload/task-column split.
-- Compatible with MySQL 5.7. Run once; do not run after provisioning the
-- already-updated CREATE TABLE definition in 001.

ALTER TABLE `ov_watch_tasks`
    ADD COLUMN original_role VARCHAR(64) COLLATE ascii_bin NULL,
    ADD COLUMN watch_interval DOUBLE NULL,
    ADD COLUMN last_execution_time DOUBLE NULL,
    ADD COLUMN last_task_id VARCHAR(255) COLLATE utf8mb4_bin NULL,
    ADD COLUMN last_status VARCHAR(64) COLLATE ascii_bin NULL,
    ADD COLUMN last_error TEXT COLLATE utf8mb4_bin NULL,
    ADD COLUMN auth_state JSON NULL,
    ADD COLUMN connector_states JSON NULL;

UPDATE `ov_watch_tasks`
SET original_role = COALESCE(
        NULLIF(JSON_UNQUOTE(JSON_EXTRACT(payload, '$.original_role')), ''),
        'user'
    ),
    watch_interval = COALESCE(
        CAST(JSON_UNQUOTE(JSON_EXTRACT(payload, '$.watch_interval')) AS DECIMAL(20, 6)),
        60
    ),
    auth_state = JSON_EXTRACT(payload, '$.auth_state'),
    connector_states = JSON_EXTRACT(payload, '$.connector_states')
WHERE original_role IS NULL
   OR watch_interval IS NULL;

ALTER TABLE `ov_watch_tasks`
    MODIFY COLUMN original_role VARCHAR(64) COLLATE ascii_bin NOT NULL DEFAULT 'user',
    MODIFY COLUMN watch_interval DOUBLE NOT NULL DEFAULT 60,
    ADD KEY ix_ov_watch_tasks_list
        (account_id, user_id, created_at, task_id),
    ADD KEY ix_ov_watch_tasks_list_active
        (account_id, user_id, enabled, created_at, task_id),
    ADD KEY ix_ov_watch_tasks_sweep_idle
        (enabled, current_run_id, next_run_at, task_id),
    ADD KEY ix_ov_watch_tasks_sweep_inflight
        (enabled, current_run_id, updated_at, task_id);
