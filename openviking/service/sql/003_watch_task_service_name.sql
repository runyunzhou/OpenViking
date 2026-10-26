-- Upgrade an existing ov_watch_tasks table to service_name-partitioned rows.
-- Compatible with MySQL 5.7. Run once after 001_schedule_provider.sql and
-- 002_watch_task_projection.sql if the table was created before service_name
-- was added to ov_watch_tasks.
--
-- If top-level service_name is not "openviking-server", set it before running:
--   SET @openviking_service_name = 'production';

SET @openviking_service_name = COALESCE(
    NULLIF(@openviking_service_name, ''),
    'openviking-server'
);

ALTER TABLE `ov_watch_tasks`
    ADD COLUMN service_name VARCHAR(255) COLLATE utf8mb4_bin NULL FIRST;

UPDATE `ov_watch_tasks`
SET service_name = @openviking_service_name
WHERE service_name IS NULL OR service_name = '';

ALTER TABLE `ov_watch_tasks`
    MODIFY COLUMN service_name VARCHAR(255) COLLATE utf8mb4_bin NOT NULL,
    DROP PRIMARY KEY,
    DROP KEY ix_ov_watch_tasks_due,
    DROP KEY ix_ov_watch_tasks_account_target,
    DROP KEY ix_ov_watch_tasks_account_target_prefix,
    DROP KEY ix_ov_watch_tasks_run,
    DROP KEY ix_ov_watch_tasks_list,
    DROP KEY ix_ov_watch_tasks_list_active,
    DROP KEY ix_ov_watch_tasks_sweep_idle,
    DROP KEY ix_ov_watch_tasks_sweep_inflight,
    ADD PRIMARY KEY (service_name, task_id),
    ADD KEY ix_ov_watch_tasks_due
        (service_name, enabled, next_run_at, task_id),
    ADD KEY ix_ov_watch_tasks_account_target
        (service_name(128), account_id(128), target_uri_hash, task_id(128)),
    ADD KEY ix_ov_watch_tasks_account_target_prefix
        (service_name(128), account_id(128), target_uri_prefix(128), task_id(128)),
    ADD KEY ix_ov_watch_tasks_run
        (service_name, current_run_id, task_id),
    ADD KEY ix_ov_watch_tasks_list
        (service_name(128), account_id(128), user_id(128), created_at, task_id(128)),
    ADD KEY ix_ov_watch_tasks_list_active
        (service_name(128), account_id(128), user_id(128), enabled, created_at, task_id(128)),
    ADD KEY ix_ov_watch_tasks_sweep_idle
        (service_name, enabled, current_run_id, next_run_at, task_id),
    ADD KEY ix_ov_watch_tasks_sweep_inflight
        (service_name, enabled, current_run_id, updated_at, task_id);
