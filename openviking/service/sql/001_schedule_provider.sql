-- OpenViking MySQL schedule provider schema.
-- Apply once after task_store_init.sql, before starting the upgraded server.
-- Required for every MySQL Task store, even when Watch is disabled.
-- Fresh installations: task_store_init.sql, then this migration.
-- Existing installations: this migration only; do not rerun if already applied.
-- For custom table names, replace ov_tasks and ov_watch_tasks independently
-- in table and index names.
-- Compatible with MySQL 5.7.

ALTER TABLE `ov_tasks`
    ADD COLUMN schedule_kind VARCHAR(32) COLLATE ascii_bin NULL,
    ADD COLUMN schedule_id VARCHAR(255) COLLATE utf8mb4_bin NULL,
    ADD COLUMN run_id VARCHAR(255) COLLATE utf8mb4_bin NULL,
    ADD KEY ix_ov_tasks_schedule_created
        (schedule_kind, schedule_id, created_at, task_id);

CREATE TABLE `ov_watch_tasks` (
    service_name VARCHAR(255) COLLATE utf8mb4_bin NOT NULL,
    task_id VARCHAR(255) COLLATE utf8mb4_bin NOT NULL,
    account_id VARCHAR(255) COLLATE utf8mb4_bin NOT NULL,
    user_id VARCHAR(255) COLLATE utf8mb4_bin NOT NULL,
    original_role VARCHAR(64) COLLATE ascii_bin NOT NULL DEFAULT 'user',
    target_uri_hash CHAR(64) COLLATE ascii_bin NOT NULL,
    target_uri_prefix VARCHAR(500) COLLATE utf8mb4_bin NOT NULL,
    target_uri TEXT COLLATE utf8mb4_bin NULL,
    enabled BOOLEAN NOT NULL DEFAULT 1,
    watch_interval DOUBLE NOT NULL DEFAULT 60,
    next_run_at DOUBLE NULL,
    current_run_id VARCHAR(255) COLLATE utf8mb4_bin NULL,
    run_started_at DOUBLE NULL,
    initial_run BOOLEAN NOT NULL DEFAULT 0,
    deleted BOOLEAN NOT NULL DEFAULT 0,
    submitted BOOLEAN NOT NULL DEFAULT 0,
    lease_owner VARCHAR(255) COLLATE utf8mb4_bin NULL,
    lease_until DOUBLE NOT NULL DEFAULT 0,
    revision INT NOT NULL DEFAULT 0,
    timer_revision INT NOT NULL DEFAULT -1,
    last_execution_time DOUBLE NULL,
    last_task_id VARCHAR(255) COLLATE utf8mb4_bin NULL,
    last_status VARCHAR(64) COLLATE ascii_bin NULL,
    last_error TEXT COLLATE utf8mb4_bin NULL,
    payload JSON NOT NULL,
    auth_state JSON NULL,
    connector_states JSON NULL,
    created_at DOUBLE NOT NULL,
    updated_at DOUBLE NOT NULL,
    PRIMARY KEY (service_name, task_id),
    KEY ix_ov_watch_tasks_due (service_name, enabled, next_run_at, task_id),
    KEY ix_ov_watch_tasks_account_target
        (service_name(128), account_id(128), target_uri_hash, task_id(128)),
    KEY ix_ov_watch_tasks_account_target_prefix
        (service_name(128), account_id(128), target_uri_prefix(128), task_id(128)),
    KEY ix_ov_watch_tasks_run (service_name, current_run_id, task_id),
    KEY ix_ov_watch_tasks_list
        (service_name(128), account_id(128), user_id(128), created_at, task_id(128)),
    KEY ix_ov_watch_tasks_list_active
        (service_name(128), account_id(128), user_id(128), enabled, created_at, task_id(128)),
    KEY ix_ov_watch_tasks_sweep_idle
        (service_name, enabled, current_run_id, next_run_at, task_id),
    KEY ix_ov_watch_tasks_sweep_inflight
        (service_name, enabled, current_run_id, updated_at, task_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC;

CREATE TABLE `ov_watch_tasks_uri_accounts` (
    service_name VARCHAR(255) COLLATE utf8mb4_bin NOT NULL,
    account_hash CHAR(64) COLLATE ascii_bin NOT NULL,
    PRIMARY KEY (service_name, account_hash)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC;

CREATE TABLE `ov_watch_tasks_uri_mutations` (
    service_name VARCHAR(255) COLLATE utf8mb4_bin NOT NULL,
    account_hash CHAR(64) COLLATE ascii_bin NOT NULL,
    token CHAR(36) COLLATE ascii_bin NOT NULL,
    scope_index INT NOT NULL,
    uri TEXT COLLATE utf8mb4_bin NOT NULL,
    expires_at DOUBLE NOT NULL,
    PRIMARY KEY (service_name, token, scope_index),
    KEY ix_ov_watch_uri_mutations_account (service_name, account_hash, expires_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC;
