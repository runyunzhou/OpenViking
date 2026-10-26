-- Upgrade the service-partitioned Watch table from 001/003.
-- Run once during a stopped deployment. Fresh installs use the updated 001
-- and MUST NOT run this migration. Custom names must replace ov_watch_tasks
-- and its two suffix tables together. Compatible with MySQL 5.7.44.

ALTER TABLE `ov_watch_tasks`
    ADD COLUMN run_started_at DOUBLE NULL,
    ADD COLUMN initial_run BOOLEAN NOT NULL DEFAULT 0,
    ADD COLUMN deleted BOOLEAN NOT NULL DEFAULT 0,
    ADD COLUMN timer_revision INT NOT NULL DEFAULT -1,
    DROP KEY ix_ov_watch_tasks_account_target,
    DROP KEY ix_ov_watch_tasks_account_target_prefix,
    DROP KEY ix_ov_watch_tasks_list,
    DROP KEY ix_ov_watch_tasks_list_active,
    ADD KEY ix_ov_watch_tasks_account_target
        (service_name(128), account_id(128), target_uri_hash, task_id(128)),
    ADD KEY ix_ov_watch_tasks_account_target_prefix
        (service_name(128), account_id(128), target_uri_prefix(128), task_id(128)),
    ADD KEY ix_ov_watch_tasks_list
        (service_name(128), account_id(128), user_id(128), created_at, task_id(128)),
    ADD KEY ix_ov_watch_tasks_list_active
        (service_name(128), account_id(128), user_id(128), enabled, created_at, task_id(128));

UPDATE `ov_watch_tasks`
SET run_started_at=updated_at,
    initial_run=(lease_owner IS NULL)
WHERE current_run_id IS NOT NULL;

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
