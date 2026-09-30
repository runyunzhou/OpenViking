-- OpenViking MySQL account store initial schema.
-- Table prefix: ov_accounts (the default account_store.params.table value).
-- Execute manually before starting a MySQL account-store deployment.
-- For a custom table prefix, replace only backtick-quoted ov_accounts table
-- identifiers. Constraint and index names are stable and table-name independent.

CREATE TABLE `ov_accounts` (
    account_pk BIGINT NOT NULL AUTO_INCREMENT,
    resource_id VARCHAR(255) COLLATE utf8mb4_bin NOT NULL,
    account_id VARCHAR(255) COLLATE utf8mb4_bin NOT NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    deletion_task_id VARCHAR(64) NULL,
    deletion_owner_account_id VARCHAR(255) COLLATE utf8mb4_bin NULL,
    deletion_owner_user_id VARCHAR(255) COLLATE utf8mb4_bin NULL,
    PRIMARY KEY (account_pk),
    CONSTRAINT uq_ov_as_account_identity UNIQUE (resource_id, account_id),
    CONSTRAINT uq_ov_as_account_resource_pk UNIQUE (resource_id, account_pk),
    INDEX ix_ov_as_account_deletion (resource_id, deletion_task_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `ov_accounts_users` (
    user_pk BIGINT NOT NULL AUTO_INCREMENT,
    resource_id VARCHAR(255) COLLATE utf8mb4_bin NOT NULL,
    account_pk BIGINT NOT NULL,
    user_id VARCHAR(255) COLLATE utf8mb4_bin NOT NULL,
    `role` VARCHAR(16) NOT NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    deletion_task_id VARCHAR(64) NULL,
    deletion_owner_account_id VARCHAR(255) COLLATE utf8mb4_bin NULL,
    deletion_owner_user_id VARCHAR(255) COLLATE utf8mb4_bin NULL,
    PRIMARY KEY (user_pk),
    CONSTRAINT fk_ov_as_user_account
        FOREIGN KEY (resource_id, account_pk)
        REFERENCES `ov_accounts` (resource_id, account_pk)
        ON DELETE CASCADE,
    CONSTRAINT uq_ov_as_user_identity
        UNIQUE (resource_id, account_pk, user_id),
    CONSTRAINT uq_ov_as_user_resource_pk UNIQUE (resource_id, user_pk),
    INDEX ix_ov_as_user_admin (resource_id, account_pk, `role`),
    INDEX ix_ov_as_user_deletion (resource_id, deletion_task_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `ov_accounts_groups` (
    group_pk BIGINT NOT NULL AUTO_INCREMENT,
    resource_id VARCHAR(255) COLLATE utf8mb4_bin NOT NULL,
    account_pk BIGINT NOT NULL,
    group_id VARCHAR(255) COLLATE utf8mb4_bin NOT NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (group_pk),
    CONSTRAINT fk_ov_as_group_account
        FOREIGN KEY (resource_id, account_pk)
        REFERENCES `ov_accounts` (resource_id, account_pk)
        ON DELETE CASCADE,
    CONSTRAINT uq_ov_as_group_identity
        UNIQUE (resource_id, account_pk, group_id),
    CONSTRAINT uq_ov_as_group_resource_pk UNIQUE (resource_id, group_pk)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `ov_accounts_group_members` (
    resource_id VARCHAR(255) COLLATE utf8mb4_bin NOT NULL,
    group_pk BIGINT NOT NULL,
    user_pk BIGINT NOT NULL,
    PRIMARY KEY (resource_id, group_pk, user_pk),
    CONSTRAINT fk_ov_as_member_group
        FOREIGN KEY (resource_id, group_pk)
        REFERENCES `ov_accounts_groups` (resource_id, group_pk)
        ON DELETE CASCADE,
    CONSTRAINT fk_ov_as_member_user
        FOREIGN KEY (resource_id, user_pk)
        REFERENCES `ov_accounts_users` (resource_id, user_pk)
        ON DELETE CASCADE,
    INDEX ix_ov_as_member_user (resource_id, user_pk)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `ov_accounts_credentials` (
    credential_id VARCHAR(32) COLLATE ascii_bin NOT NULL,
    resource_id VARCHAR(255) COLLATE utf8mb4_bin NOT NULL,
    user_pk BIGINT NOT NULL,
    material TEXT COLLATE ascii_bin NOT NULL,
    lookup_digest VARCHAR(64) COLLATE ascii_bin NOT NULL,
    prefix VARCHAR(8) COLLATE ascii_bin NOT NULL,
    revoked BOOLEAN NOT NULL DEFAULT 0,
    version BIGINT NOT NULL DEFAULT 1,
    sequence BIGINT NOT NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    revoked_at DATETIME(6) NULL,
    active_user_pk BIGINT NULL,
    PRIMARY KEY (credential_id),
    CONSTRAINT fk_ov_as_credential_user
        FOREIGN KEY (resource_id, user_pk)
        REFERENCES `ov_accounts_users` (resource_id, user_pk)
        ON DELETE CASCADE,
    CONSTRAINT uq_ov_as_active_credential
        UNIQUE (resource_id, active_user_pk),
    INDEX ix_ov_as_credential_lookup
        (resource_id, lookup_digest, revoked, user_pk),
    INDEX ix_ov_as_credential_history
        (resource_id, user_pk, revoked, sequence)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
