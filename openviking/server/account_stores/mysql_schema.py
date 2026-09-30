# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0
"""Fixed SQLAlchemy schema for the MySQL account-store provider.

``resource_id`` is the tenant boundary. It is deliberately present in every
primary lookup, unique key, and foreign-key relationship so one database can
hold many OpenViking deployments safely.
"""

from __future__ import annotations

from typing import Optional

from sqlalchemy import (
    BigInteger,
    Boolean,
    ForeignKeyConstraint,
    Index,
    PrimaryKeyConstraint,
    String,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.mysql import DATETIME
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Account(Base):
    __tablename__ = "ov_accounts"
    __table_args__ = (
        UniqueConstraint(
            "resource_id",
            "account_id",
            name="uq_ov_accounts_account_identity",
        ),
        UniqueConstraint(
            "resource_id",
            "account_pk",
            name="uq_ov_accounts_account_resource_pk",
        ),
        Index(
            "ix_ov_accounts_account_deletion",
            "resource_id",
            "deletion_task_id",
        ),
        {"mysql_engine": "InnoDB", "mysql_charset": "utf8mb4"},
    )

    account_pk: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    resource_id: Mapped[str] = mapped_column(String(255, collation="utf8mb4_bin"))
    account_id: Mapped[str] = mapped_column(String(255, collation="utf8mb4_bin"))
    created_at: Mapped[object] = mapped_column(
        DATETIME(fsp=6), server_default=text("CURRENT_TIMESTAMP(6)")
    )
    deletion_task_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    deletion_owner_account_id: Mapped[Optional[str]] = mapped_column(
        String(255, collation="utf8mb4_bin"), nullable=True
    )
    deletion_owner_user_id: Mapped[Optional[str]] = mapped_column(
        String(255, collation="utf8mb4_bin"), nullable=True
    )


class User(Base):
    __tablename__ = "ov_accounts_users"
    __table_args__ = (
        ForeignKeyConstraint(
            ["resource_id", "account_pk"],
            ["ov_accounts.resource_id", "ov_accounts.account_pk"],
            ondelete="CASCADE",
            name="fk_ov_accounts_user_account",
        ),
        UniqueConstraint(
            "resource_id",
            "account_pk",
            "user_id",
            name="uq_ov_accounts_user_identity",
        ),
        UniqueConstraint(
            "resource_id",
            "user_pk",
            name="uq_ov_accounts_user_resource_pk",
        ),
        UniqueConstraint(
            "resource_id",
            "account_pk",
            "user_pk",
            name="uq_ov_accounts_user_account_pk",
        ),
        Index(
            "ix_ov_accounts_user_admin",
            "resource_id",
            "account_pk",
            "role",
        ),
        Index(
            "ix_ov_accounts_user_deletion",
            "resource_id",
            "deletion_task_id",
        ),
        {"mysql_engine": "InnoDB", "mysql_charset": "utf8mb4"},
    )

    user_pk: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    resource_id: Mapped[str] = mapped_column(String(255, collation="utf8mb4_bin"))
    account_pk: Mapped[int] = mapped_column(BigInteger)
    user_id: Mapped[str] = mapped_column(String(255, collation="utf8mb4_bin"))
    role: Mapped[str] = mapped_column(String(16))
    created_at: Mapped[object] = mapped_column(
        DATETIME(fsp=6), server_default=text("CURRENT_TIMESTAMP(6)")
    )
    deletion_task_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    deletion_owner_account_id: Mapped[Optional[str]] = mapped_column(
        String(255, collation="utf8mb4_bin"), nullable=True
    )
    deletion_owner_user_id: Mapped[Optional[str]] = mapped_column(
        String(255, collation="utf8mb4_bin"), nullable=True
    )


class Group(Base):
    __tablename__ = "ov_accounts_groups"
    __table_args__ = (
        ForeignKeyConstraint(
            ["resource_id", "account_pk"],
            ["ov_accounts.resource_id", "ov_accounts.account_pk"],
            ondelete="CASCADE",
            name="fk_ov_accounts_group_account",
        ),
        UniqueConstraint(
            "resource_id",
            "account_pk",
            "group_id",
            name="uq_ov_accounts_group_identity",
        ),
        UniqueConstraint(
            "resource_id",
            "group_pk",
            name="uq_ov_accounts_group_resource_pk",
        ),
        UniqueConstraint(
            "resource_id",
            "account_pk",
            "group_pk",
            name="uq_ov_accounts_group_account_pk",
        ),
        {"mysql_engine": "InnoDB", "mysql_charset": "utf8mb4"},
    )

    group_pk: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    resource_id: Mapped[str] = mapped_column(String(255, collation="utf8mb4_bin"))
    account_pk: Mapped[int] = mapped_column(BigInteger)
    group_id: Mapped[str] = mapped_column(String(255, collation="utf8mb4_bin"))
    created_at: Mapped[object] = mapped_column(
        DATETIME(fsp=6), server_default=text("CURRENT_TIMESTAMP(6)")
    )


class GroupMember(Base):
    __tablename__ = "ov_accounts_group_members"
    __table_args__ = (
        PrimaryKeyConstraint("resource_id", "account_pk", "group_pk", "user_pk"),
        ForeignKeyConstraint(
            ["resource_id", "account_pk", "group_pk"],
            [
                "ov_accounts_groups.resource_id",
                "ov_accounts_groups.account_pk",
                "ov_accounts_groups.group_pk",
            ],
            ondelete="CASCADE",
            name="fk_ov_accounts_member_group",
        ),
        ForeignKeyConstraint(
            ["resource_id", "account_pk", "user_pk"],
            [
                "ov_accounts_users.resource_id",
                "ov_accounts_users.account_pk",
                "ov_accounts_users.user_pk",
            ],
            ondelete="CASCADE",
            name="fk_ov_accounts_member_user",
        ),
        Index(
            "ix_ov_accounts_member_user",
            "resource_id",
            "account_pk",
            "user_pk",
        ),
        {"mysql_engine": "InnoDB", "mysql_charset": "utf8mb4"},
    )

    resource_id: Mapped[str] = mapped_column(String(255, collation="utf8mb4_bin"))
    account_pk: Mapped[int] = mapped_column(BigInteger)
    group_pk: Mapped[int] = mapped_column(BigInteger)
    user_pk: Mapped[int] = mapped_column(BigInteger)


class Credential(Base):
    __tablename__ = "ov_accounts_credentials"
    __table_args__ = (
        ForeignKeyConstraint(
            ["resource_id", "user_pk"],
            ["ov_accounts_users.resource_id", "ov_accounts_users.user_pk"],
            ondelete="CASCADE",
            name="fk_ov_accounts_credential_user",
        ),
        UniqueConstraint(
            "resource_id",
            "active_user_pk",
            name="uq_ov_accounts_active_credential",
        ),
        Index(
            "ix_ov_accounts_credential_lookup",
            "resource_id",
            "lookup_digest",
            "revoked",
            "user_pk",
        ),
        Index(
            "ix_ov_accounts_credential_history",
            "resource_id",
            "user_pk",
            "revoked",
            "sequence",
        ),
        {"mysql_engine": "InnoDB", "mysql_charset": "utf8mb4"},
    )

    credential_id: Mapped[str] = mapped_column(String(32, collation="ascii_bin"), primary_key=True)
    resource_id: Mapped[str] = mapped_column(String(255, collation="utf8mb4_bin"))
    user_pk: Mapped[int] = mapped_column(BigInteger)
    material: Mapped[str] = mapped_column(Text(collation="ascii_bin"))
    lookup_digest: Mapped[str] = mapped_column(String(64, collation="ascii_bin"))
    prefix: Mapped[str] = mapped_column(String(8, collation="ascii_bin"))
    revoked: Mapped[bool] = mapped_column(Boolean, server_default=text("0"))
    version: Mapped[int] = mapped_column(BigInteger, server_default=text("1"))
    sequence: Mapped[int] = mapped_column(BigInteger)
    created_at: Mapped[object] = mapped_column(
        DATETIME(fsp=6), server_default=text("CURRENT_TIMESTAMP(6)")
    )
    revoked_at: Mapped[Optional[object]] = mapped_column(DATETIME(fsp=6), nullable=True)
    active_user_pk: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
