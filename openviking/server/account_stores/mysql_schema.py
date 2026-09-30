# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0
"""SQLAlchemy schema for the MySQL account-store provider.

The table prefix is an operational namespace, while ``resource_id`` is the
tenant boundary.  The latter is deliberately present in every primary lookup,
unique key, and foreign-key relationship so one database can hold many
OpenViking deployments safely.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
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


def _schema_object_name(kind: str, semantic_name: str) -> str:
    """Return a stable schema-object name independent of configured table names."""
    return f"{kind}_ov_as_{semantic_name}"


def validate_table_prefix(table_prefix: object) -> str:
    if not isinstance(table_prefix, str) or not re.fullmatch(
        r"[A-Za-z][A-Za-z0-9_]{0,49}", table_prefix
    ):
        raise ValueError(
            "MySQL table must start with a letter, contain only letters, "
            "digits, or underscores, and be at most 50 characters"
        )
    return table_prefix


@dataclass(frozen=True)
class AccountStoreModels:
    Base: type[DeclarativeBase]
    Account: type
    User: type
    Group: type
    GroupMember: type
    Credential: type
    table_prefix: str

    @property
    def table_names(self) -> dict[str, str]:
        return {
            "accounts": self.Account.__tablename__,
            "users": self.User.__tablename__,
            "groups": self.Group.__tablename__,
            "memberships": self.GroupMember.__tablename__,
            "credentials": self.Credential.__tablename__,
        }


def make_account_store_models(table_prefix: str) -> AccountStoreModels:
    """Build a mapper set for one configured account-store table prefix."""
    table_prefix = validate_table_prefix(table_prefix)
    accounts = table_prefix
    users = f"{table_prefix}_users"
    groups = f"{table_prefix}_groups"
    members = f"{table_prefix}_group_members"
    credentials = f"{table_prefix}_credentials"

    class Base(DeclarativeBase):
        pass

    class Account(Base):
        __tablename__ = accounts
        __table_args__ = (
            UniqueConstraint(
                "resource_id",
                "account_id",
                name=_schema_object_name("uq", "account_identity"),
            ),
            UniqueConstraint(
                "resource_id",
                "account_pk",
                name=_schema_object_name("uq", "account_resource_pk"),
            ),
            Index(
                _schema_object_name("ix", "account_deletion"),
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
        __tablename__ = users
        __table_args__ = (
            ForeignKeyConstraint(
                ["resource_id", "account_pk"],
                [f"{accounts}.resource_id", f"{accounts}.account_pk"],
                ondelete="CASCADE",
                name=_schema_object_name("fk", "user_account"),
            ),
            UniqueConstraint(
                "resource_id",
                "account_pk",
                "user_id",
                name=_schema_object_name("uq", "user_identity"),
            ),
            UniqueConstraint(
                "resource_id",
                "user_pk",
                name=_schema_object_name("uq", "user_resource_pk"),
            ),
            Index(
                _schema_object_name("ix", "user_admin"),
                "resource_id",
                "account_pk",
                "role",
            ),
            Index(
                _schema_object_name("ix", "user_deletion"),
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
        __tablename__ = groups
        __table_args__ = (
            ForeignKeyConstraint(
                ["resource_id", "account_pk"],
                [f"{accounts}.resource_id", f"{accounts}.account_pk"],
                ondelete="CASCADE",
                name=_schema_object_name("fk", "group_account"),
            ),
            UniqueConstraint(
                "resource_id",
                "account_pk",
                "group_id",
                name=_schema_object_name("uq", "group_identity"),
            ),
            UniqueConstraint(
                "resource_id",
                "group_pk",
                name=_schema_object_name("uq", "group_resource_pk"),
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
        __tablename__ = members
        __table_args__ = (
            PrimaryKeyConstraint("resource_id", "group_pk", "user_pk"),
            ForeignKeyConstraint(
                ["resource_id", "group_pk"],
                [f"{groups}.resource_id", f"{groups}.group_pk"],
                ondelete="CASCADE",
                name=_schema_object_name("fk", "member_group"),
            ),
            ForeignKeyConstraint(
                ["resource_id", "user_pk"],
                [f"{users}.resource_id", f"{users}.user_pk"],
                ondelete="CASCADE",
                name=_schema_object_name("fk", "member_user"),
            ),
            Index(
                _schema_object_name("ix", "member_user"),
                "resource_id",
                "user_pk",
            ),
            {"mysql_engine": "InnoDB", "mysql_charset": "utf8mb4"},
        )

        resource_id: Mapped[str] = mapped_column(String(255, collation="utf8mb4_bin"))
        group_pk: Mapped[int] = mapped_column(BigInteger)
        user_pk: Mapped[int] = mapped_column(BigInteger)

    class Credential(Base):
        __tablename__ = credentials
        __table_args__ = (
            ForeignKeyConstraint(
                ["resource_id", "user_pk"],
                [f"{users}.resource_id", f"{users}.user_pk"],
                ondelete="CASCADE",
                name=_schema_object_name("fk", "credential_user"),
            ),
            UniqueConstraint(
                "resource_id",
                "active_user_pk",
                name=_schema_object_name("uq", "active_credential"),
            ),
            Index(
                _schema_object_name("ix", "credential_lookup"),
                "resource_id",
                "lookup_digest",
                "revoked",
                "user_pk",
            ),
            Index(
                _schema_object_name("ix", "credential_history"),
                "resource_id",
                "user_pk",
                "revoked",
                "sequence",
            ),
            {"mysql_engine": "InnoDB", "mysql_charset": "utf8mb4"},
        )

        credential_id: Mapped[str] = mapped_column(
            String(32, collation="ascii_bin"), primary_key=True
        )
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
        active_user_pk: Mapped[Optional[int]] = mapped_column(
            BigInteger,
            nullable=True,
        )

    return AccountStoreModels(Base, Account, User, Group, GroupMember, Credential, table_prefix)
