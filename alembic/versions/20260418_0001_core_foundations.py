from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260418_0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "accounts",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("slug", sa.String(length=100), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False, server_default="active"),
        sa.Column("default_currency", sa.String(length=8), nullable=False, server_default="RUB"),
        sa.Column("default_timezone", sa.String(length=64), nullable=False, server_default="Etc/UTC"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.UniqueConstraint("slug"),
    )
    op.create_index("ix_accounts_slug", "accounts", ["slug"], unique=True)

    op.create_table(
        "users",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("email", sa.String(length=255), nullable=False),
        sa.Column("full_name", sa.String(length=255), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False, server_default="active"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.UniqueConstraint("email"),
    )
    op.create_index("ix_users_email", "users", ["email"], unique=True)

    op.create_table(
        "roles",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("code", sa.String(length=64), nullable=False),
        sa.Column("name", sa.String(length=128), nullable=False),
        sa.Column("description", sa.Text(), nullable=False, server_default=""),
        sa.Column("is_system", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.UniqueConstraint("code"),
    )
    op.create_index("ix_roles_code", "roles", ["code"], unique=True)

    op.create_table(
        "permissions",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("code", sa.String(length=100), nullable=False),
        sa.Column("name", sa.String(length=128), nullable=False),
        sa.Column("module", sa.String(length=64), nullable=False),
        sa.Column("description", sa.Text(), nullable=False, server_default=""),
        sa.UniqueConstraint("code"),
    )
    op.create_index("ix_permissions_code", "permissions", ["code"], unique=True)

    op.create_table(
        "role_permissions",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("role_id", sa.Integer(), nullable=False),
        sa.Column("permission_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(["permission_id"], ["permissions.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["role_id"], ["roles.id"], ondelete="CASCADE"),
        sa.UniqueConstraint("role_id", "permission_id"),
    )

    op.create_table(
        "account_users",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("account_id", sa.Integer(), nullable=False),
        sa.Column("user_id", sa.Integer(), nullable=False),
        sa.Column("role_id", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False, server_default="active"),
        sa.Column("joined_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.ForeignKeyConstraint(["account_id"], ["accounts.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["role_id"], ["roles.id"]),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.UniqueConstraint("account_id", "user_id"),
    )
    op.create_index("ix_account_users_account_id", "account_users", ["account_id"], unique=False)
    op.create_index("ix_account_users_user_id", "account_users", ["user_id"], unique=False)
    op.create_index("ix_account_users_role_id", "account_users", ["role_id"], unique=False)

    op.create_table(
        "audit_logs",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("account_id", sa.Integer(), nullable=False),
        sa.Column("actor_user_id", sa.Integer(), nullable=True),
        sa.Column("source", sa.String(length=64), nullable=False, server_default="system"),
        sa.Column("action", sa.String(length=128), nullable=False),
        sa.Column("entity_type", sa.String(length=64), nullable=False),
        sa.Column("entity_id", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False, server_default="success"),
        sa.Column("request_id", sa.String(length=64), nullable=True),
        sa.Column("details_json", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["account_id"], ["accounts.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["actor_user_id"], ["users.id"], ondelete="SET NULL"),
    )
    op.create_index("ix_audit_logs_account_created_at", "audit_logs", ["account_id", "created_at"], unique=False)
    op.create_index("ix_audit_logs_account_actor_created_at", "audit_logs", ["account_id", "actor_user_id", "created_at"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_audit_logs_account_actor_created_at", table_name="audit_logs")
    op.drop_index("ix_audit_logs_account_created_at", table_name="audit_logs")
    op.drop_table("audit_logs")

    op.drop_index("ix_account_users_role_id", table_name="account_users")
    op.drop_index("ix_account_users_user_id", table_name="account_users")
    op.drop_index("ix_account_users_account_id", table_name="account_users")
    op.drop_table("account_users")

    op.drop_table("role_permissions")

    op.drop_index("ix_permissions_code", table_name="permissions")
    op.drop_table("permissions")

    op.drop_index("ix_roles_code", table_name="roles")
    op.drop_table("roles")

    op.drop_index("ix_users_email", table_name="users")
    op.drop_table("users")

    op.drop_index("ix_accounts_slug", table_name="accounts")
    op.drop_table("accounts")
