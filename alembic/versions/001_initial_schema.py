"""Initial schema: jobs, companies, manus_tasks

Revision ID: 001
Revises:
Create Date: 2026-02-03

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # --- Jobs table ---
    op.create_table(
        "jobs",
        sa.Column("id", sa.String(50), primary_key=True),
        sa.Column("user_email", sa.String(255), nullable=False),
        sa.Column("city", sa.String(255), nullable=False, server_default=""),
        sa.Column("zip_code", sa.String(20), nullable=False, server_default=""),
        sa.Column("status", sa.String(50), nullable=False, server_default="pending"),
        sa.Column("current_phase", sa.String(50), nullable=True),
        sa.Column("manus_project_id", sa.String(255), nullable=True),
        sa.Column("manus_webhook_id", sa.String(255), nullable=True),
        # Timestamps
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column("started_at", sa.DateTime(), nullable=True),
        sa.Column("completed_at", sa.DateTime(), nullable=True),
        # Statistics
        sa.Column("stats_scraped", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("stats_filtered_valid", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("stats_filtered_excluded", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("stats_enrichment_pending", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("stats_enrichment_completed", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("stats_enrichment_failed", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("stats_emails_found", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("stats_email_coverage", sa.Float(), nullable=False, server_default="0.0"),
        # Phase tracking - Scraping
        sa.Column("scraping_status", sa.String(50), nullable=True),
        sa.Column("scraping_started_at", sa.DateTime(), nullable=True),
        sa.Column("scraping_completed_at", sa.DateTime(), nullable=True),
        sa.Column("scraping_duration_seconds", sa.Integer(), nullable=True),
        # Phase tracking - Filtering
        sa.Column("filtering_status", sa.String(50), nullable=True),
        sa.Column("filtering_started_at", sa.DateTime(), nullable=True),
        sa.Column("filtering_completed_at", sa.DateTime(), nullable=True),
        sa.Column("filtering_duration_seconds", sa.Integer(), nullable=True),
        # Phase tracking - Enriching
        sa.Column("enriching_status", sa.String(50), nullable=True),
        sa.Column("enriching_started_at", sa.DateTime(), nullable=True),
        sa.Column("enriching_completed_at", sa.DateTime(), nullable=True),
        sa.Column("enriching_duration_seconds", sa.Integer(), nullable=True),
        # Phase tracking - Exporting
        sa.Column("exporting_status", sa.String(50), nullable=True),
        sa.Column("exporting_started_at", sa.DateTime(), nullable=True),
        sa.Column("exporting_completed_at", sa.DateTime(), nullable=True),
        sa.Column("exporting_duration_seconds", sa.Integer(), nullable=True),
        # Error tracking (JSONB)
        sa.Column("errors", sa.JSON(), nullable=True),
    )
    op.create_index("idx_jobs_user_email", "jobs", ["user_email"])
    op.create_index("idx_jobs_status", "jobs", ["status"])
    op.create_index("idx_jobs_created_at", "jobs", ["created_at"])

    # --- Companies table ---
    op.create_table(
        "companies",
        sa.Column("id", sa.String(50), primary_key=True),
        sa.Column(
            "job_id",
            sa.String(50),
            sa.ForeignKey("jobs.id", ondelete="CASCADE"),
            nullable=False,
        ),
        # Scraper fields
        sa.Column("name", sa.String(500), nullable=False, server_default=""),
        sa.Column("address", sa.Text(), nullable=True),
        sa.Column("street", sa.String(500), nullable=True),
        sa.Column("postal_code", sa.String(20), nullable=True),
        sa.Column("city", sa.String(255), nullable=True),
        sa.Column("phone", sa.String(100), nullable=True),
        sa.Column("category", sa.String(500), nullable=True),
        sa.Column("website", sa.String(1000), nullable=True),
        sa.Column("detail_url", sa.String(1000), nullable=True),
        # AI classification fields
        sa.Column("ai_classification", sa.String(100), nullable=True),
        sa.Column("ai_classification_reason", sa.Text(), nullable=True),
        sa.Column("filtered_out", sa.Boolean(), nullable=False, server_default="false"),
        # Enrichment fields
        sa.Column("enrichment_status", sa.String(50), nullable=False, server_default="pending"),
        sa.Column("email", sa.String(500), nullable=True),
        sa.Column("email_source", sa.String(100), nullable=True),
        sa.Column("retry_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("error_timestamp", sa.DateTime(), nullable=True),
        # Timestamps
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
    )
    op.create_index("idx_companies_job_id", "companies", ["job_id"])
    op.create_index("idx_companies_enrichment_status", "companies", ["enrichment_status"])
    op.create_index(
        "idx_companies_email",
        "companies",
        ["email"],
        postgresql_where=sa.text("email IS NOT NULL"),
    )

    # --- Manus Tasks table ---
    op.create_table(
        "manus_tasks",
        sa.Column("id", sa.String(50), primary_key=True),
        sa.Column(
            "job_id",
            sa.String(50),
            sa.ForeignKey("jobs.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "company_id",
            sa.String(50),
            sa.ForeignKey("companies.id", ondelete="CASCADE"),
            nullable=False,
        ),
        # Status and timestamps
        sa.Column("status", sa.String(50), nullable=False, server_default="pending"),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column("completed_at", sa.DateTime(), nullable=True),
        # Result fields (JSONB)
        sa.Column("result", sa.JSON(), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
    )
    op.create_index("idx_manus_tasks_job_id", "manus_tasks", ["job_id"])
    op.create_index("idx_manus_tasks_company_id", "manus_tasks", ["company_id"])
    op.create_index("idx_manus_tasks_status", "manus_tasks", ["status"])
    op.create_index("idx_manus_tasks_id", "manus_tasks", ["id"], unique=True)


def downgrade() -> None:
    op.drop_table("manus_tasks")
    op.drop_table("companies")
    op.drop_table("jobs")
