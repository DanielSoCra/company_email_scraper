from datetime import datetime
from typing import TYPE_CHECKING, Optional

from sqlalchemy import Column, Index, String, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlmodel import Field, Relationship, SQLModel

if TYPE_CHECKING:
    from .company import Company
    from .manus_task import ManusTask


class Job(SQLModel, table=True):
    __tablename__ = "jobs"
    __table_args__ = (
        Index("idx_jobs_user_email", "user_email"),
        Index("idx_jobs_status", "status"),
        Index("idx_jobs_created_at", "created_at"),
    )

    id: str = Field(sa_column=Column(String(50), primary_key=True))
    user_email: str = Field(sa_column=Column(String(255), nullable=False))
    city: str = Field(default="", sa_column=Column(String(255), nullable=False, server_default=""))
    zip_code: str = Field(default="", sa_column=Column(String(20), nullable=False, server_default=""))
    status: str = Field(default="pending", sa_column=Column(String(50), nullable=False, server_default="pending"))
    current_phase: Optional[str] = Field(default=None, sa_column=Column(String(50)))
    manus_project_id: Optional[str] = Field(default=None, sa_column=Column(String(255)))
    manus_webhook_id: Optional[str] = Field(default=None, sa_column=Column(String(255)))

    # Timestamps
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    started_at: Optional[datetime] = Field(default=None)
    completed_at: Optional[datetime] = Field(default=None)

    # Statistics
    stats_scraped: int = Field(default=0)
    stats_filtered_valid: int = Field(default=0)
    stats_filtered_excluded: int = Field(default=0)
    stats_enrichment_pending: int = Field(default=0)
    stats_enrichment_completed: int = Field(default=0)
    stats_enrichment_failed: int = Field(default=0)
    stats_emails_found: int = Field(default=0)
    stats_email_coverage: float = Field(default=0.0)

    # Phase tracking - Scraping
    scraping_status: Optional[str] = Field(default=None, sa_column=Column(String(50)))
    scraping_started_at: Optional[datetime] = Field(default=None)
    scraping_completed_at: Optional[datetime] = Field(default=None)
    scraping_duration_seconds: Optional[int] = Field(default=None)

    # Phase tracking - Filtering
    filtering_status: Optional[str] = Field(default=None, sa_column=Column(String(50)))
    filtering_started_at: Optional[datetime] = Field(default=None)
    filtering_completed_at: Optional[datetime] = Field(default=None)
    filtering_duration_seconds: Optional[int] = Field(default=None)

    # Phase tracking - Enriching
    enriching_status: Optional[str] = Field(default=None, sa_column=Column(String(50)))
    enriching_started_at: Optional[datetime] = Field(default=None)
    enriching_completed_at: Optional[datetime] = Field(default=None)
    enriching_duration_seconds: Optional[int] = Field(default=None)

    # Phase tracking - Exporting
    exporting_status: Optional[str] = Field(default=None, sa_column=Column(String(50)))
    exporting_started_at: Optional[datetime] = Field(default=None)
    exporting_completed_at: Optional[datetime] = Field(default=None)
    exporting_duration_seconds: Optional[int] = Field(default=None)

    # Error tracking (JSONB)
    errors: Optional[list] = Field(default=None, sa_column=Column(JSONB))

    # Relationships
    companies: list["Company"] = Relationship(back_populates="job")
    manus_tasks: list["ManusTask"] = Relationship(back_populates="job")
