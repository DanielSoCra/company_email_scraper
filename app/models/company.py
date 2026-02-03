from datetime import datetime
from typing import TYPE_CHECKING, Optional

from sqlalchemy import Column, ForeignKey, Index, String, Text
from sqlmodel import Field, Relationship, SQLModel

if TYPE_CHECKING:
    from .job import Job
    from .manus_task import ManusTask


class Company(SQLModel, table=True):
    __tablename__ = "companies"
    __table_args__ = (
        Index("idx_companies_job_id", "job_id"),
        Index("idx_companies_enrichment_status", "enrichment_status"),
        Index(
            "idx_companies_email",
            "email",
            postgresql_where="email IS NOT NULL",
        ),
    )

    id: str = Field(sa_column=Column(String(50), primary_key=True))
    job_id: str = Field(
        sa_column=Column(String(50), ForeignKey("jobs.id"), nullable=False),
    )

    # Scraper fields
    name: str = Field(default="", sa_column=Column(String(500), nullable=False, server_default=""))
    address: Optional[str] = Field(default=None, sa_column=Column(Text))
    street: Optional[str] = Field(default=None, sa_column=Column(String(500)))
    postal_code: Optional[str] = Field(default=None, sa_column=Column(String(20)))
    city: Optional[str] = Field(default=None, sa_column=Column(String(255)))
    phone: Optional[str] = Field(default=None, sa_column=Column(String(100)))
    category: Optional[str] = Field(default=None, sa_column=Column(String(500)))
    website: Optional[str] = Field(default=None, sa_column=Column(String(1000)))
    detail_url: Optional[str] = Field(default=None, sa_column=Column(String(1000)))

    # AI classification fields
    ai_classification: Optional[str] = Field(default=None, sa_column=Column(String(100)))
    ai_classification_reason: Optional[str] = Field(default=None, sa_column=Column(Text))
    filtered_out: bool = Field(default=False)

    # Enrichment fields
    enrichment_status: str = Field(
        default="pending",
        sa_column=Column(String(50), nullable=False, server_default="pending"),
    )
    email: Optional[str] = Field(default=None, sa_column=Column(String(500)))
    email_source: Optional[str] = Field(default=None, sa_column=Column(String(100)))
    retry_count: int = Field(default=0)
    last_error: Optional[str] = Field(default=None, sa_column=Column(Text))
    error_timestamp: Optional[datetime] = Field(default=None)

    # Timestamps
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    # Relationships
    job: Optional["Job"] = Relationship(back_populates="companies")
    manus_tasks: list["ManusTask"] = Relationship(back_populates="company")
