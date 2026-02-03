from datetime import datetime
from typing import TYPE_CHECKING, Optional

from sqlalchemy import Column, ForeignKey, Index, String, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlmodel import Field, Relationship, SQLModel

if TYPE_CHECKING:
    from .company import Company
    from .job import Job


class ManusTask(SQLModel, table=True):
    __tablename__ = "manus_tasks"
    __table_args__ = (
        Index("idx_manus_tasks_job_id", "job_id"),
        Index("idx_manus_tasks_company_id", "company_id"),
        Index("idx_manus_tasks_status", "status"),
        Index("idx_manus_tasks_id", "id", unique=True),
    )

    id: str = Field(sa_column=Column(String(50), primary_key=True))
    job_id: str = Field(
        sa_column=Column(String(50), ForeignKey("jobs.id"), nullable=False),
    )
    company_id: str = Field(
        sa_column=Column(String(50), ForeignKey("companies.id"), nullable=False),
    )

    # Status and timestamps
    status: str = Field(
        default="pending",
        sa_column=Column(String(50), nullable=False, server_default="pending"),
    )
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    completed_at: Optional[datetime] = Field(default=None)

    # Result fields
    result: Optional[dict] = Field(default=None, sa_column=Column(JSONB))
    error: Optional[str] = Field(default=None, sa_column=Column(Text))

    # Relationships
    job: Optional["Job"] = Relationship(back_populates="manus_tasks")
    company: Optional["Company"] = Relationship(back_populates="manus_tasks")
