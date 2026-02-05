# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Company Email Scraper is an automated pipeline for discovering company email addresses. It scrapes German business directories (Stadtbranchenbuch), filters companies using AI classification (Claude Haiku), enriches with email discovery via Manus API, and exports results as CSV.

## Development Commands

```bash
# Install dependencies
pip install -r requirements.txt

# Run database migrations
alembic upgrade head

# Start development server
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000

# Create new migration after model changes
alembic revision --autogenerate -m "description"

# Rollback one migration
alembic downgrade -1
```

## Architecture

### Technology Stack
- **FastAPI** with async/await throughout
- **SQLModel** (SQLAlchemy + Pydantic) with asyncpg driver
- **PostgreSQL** with JSONB columns for flexible data
- **Alembic** for migrations
- **Jinja2** templates with HTTP-only JWT cookies for auth

### Four-Phase Job Pipeline

Jobs execute through four sequential phases in `app/services/job_processor.py`:

1. **Scraping** (`scraper_service.py`) - Streams companies from Stadtbranchenbuch with user agent rotation and request delays
2. **Filtering** (`claude_filter_service.py`) - Batch AI classification (50-100 companies per batch) with rule-based fallback
3. **Enriching** (`manus_service.py`) - Email discovery via Manus API with rate limiting, webhook/polling support
4. **Exporting** - CSV generation with streaming response

Each phase has retry logic (3 retries with exponential backoff), status tracking, and duration measurement.

### Key Patterns

**Lifespan Management** (`app/main.py`):
- Startup: DB health check, resume incomplete jobs, start timeout monitor
- Shutdown: 5-minute graceful wait for active jobs, close HTTP clients
- Background tasks tracked in `task_registry.py` for proper shutdown

**Configuration** (`app/config.py`):
- Nested dataclass structure: `AuthConfig`, `DatabaseConfig`, `APIConfig`, `EmailConfig`, `AppConfig`
- Lazy singleton via `get_settings()` - initialized once on first call
- Validates and clamps batch sizes

**Database Models** (`app/models/`):
- `Job` - Parent record with phase-specific status, timestamps, durations, errors (JSONB array)
- `Company` - Scraped data + AI classification + enrichment fields
- `ManusTask` - Individual enrichment task tracking with JSONB result storage

### Service Dependencies

```
job_processor.py
├── scraper_service.py (web scraping)
├── claude_filter_service.py (AI classification)
├── manus_service.py (email enrichment)
├── email_service.py (notifications)
└── csv_service.py (export)
```

HTTP clients for Claude and Manus APIs are global singletons closed during shutdown.

## Required Environment Variables

```
DATABASE_URL          # PostgreSQL connection (auto-set by Railway)
JWT_SECRET_KEY        # Required for auth
USERS                 # Format: email1:pass1,email2:pass2
MANUS_API_KEY         # Required for email enrichment
ANTHROPIC_API_KEY     # Required for AI classification
```

See `.env.example` for all configuration options with descriptions.

## Web Flow

1. `GET /` → login page
2. `POST /login` → authenticate, set JWT cookie
3. `GET /submit` → job submission form
4. `POST /scrape` → create job, launch background task
5. `GET /history` → view user's jobs
6. `GET /results/{job_id}` → stream CSV download
7. `POST /webhooks/manus` → receive enrichment callbacks
