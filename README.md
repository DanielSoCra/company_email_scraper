# Company Email Scraper

Automated pipeline for discovering company email addresses. Scrapes business directories, filters companies using AI classification, enriches with email discovery via Manus API, and exports results.

## Technology Stack

- **FastAPI** - Async web framework
- **SQLModel** - Type-safe ORM combining SQLAlchemy and Pydantic
- **PostgreSQL** - Primary database (via asyncpg)
- **Alembic** - Database migrations
- **Jinja2** - HTML templates

## Prerequisites

- Python 3.11
- PostgreSQL 14+

## Local Development Setup

```bash
# Install dependencies
pip install -r requirements.txt

# Setup environment
cp .env.example .env
# Edit .env with your configuration

# Run migrations
alembic upgrade head

# Start development server
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

## Environment Variables

See `.env.example` for all required configuration. Key variables:

| Variable | Description |
|----------|-------------|
| `DATABASE_URL` | PostgreSQL connection string |
| `JWT_SECRET_KEY` | Secret for JWT token signing |
| `USERS` | Comma-separated `email:password` pairs |
| `MANUS_API_KEY` | API key for Manus email enrichment |
| `ANTHROPIC_API_KEY` | API key for Claude AI classification |

## Database Migrations

```bash
# Apply all migrations
alembic upgrade head

# Create a new migration
alembic revision --autogenerate -m "description"

# Rollback one migration
alembic downgrade -1
```

## Project Structure

```
company_email_scraper/
├── app/
│   ├── main.py          # FastAPI application entry point
│   ├── config.py        # Environment variable configuration
│   ├── database.py      # Database connection and session management
│   ├── models/          # SQLModel database models
│   ├── routes/          # API endpoints
│   ├── services/        # Business logic
│   └── templates/       # Jinja2 HTML templates
├── alembic/             # Database migrations
├── alembic.ini          # Alembic settings
├── requirements.txt     # Python dependencies
├── .env.example         # Environment variable template
└── .python-version      # Python version
```
