# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

FastAPI-based REST API with SQLAlchemy ORM, using SQLite as the database. Managed with `uv` for dependency and environment management. Python 3.13+.

## Development Commands

**Start development server:**
```bash
uv run uvicorn app.main:app --reload
```

**Run all tests:**
```bash
uv run pytest
```

**Run specific test:**
```bash
uv run pytest tests/api/v1/test_user.py::test_create_and_get_user
```

**Docker:**
```bash
docker-compose up --build
```

## Architecture

### Layer Separation

The codebase follows a clean 3-layer architecture:

1. **API Layer** (`app/api/v1/`): FastAPI routers that handle HTTP requests/responses. Uses dependency injection for services.

2. **Service Layer** (`app/services/`): Business logic. Services receive a SQLAlchemy session in their constructor and handle all database operations.

3. **Data Layer** (`app/db/schema.py`): SQLAlchemy ORM models (tables). Separate from Pydantic models.

### Model Split

- **ORM Models** (`app/db/schema.py`): SQLAlchemy table definitions inheriting from `Base`
- **Pydantic Models** (`app/models/`): Request/response schemas for API validation (e.g., `UserCreate`, `UserRead`)

### Database Setup

- Database initialization happens at application startup in `app/main.py:9` via `Base.metadata.create_all()`
- Default: SQLite file at `./test.db`
- Configuration via environment variables or `.env` file (see `.env.example`)

### Dependency Injection Pattern

Services are injected into route handlers using FastAPI's `Depends()`. Example in `app/api/v1/user.py:10-11`:

```python
def get_user_service() -> UserService:
    return UserService(session=SessionLocal())
```

### Testing

Tests use an in-memory SQLite database configured in `tests/test_db.py`. The test setup overrides the production `get_user_service` dependency to inject the test database session instead of the production one.

## Configuration

Environment variables loaded via `python-dotenv` from `.env` file. Configuration centralized in `app/core/config.py` using Pydantic Settings. Key settings:
- `app_name`: Application name
- `db_user`, `db_password`: Database credentials (currently unused for SQLite)
- `db_name`: Database filename (default: `test.db`)

## Adding New Endpoints

When adding a new resource endpoint:
1. Create ORM model in `app/db/schema.py`
2. Create Pydantic schemas in `app/models/`
3. Create service class in `app/services/`
4. Create router in `app/api/v1/`
5. Register router in `app/main.py` using `app.include_router()`
6. Write tests in `tests/api/v1/`

## Source Control

**Repository:** `git@github.com:nichom01/cohort-manager-api.git`

**Commit Message Guidelines:**
- Focus on functional changes made since the last commit
- Do NOT mention Claude, AI assistance, or automated generation
- Write clear, descriptive messages about what changed and why
- Use conventional commit format when appropriate (e.g., `feat:`, `fix:`, `refactor:`)
- Example: "Add email validation to user registration endpoint" not "Claude added email validation"
