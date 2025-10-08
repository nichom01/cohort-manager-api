# FastAPI User Management API

A FastAPI-based REST API with SQLAlchemy ORM and SQLite database.

## Prerequisites

Install [uv](https://docs.astral.sh/uv/) package manager:

**macOS/Linux:**
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

**Windows:**
```bash
powershell -c "irm https://astral.sh/uv/install.ps1 | iex"
```

## Running Locally

Start the development server:

```bash
uv run uvicorn app.main:app --reload
```

The API will be available at `http://localhost:8000`

Interactive API documentation: `http://localhost:8000/docs`

## Running Tests

Run all tests:

```bash
uv run pytest
```

Run specific test file:

```bash
uv run pytest tests/api/v1/test_user.py
```

Run with verbose output:

```bash
uv run pytest -v
```

## Running with Docker

Build and run with Docker Compose:

```bash
docker-compose up --build
```

Or build and run with Docker directly:

```bash
docker build -t fastapi-app .
docker run -p 8000:80 fastapi-app
```

The API will be available at `http://localhost:8000`

## Example Requests

Here's a set of simple curl examples you can use to interact with the API:

### 1️⃣ Create a User

```bash
curl -X POST "http://localhost:8000/api/v1/users" \
     -H "Content-Type: application/json" \
     -d '{"name": "Ada Lovelace"}'
```

### 2️⃣ Get All Users

```bash
curl -X GET "http://localhost:8000/api/v1/users"
```

### 3️⃣ Get a User by ID

(Replace 1 with the actual ID from the create response)

```bash
curl -X GET "http://localhost:8000/api/v1/users/1"
```

### 4️⃣ Update a User

```bash
curl -X PUT "http://localhost:8000/api/v1/users/1" \
     -H "Content-Type: application/json" \
     -d '{"name": "Grace Hopper"}'
```

### 5️⃣ Delete a User

```bash
curl -X DELETE "http://localhost:8000/api/v1/users/1"
```
