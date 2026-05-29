# Getting Started — Python Project Setup

How to start a new Python backend project. The Spring equivalent is going to [start.spring.io](https://start.spring.io) — Python doesn't have one website, but the equivalent decisions and commands are just as fast once you know them.

---

## The Spring Initializr Equivalent

| Spring Initializr | Python equivalent |
|---|---|
| Go to start.spring.io | `uv init <project-name>` in terminal |
| Choose Group / Artifact | Set `name`, `version` in `pyproject.toml` |
| Choose Java version | Set `requires-python = ">=3.12"` |
| Add dependencies (checkboxes) | `uv add fastapi sqlalchemy ...` |
| Click Generate → download ZIP | Project folder is ready immediately |
| Open in IntelliJ | Open in VS Code / PyCharm |
| Run with `./gradlew bootRun` | Run with `uv run uvicorn src.main:app --reload` |

---

## Step 1 — Prerequisites

Install once, use everywhere:

```bash
# Package manager (replaces pip/poetry — ~100x faster)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Verify
uv --version
```

You don't install Python separately — uv manages Python versions for you.

---

## Step 2 — Create the Project

```bash
uv init kafka-ai-python
cd kafka-ai-python
```

This creates:

```
kafka-ai-python/
├── pyproject.toml    ← equivalent of build.gradle / pom.xml
├── .python-version   ← pins Python version (like toolchain in Gradle)
├── hello.py          ← delete this, you'll create your own structure
└── .venv/            ← virtual environment (equivalent of Gradle cache)
```

---

## Step 3 — Choose Your Stack

Unlike Spring (where starters bundle everything), Python uses separate focused libraries. Make these decisions upfront:

| Concern | Recommended library | Command |
|---|---|---|
| REST framework | FastAPI | `uv add "fastapi[standard]"` |
| ASGI server | uvicorn (bundled with fastapi[standard]) | included above |
| PostgreSQL (async) | SQLAlchemy + asyncpg | `uv add "sqlalchemy[asyncio]" asyncpg` |
| MongoDB (async) | PyMongo Async | `uv add pymongo` |
| Kafka | confluent-kafka | `uv add "confluent-kafka[avro,schemaregistry]"` |
| Validation | Pydantic v2 (bundled with FastAPI) | included with fastapi |
| Config / env vars | pydantic-settings | `uv add pydantic-settings` |
| DB migrations | Alembic | `uv add alembic` |
| Structured logging | structlog | `uv add structlog` |

**Add everything in one go:**

```bash
uv add "fastapi[standard]" "sqlalchemy[asyncio]" asyncpg pymongo \
       "confluent-kafka[avro,schemaregistry]" pydantic-settings \
       alembic structlog python-dotenv
```

**Add dev/test dependencies:**

```bash
uv add --group dev pytest pytest-asyncio httpx \
       "testcontainers[postgres,kafka,mongodb]"
```

This updates `pyproject.toml` and generates `uv.lock` (pin file — commit both).

> **Note:** Motor (older async MongoDB driver) was deprecated **14 May 2025**. Always use `pymongo` 4.x with `AsyncMongoClient` for new projects.

---

## Step 4 — Project Structure

Delete `hello.py` and create this layout (domain-driven, not layer-driven):

```bash
mkdir -p src/core src/db src/kafka src/employee src/department src/shared
mkdir -p avro alembic/versions tests docs "AI docs"
touch src/__init__.py src/core/__init__.py src/db/__init__.py \
      src/kafka/__init__.py src/employee/__init__.py \
      src/department/__init__.py src/shared/__init__.py \
      tests/__init__.py
```

Result:

```
src/
├── main.py              ← FastAPI app + lifespan (= @SpringBootApplication)
├── core/
│   ├── config.py        ← pydantic-settings (= application.yml + @Value)
│   └── logging.py       ← structlog setup
├── db/
│   ├── postgres.py      ← SQLAlchemy async engine + session
│   └── mongo.py         ← PyMongo AsyncClient
├── kafka/
│   ├── avro_schema.py   ← load .avsc at runtime
│   ├── producer.py      ← confluent-kafka Producer
│   └── consumer.py      ← poll loop in background thread
├── employee/            ← one folder per domain entity
│   ├── router.py        ← @router.get/post/put/delete (= @RestController)
│   ├── schemas.py       ← Pydantic request/response (= DTO + Bean Validation)
│   ├── models.py        ← SQLAlchemy ORM model (= @Entity)
│   ├── service.py       ← business logic (= @Service)
│   └── repository.py   ← DB queries (= JpaRepository + MongoRepository)
└── department/          ← same structure
    └── ...
```

---

## Step 5 — Minimum Working App

`src/main.py`:

```python
from fastapi import FastAPI

app = FastAPI(title="My API")

@app.get("/health")
async def health():
    return {"status": "ok"}
```

Run it:

```bash
uv run uvicorn src.main:app --reload --port 8080
```

Open: `http://localhost:8080/docs` — Swagger UI is automatic, no extra config.

---

## Step 6 — Config (the .env approach)

Create `.env` (never commit this — add to `.gitignore`):

```bash
POSTGRES_URL=postgresql+asyncpg://user:pass@localhost:5432/mydb
MONGO_URL=mongodb://localhost:27017
MONGO_DB=my_events
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_SCHEMA_REGISTRY_URL=http://localhost:8081
KAFKA_TOPIC_EMPLOYEE=employee_topic
LOG_LEVEL=INFO
```

Create `.env.example` (commit this — shows what vars are needed without values).

`src/core/config.py`:

```python
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", case_sensitive=False)

    postgres_url: str
    mongo_url: str
    mongo_db: str
    kafka_bootstrap_servers: str
    kafka_schema_registry_url: str
    kafka_topic_employee: str = "employee_topic"
    log_level: str = "INFO"

settings = Settings()  # auto-reads .env
```

---

## Step 7 — Database Setup (Alembic)

Alembic is Flyway/Liquibase equivalent — explicit migration files instead of `ddl-auto: update`.

```bash
# Initialise Alembic (one-time)
uv run alembic init alembic

# After creating your SQLAlchemy models, generate the first migration
uv run alembic revision --autogenerate -m "initial schema"

# Apply migrations
uv run alembic upgrade head
```

Edit `alembic/env.py` to import your `Base` and `Settings` — see `alembic/env.py` in this project for the async pattern.

---

## Step 8 — Infrastructure (Docker Compose)

Start all services before running the app locally:

```bash
docker-compose up -d postgres zookeeper kafka schema-registry mongodb
```

Then run the app:

```bash
uv run uvicorn src.main:app --reload --port 8080
```

Run tests (spins up real containers automatically via Testcontainers):

```bash
uv run pytest tests/ -v
```

---

## Decisions Cheat Sheet

Decisions to make at project start — equivalent to the Spring Initializr checkboxes:

| Decision | This project chose | Alternatives |
|---|---|---|
| Framework | FastAPI | Flask (simpler, no async), Django (batteries-included) |
| Async DB | SQLAlchemy 2.x async + asyncpg | psycopg3 async (lighter, less ORM) |
| MongoDB | PyMongo Async | — (Motor is deprecated) |
| Kafka | confluent-kafka | aiokafka (async-native, less production-proven) |
| Config | pydantic-settings | python-decouple, dynaconf |
| Migrations | Alembic | django-migrations (if using Django) |
| Logging | structlog | loguru, Python stdlib logging |
| Package manager | uv | poetry (slower), pip (no lock file by default) |
| Test HTTP client | httpx (async) | requests (sync only) |
| Containers in tests | testcontainers-python | spin up docker-compose manually |

---

## Common Commands

```bash
uv add <package>                      # add a dependency (= implementation '...' in Gradle)
uv add --group dev <package>          # add a dev dependency (= testImplementation)
uv sync                               # install all deps from lockfile (= gradle build deps)
uv sync --all-groups                  # install including dev deps
uv run uvicorn src.main:app --reload  # run app (= ./gradlew bootRun)
uv run pytest tests/ -v               # run tests (= ./gradlew test)
uv run alembic upgrade head           # apply DB migrations
uv run alembic revision --autogenerate -m "description"  # generate migration
uv lock                               # regenerate uv.lock (= Gradle dependency lock)
```

---

## What to Read Next

| Topic | Doc |
|---|---|
| How Python differs from Java in this project | [`java-vs-python.md`](java-vs-python.md) |
| Kafka config deep-dive | [`kafka-config-reference.md`](kafka-config-reference.md) |
| PostgreSQL + Alembic config | [`postgres-config-reference.md`](postgres-config-reference.md) |
| MongoDB + PyMongo setup | [`mongo-config-reference.md`](mongo-config-reference.md) |
| Avro schema explained | [`avro-schema-reference.md`](avro-schema-reference.md) |
| Full dependency reference | [`build-reference.md`](build-reference.md) |
