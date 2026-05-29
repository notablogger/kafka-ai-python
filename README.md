# kafka-ai-python

> Part of the **kafka-ai** multi-language series — the same event-driven application rebuilt in Python to compare AI behaviour across language ecosystems.

---

## The Mission

This is the Python version of [kafka-ai-spring-boot](../kafka-ai-spring-boot). Same domain, same Avro schema, same architecture — different language, different ecosystem, different AI experience.

The goal is to answer: **how does AI behave differently when building in Python vs Java?** What tradeoffs does it make? Where does it struggle? Which language produces less boilerplate with AI assistance?

---

## Architecture

```
POST/PUT/DELETE → EmployeeRouter → EmployeeService → PostgreSQL (source of truth)
                                          │
                               EmployeeEventProducer
                                          │
                              employee_topic (Kafka + Avro)
                                          │
                              EmployeeEventConsumer (background thread)
                                          │
                                       MongoDB (read store)
                                          │
                    GET → latest event snapshot per employee, DELETED excluded
```

---

## Stack

| Concern | Library | Version |
|---|---|---|
| Framework | FastAPI | 0.136.3 |
| Kafka | confluent-kafka | 2.14.0 |
| Avro + Schema Registry | confluent-kafka[avro,schemaregistry] | 2.14.0 |
| PostgreSQL | SQLAlchemy async + asyncpg | 2.0.50 + 0.30.0 |
| MongoDB | PyMongo Async | 4.x |
| Config | pydantic-settings | 2.14.1 |
| Migrations | Alembic | 1.18.4 |
| Logging | structlog | 25.5.0 |
| Testing | pytest + testcontainers + httpx | 9.0.3 + 4.14.2 + 0.28.1 |
| Package manager | uv | latest |
| Python | 3.12 | — |

> **Note:** Motor (the async MongoDB driver) was deprecated on 5/14/2025. This project uses PyMongo Async (`pymongo` 4.x) which is its official replacement.

---

## API

| Method | Path | Source | Description |
|---|---|---|---|
| GET | `/api/departments` | Postgres | List all departments |
| POST | `/api/departments` | Postgres | Create department |
| GET | `/api/departments/{id}` | Postgres | Get department by ID |
| PUT | `/api/departments/{id}` | Postgres | Update department |
| DELETE | `/api/departments/{id}` | Postgres | Delete department |
| GET | `/api/employees` | MongoDB | Latest snapshot per employee |
| POST | `/api/employees` | Postgres + Kafka | Create employee, fire CREATED event |
| GET | `/api/employees/{id}` | MongoDB | Latest snapshot for employee |
| GET | `/api/employees/department/{id}` | MongoDB | Employees by department |
| PUT | `/api/employees/{id}` | Postgres + Kafka | Update employee, fire UPDATED event |
| DELETE | `/api/employees/{id}` | Postgres + Kafka | Delete employee, fire DELETED event |

Swagger UI: `http://localhost:8080/docs`

---

## Running Locally

```bash
# 1. Copy and fill in environment variables
cp .env.example .env

# 2. Start all infrastructure
docker-compose up -d postgres zookeeper kafka schema-registry mongodb

# 3. Install dependencies
uv sync

# 4. Run migrations
uv run alembic upgrade head

# 5. Start the app
uv run uvicorn src.main:app --host 0.0.0.0 --port 8080 --reload
```

---

## Running Tests

Requires Docker (Testcontainers spins up real containers).

```bash
uv run pytest tests/ -v
```

Tests cover:
- Department CRUD (201, 200, 204, 404)
- Employee CRUD (201, 200, 204, 404) + validation 400s (invalid email, negative salary)
- Full Kafka flow: CREATED event → MongoDB → GET; UPDATED event → GET; DELETED event → 404

---

## Project Structure

```
src/
├── main.py               FastAPI app + lifespan (Kafka consumer thread start/stop)
├── core/
│   ├── config.py        Pydantic Settings — all config from env vars
│   └── logging.py       structlog JSON setup
├── db/
│   ├── postgres.py      SQLAlchemy async engine + session factory
│   └── mongo.py         PyMongo AsyncClient
├── kafka/
│   ├── avro_schema.py   Dynamic schema loading + serializer/deserializer setup
│   ├── producer.py      EmployeeEventProducer
│   └── consumer.py      EmployeeEventConsumer (blocking poll in background thread)
├── employee/
│   ├── router.py        FastAPI router — CRUD endpoints
│   ├── schemas.py       Pydantic request/response models
│   ├── models.py        SQLAlchemy ORM model
│   ├── service.py       Business logic
│   └── repository.py   Postgres + MongoDB data access
└── department/
    ├── router.py
    ├── schemas.py
    ├── models.py
    ├── service.py
    └── repository.py
```

---

## Key Python vs Java Differences

| Concern | Java | Python |
|---|---|---|
| Avro | Compile-time code generation → typed classes | Dynamic schema loading → plain dicts |
| Kafka consumer | `@KafkaListener` Spring annotation, managed thread | `threading.Thread` in FastAPI lifespan |
| ORM | Spring Data JPA (interface-only) | SQLAlchemy 2.x (explicit queries) |
| MongoDB | Spring Data MongoDB | PyMongo Async (`AsyncMongoClient`) |
| Validation | Bean Validation (`@NotBlank`, `@Email`) | Pydantic v2 field validators |
| Config | `application.yml` + `@Value` | pydantic-settings + `.env` |
| Migrations | `ddl-auto: update` (Hibernate) | Alembic (explicit migration files) |
| Dependency injection | Spring IoC container | FastAPI `Depends()` |

📁 Full AI story in [`AI docs/`](./AI%20docs/)
