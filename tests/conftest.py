"""
Test configuration.

Container startup order is critical: all containers must be started and env vars
set BEFORE any src modules are imported. pydantic-settings (Settings) reads env
vars once at instantiation — if it runs before containers are ready, every
connection will point at the default localhost URLs and fail.

The module-level block starts all containers and writes env vars so that the
subsequent src imports pick up the correct container URLs.

Schema Registry / Kafka networking explained:
  KafkaContainer's tc_start() writes a startup script that sets:
    KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:PORT,BROKER://$(hostname -i):9092
  $(hostname -i) evaluates inside the container to Kafka's actual Docker network
  IP (e.g. 172.17.0.2). After SR bootstraps, Kafka advertises PLAINTEXT://localhost:PORT
  (unreachable from inside Docker) BUT also BROKER://172.17.0.2:9092 (the real
  container IP, reachable from any container on the bridge network).
  Fix: bootstrap SR to PLAINTEXT://172.17.0.2:9092 so Kafka's advertised address
  matches the reachable IP. Works on both macOS and Linux without host networking.
"""

import atexit
import os
import time
import urllib.request

import docker as _docker_sdk
from testcontainers.core.container import DockerContainer
from testcontainers.core.network import Network
from testcontainers.kafka import KafkaContainer
from testcontainers.mongodb import MongoDbContainer
from testcontainers.postgres import PostgresContainer

KAFKA_IMAGE = "confluentinc/cp-kafka:7.6.1"
SCHEMA_REGISTRY_IMAGE = "confluentinc/cp-schema-registry:7.6.1"

# ── Start containers ──────────────────────────────────────────────────────────

_network = Network()
_network.__enter__()

_postgres = PostgresContainer("postgres:16")
_postgres.__enter__()

_kafka = KafkaContainer(image=KAFKA_IMAGE)
_kafka.with_network(_network)
_kafka.with_network_aliases("kafka")
_kafka.__enter__()

# Get Kafka's actual Docker network IP so SR can stay connected after metadata.
# Kafka's tc_start() startup script advertises BROKER://$(hostname -i):9092.
# $(hostname -i) resolves to this IP inside the container — reachable from all
# containers on the same bridge network.
_docker_api = _docker_sdk.from_env()
_kafka_obj = _docker_api.containers.get(_kafka.get_wrapped_container().id)
_kafka_obj.reload()
_kafka_docker_ip = next(
    (
        data["IPAddress"]
        for data in _kafka_obj.attrs["NetworkSettings"]["Networks"].values()
        if data.get("IPAddress")
    ),
    None,
)
if not _kafka_docker_ip:
    raise RuntimeError("Could not determine Kafka container Docker network IP")

_sr = DockerContainer(SCHEMA_REGISTRY_IMAGE)
_sr.with_network(_network)
_sr.with_env("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
_sr.with_env(
    "SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS",
    f"PLAINTEXT://{_kafka_docker_ip}:9092",  # Kafka BROKER listener at its Docker IP
)
_sr.with_env("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
_sr.with_exposed_ports(8081)
_sr.__enter__()

_sr_port = _sr.get_exposed_port(8081)
_sr_url = f"http://localhost:{_sr_port}/subjects"
for _ in range(90):
    try:
        urllib.request.urlopen(_sr_url, timeout=2)
        break
    except Exception:
        time.sleep(1)
else:
    raise RuntimeError(
        f"Schema Registry did not become ready within 90s "
        f"(kafka_ip={_kafka_docker_ip}, sr_port={_sr_port})"
    )

_mongo = MongoDbContainer("mongo:7")
_mongo.__enter__()


# ── Set env vars with real container URLs BEFORE importing src ────────────────

_pg_url = _postgres.get_connection_url().replace("postgresql+psycopg2", "postgresql+asyncpg")
_sr_env_url = f"http://localhost:{_sr_port}"

os.environ["POSTGRES_URL"] = _pg_url
os.environ["MONGO_URL"] = _mongo.get_connection_url()
os.environ["MONGO_DB"] = "test_kafka_events"
os.environ["KAFKA_BOOTSTRAP_SERVERS"] = _kafka.get_bootstrap_server()
os.environ["KAFKA_SCHEMA_REGISTRY_URL"] = _sr_env_url
os.environ["KAFKA_TOPIC_EMPLOYEE"] = "employee_topic"
os.environ["KAFKA_CONSUMER_GROUP"] = "test-group"


# ── Cleanup on process exit ───────────────────────────────────────────────────

def _cleanup():
    for c in (_mongo, _sr, _kafka, _postgres, _network):
        try:
            c.__exit__(None, None, None)
        except Exception:
            pass


atexit.register(_cleanup)


# ── Import src (Settings() reads env vars above; we also patch singletons as a
#    safety net in case pydantic-settings resolved the class before env vars
#    were visible — e.g. due to pytest plugin import ordering on some runners) ──

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from src.db.postgres import Base

# Guarantee the app's module-level singletons use container URLs regardless of
# when Settings() was instantiated (the env-var path covers the normal case;
# this direct patch covers the edge-case where the class initialised first).
import src.core.config as _cfg
import src.db.postgres as _pg_mod

_cfg.settings.postgres_url = _pg_url
_cfg.settings.mongo_url = _mongo.get_connection_url()
_cfg.settings.mongo_db = "test_kafka_events"
_cfg.settings.kafka_bootstrap_servers = _kafka.get_bootstrap_server()
_cfg.settings.kafka_schema_registry_url = _sr_env_url
_cfg.settings.kafka_topic_employee = "employee_topic"
_cfg.settings.kafka_consumer_group = "test-group"

# Recreate the engine so the pool targets the container, not the default URL.
_pg_mod.engine = create_async_engine(_pg_url, echo=False)
_pg_mod.AsyncSessionLocal = async_sessionmaker(
    bind=_pg_mod.engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


# ── Fixtures (containers are already running — just expose them) ──────────────

@pytest.fixture(scope="session")
def env_setup():
    return {
        "postgres_url": _pg_url,
        "mongo_url": _mongo.get_connection_url(),
        "sr_url": _sr_env_url,
        "kafka_brokers": _kafka.get_bootstrap_server(),
    }


@pytest_asyncio.fixture(scope="session")
async def setup_db(env_setup):
    engine = create_async_engine(env_setup["postgres_url"])
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    await engine.dispose()


@pytest_asyncio.fixture(scope="session")
async def client(env_setup, setup_db):
    from src.main import app
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        yield ac
