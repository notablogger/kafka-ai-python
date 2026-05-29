import os
import time
import urllib.request

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from testcontainers.core.container import DockerContainer
from testcontainers.core.network import Network
from testcontainers.kafka import KafkaContainer
from testcontainers.mongodb import MongoDbContainer
from testcontainers.postgres import PostgresContainer

from src.db.postgres import Base


KAFKA_IMAGE = "confluentinc/cp-kafka:7.6.1"
SCHEMA_REGISTRY_IMAGE = "confluentinc/cp-schema-registry:7.6.1"


@pytest.fixture(scope="session")
def docker_network():
    with Network() as network:
        yield network


@pytest.fixture(scope="session")
def postgres_container():
    with PostgresContainer("postgres:16") as pg:
        yield pg


@pytest.fixture(scope="session")
def kafka_container(docker_network):
    # CRITICAL: configure network and alias BEFORE the context manager starts the container.
    # Calling .with_network() inside "with KafkaContainer() as k:" is too late —
    # the container has already started without the network attached.
    kafka = KafkaContainer(image=KAFKA_IMAGE)
    kafka.with_network(docker_network)
    kafka.with_network_aliases("kafka")
    with kafka:
        yield kafka


@pytest.fixture(scope="session")
def schema_registry_container(kafka_container, docker_network):
    # KafkaContainer exposes the BROKER listener on port 9093 (PLAINTEXT protocol).
    # Schema Registry connects to this via the shared Docker network using the alias.
    container = DockerContainer(SCHEMA_REGISTRY_IMAGE)
    container.with_network(docker_network)
    container.with_env("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
    container.with_env(
        "SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS",
        "PLAINTEXT://kafka:9093",  # BROKER listener port on the shared network
    )
    container.with_env("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
    container.with_exposed_ports(8081)

    with container:
        # Poll until Schema Registry HTTP endpoint responds (up to 90s).
        # Must confirm readiness here — env_setup calls get_exposed_port() on this
        # container and will fail if SR has crashed or hasn't finished starting.
        sr_port = container.get_exposed_port(8081)
        sr_url = f"http://localhost:{sr_port}/subjects"
        ready = False
        for _ in range(90):
            try:
                urllib.request.urlopen(sr_url, timeout=2)
                ready = True
                break
            except Exception:
                time.sleep(1)

        if not ready:
            raise RuntimeError(
                "Schema Registry did not become ready within 90s. "
                "Ensure Kafka is reachable at kafka:9093 on the shared Docker network."
            )

        yield container


@pytest.fixture(scope="session")
def mongodb_container():
    with MongoDbContainer("mongo:7") as mongo:
        yield mongo


@pytest.fixture(scope="session")
def env_setup(postgres_container, kafka_container, schema_registry_container, mongodb_container):
    pg_url = postgres_container.get_connection_url().replace(
        "postgresql+psycopg2", "postgresql+asyncpg"
    )
    sr_port = schema_registry_container.get_exposed_port(8081)

    os.environ["POSTGRES_URL"] = pg_url
    os.environ["MONGO_URL"] = mongodb_container.get_connection_url()
    os.environ["MONGO_DB"] = "test_kafka_events"
    os.environ["KAFKA_BOOTSTRAP_SERVERS"] = kafka_container.get_bootstrap_server()
    os.environ["KAFKA_SCHEMA_REGISTRY_URL"] = f"http://localhost:{sr_port}"
    os.environ["KAFKA_TOPIC_EMPLOYEE"] = "employee_topic"
    os.environ["KAFKA_CONSUMER_GROUP"] = "test-group"

    return {
        "postgres_url": pg_url,
        "mongo_url": mongodb_container.get_connection_url(),
        "sr_url": f"http://localhost:{sr_port}",
        "kafka_brokers": kafka_container.get_bootstrap_server(),
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
    # Import app AFTER env vars are set — pydantic-settings reads them at import time.
    from src.main import app
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        yield ac
