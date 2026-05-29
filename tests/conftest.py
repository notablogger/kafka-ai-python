import os
import platform
import time
import urllib.request

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import create_async_engine
from testcontainers.core.container import DockerContainer
from testcontainers.core.network import Network
from testcontainers.kafka import KafkaContainer
from testcontainers.mongodb import MongoDbContainer
from testcontainers.postgres import PostgresContainer

from src.db.postgres import Base


KAFKA_IMAGE = "confluentinc/cp-kafka:7.6.1"
SCHEMA_REGISTRY_IMAGE = "confluentinc/cp-schema-registry:7.6.1"
IS_LINUX = platform.system() == "Linux"


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
    kafka = KafkaContainer(image=KAFKA_IMAGE)
    kafka.with_network(docker_network)
    kafka.with_network_aliases("kafka")
    with kafka:
        yield kafka


@pytest.fixture(scope="session")
def schema_registry_container(kafka_container, docker_network):
    """
    Start Schema Registry connected to Kafka.

    The fundamental challenge: Schema Registry runs inside Docker but needs to reach
    Kafka. Kafka's advertised listeners point to `localhost:PORT` on the host.
    From inside a normal Docker container, `localhost` is the container itself —
    not the host — so the connection fails.

    Fix (Linux / CI): run Schema Registry with network_mode=host so it shares the
    host's network namespace. It can then reach `localhost:PORT` directly, matching
    Kafka's advertised listeners. SR is accessible at localhost:8081.

    Fix (macOS / local): use an explicit `host.docker.internal` extra-host entry
    so containers can resolve the host. SR is accessed on a mapped port.
    """
    bootstrap = kafka_container.get_bootstrap_server()  # "localhost:PORT"

    container = DockerContainer(SCHEMA_REGISTRY_IMAGE)
    container.with_env("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")

    if IS_LINUX:
        # Host network: SR sees the same localhost as the host → can reach Kafka
        container.with_env("SCHEMA_REGISTRY_HOST_NAME", "localhost")
        container.with_env(
            "SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS",
            f"PLAINTEXT://{bootstrap}",
        )
        container.with_kwargs(network_mode="host")
    else:
        # macOS: Docker Desktop translates host.docker.internal → host IP
        kafka_port = bootstrap.split(":")[-1]
        container.with_env("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
        container.with_env(
            "SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS",
            f"PLAINTEXT://host.docker.internal:{kafka_port}",
        )
        container.with_network(docker_network)
        container.with_exposed_ports(8081)

    with container:
        if IS_LINUX:
            sr_url = "http://localhost:8081/subjects"
        else:
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
                f"Schema Registry did not become ready within 90s (bootstrap={bootstrap}). "
                "Check Kafka connectivity."
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

    if IS_LINUX:
        sr_url = "http://localhost:8081"
    else:
        sr_port = schema_registry_container.get_exposed_port(8081)
        sr_url = f"http://localhost:{sr_port}"

    os.environ["POSTGRES_URL"] = pg_url
    os.environ["MONGO_URL"] = mongodb_container.get_connection_url()
    os.environ["MONGO_DB"] = "test_kafka_events"
    os.environ["KAFKA_BOOTSTRAP_SERVERS"] = kafka_container.get_bootstrap_server()
    os.environ["KAFKA_SCHEMA_REGISTRY_URL"] = sr_url
    os.environ["KAFKA_TOPIC_EMPLOYEE"] = "employee_topic"
    os.environ["KAFKA_CONSUMER_GROUP"] = "test-group"

    return {
        "postgres_url": pg_url,
        "mongo_url": mongodb_container.get_connection_url(),
        "sr_url": sr_url,
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
    from src.main import app
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        yield ac
