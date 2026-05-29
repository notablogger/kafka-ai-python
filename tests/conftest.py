import os
import time

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from testcontainers.kafka import KafkaContainer
from testcontainers.mongodb import MongoDbContainer
from testcontainers.postgres import PostgresContainer
from testcontainers.core.container import DockerContainer
from testcontainers.core.network import Network

from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

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
    with KafkaContainer(image=KAFKA_IMAGE) as kafka:
        kafka.with_network(docker_network)
        kafka.with_network_aliases("kafka")
        yield kafka


@pytest.fixture(scope="session")
def schema_registry_container(kafka_container, docker_network):
    kafka_internal = f"PLAINTEXT://kafka:9092"
    container = (
        DockerContainer(SCHEMA_REGISTRY_IMAGE)
        .with_network(docker_network)
        .with_env("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
        .with_env("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", kafka_internal)
        .with_env("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
        .with_exposed_ports(8081)
    )
    with container:
        # Wait for Schema Registry to be ready
        import urllib.request
        sr_url = f"http://localhost:{container.get_exposed_port(8081)}/subjects"
        for _ in range(30):
            try:
                urllib.request.urlopen(sr_url, timeout=2)
                break
            except Exception:
                time.sleep(1)
        yield container


@pytest.fixture(scope="session")
def mongodb_container():
    with MongoDbContainer("mongo:7") as mongo:
        yield mongo


@pytest.fixture(scope="session")
def env_setup(postgres_container, kafka_container, schema_registry_container, mongodb_container):
    pg_url = postgres_container.get_connection_url().replace("postgresql+psycopg2", "postgresql+asyncpg")
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
    # Re-import app AFTER env vars are set so config picks them up
    from src.main import app
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        yield ac
