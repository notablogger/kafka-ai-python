import json
from pathlib import Path

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

from src.core.config import settings

_SCHEMA_PATH = Path(__file__).parent.parent.parent / "avro" / "message.avsc"


def _load_schema_str() -> str:
    return _SCHEMA_PATH.read_text()


def get_schema_registry_client() -> SchemaRegistryClient:
    return SchemaRegistryClient({"url": settings.kafka_schema_registry_url})


def get_serializer(client: SchemaRegistryClient) -> AvroSerializer:
    return AvroSerializer(client, _load_schema_str())


def get_deserializer(client: SchemaRegistryClient) -> AvroDeserializer:
    return AvroDeserializer(client, _load_schema_str())


def make_serialization_context(topic: str) -> SerializationContext:
    return SerializationContext(topic, MessageField.VALUE)
