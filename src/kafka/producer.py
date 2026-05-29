import datetime
from decimal import Decimal

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

from src.core.config import settings
from src.core.logging import get_logger
from src.kafka.avro_schema import get_schema_registry_client, get_serializer, make_serialization_context

logger = get_logger(__name__)


class EmployeeEventProducer:
    def __init__(self):
        self._sr_client: SchemaRegistryClient = get_schema_registry_client()
        self._serializer: AvroSerializer = get_serializer(self._sr_client)
        self._producer = Producer({"bootstrap.servers": settings.kafka_bootstrap_servers})
        self._topic = settings.kafka_topic_employee

    def send_event(self, employee, department, event_type: str) -> None:
        payload = {
            "id": employee.id,
            "firstName": employee.first_name,
            "lastName": employee.last_name,
            "email": employee.email,
            "salary": employee.salary,
            "hireDate": employee.hire_date,
            "department": {
                "id": department.id,
                "name": department.name,
                "location": department.location,
            },
            "eventType": event_type,
            "eventTimestamp": int(datetime.datetime.now(datetime.UTC).timestamp() * 1000),
        }

        ctx = make_serialization_context(self._topic)

        self._producer.produce(
            topic=self._topic,
            key=str(employee.id),
            value=self._serializer(payload, ctx),
            headers={"eventType": event_type},
            on_delivery=self._on_delivery,
        )
        self._producer.poll(0)

    def flush(self) -> None:
        self._producer.flush()

    def _on_delivery(self, err, msg) -> None:
        if err:
            logger.error("kafka_produce_failed", error=str(err))
        else:
            logger.info("kafka_produce_success", topic=msg.topic(), partition=msg.partition(), offset=msg.offset())
