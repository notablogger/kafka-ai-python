import asyncio
import datetime
import threading
from decimal import Decimal

from confluent_kafka import Consumer, KafkaError
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

from src.core.config import settings
from src.core.logging import get_logger
from src.db.mongo import get_db
from src.kafka.avro_schema import get_deserializer, get_schema_registry_client, make_serialization_context

logger = get_logger(__name__)


class EmployeeEventConsumer:
    def __init__(self):
        self._sr_client = get_schema_registry_client()
        self._deserializer: AvroDeserializer = get_deserializer(self._sr_client)
        self._consumer = Consumer(
            {
                "bootstrap.servers": settings.kafka_bootstrap_servers,
                "group.id": settings.kafka_consumer_group,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )
        self._topic = settings.kafka_topic_employee

    def run(self, stop_event: threading.Event) -> None:
        self._consumer.subscribe([self._topic])
        logger.info("kafka_consumer_started", topic=self._topic)

        try:
            while not stop_event.is_set():
                msg = self._consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        logger.error("kafka_consumer_error", error=str(msg.error()))
                    continue

                self._handle(msg)
                self._consumer.commit(msg)
        finally:
            self._consumer.close()
            logger.info("kafka_consumer_stopped")

    def _handle(self, msg) -> None:
        try:
            ctx = make_serialization_context(msg.topic())
            event = self._deserializer(msg.value(), ctx)
            event_type = None
            for k, v in (msg.headers() or []):
                if k == "eventType":
                    event_type = v.decode() if isinstance(v, bytes) else v

            document = {
                "employee_id": event["id"],
                "first_name": event["firstName"],
                "last_name": event["lastName"],
                "email": event["email"],
                "salary": str(event["salary"]),
                "hire_date": datetime.date.fromordinal(
                    datetime.date(1970, 1, 1).toordinal() + event["hireDate"]
                ),
                "department_id": event["department"]["id"],
                "department_name": event["department"]["name"],
                "department_location": event["department"]["location"],
                "event_type": event_type or event.get("eventType"),
                "event_timestamp": datetime.datetime.fromtimestamp(
                    event["eventTimestamp"] / 1000, tz=datetime.UTC
                ),
                "received_at": datetime.datetime.now(datetime.UTC),
            }

            asyncio.run(self._save(document))
            logger.info("kafka_event_consumed", employee_id=document["employee_id"], event_type=document["event_type"])
        except Exception as e:
            logger.error("kafka_event_handling_failed", error=str(e))

    async def _save(self, document: dict) -> None:
        collection = get_db()["employee_events"]
        await collection.insert_one(document)
