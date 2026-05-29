from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", case_sensitive=False)

    # PostgreSQL
    postgres_url: str = "postgresql+asyncpg://traininguser:trainingpassword@localhost:5432/kafka_training_db"

    # MongoDB
    mongo_url: str = "mongodb://localhost:27017"
    mongo_db: str = "kafka_training_events"

    # Kafka
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_schema_registry_url: str = "http://localhost:8081"
    kafka_topic_employee: str = "employee_topic"
    kafka_consumer_group: str = "kafka-python-group"

    # App
    app_env: str = "development"
    log_level: str = "INFO"


settings = Settings()
