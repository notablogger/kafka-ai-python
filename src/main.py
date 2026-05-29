import threading
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from src.core.logging import configure_logging, get_logger
from src.db.mongo import close_client
from src.department.router import router as department_router
from src.employee.router import router as employee_router
from src.kafka.consumer import EmployeeEventConsumer
from src.kafka.producer import EmployeeEventProducer
from src.shared.exceptions import ResourceNotFoundException

configure_logging()
logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("app_starting")

    producer = EmployeeEventProducer()
    app.state.producer = producer

    consumer = EmployeeEventConsumer()
    stop_event = threading.Event()
    consumer_thread = threading.Thread(target=consumer.run, args=(stop_event,), daemon=True)
    consumer_thread.start()

    logger.info("app_started")
    yield

    logger.info("app_stopping")
    stop_event.set()
    consumer_thread.join(timeout=10)
    producer.flush()
    await close_client()
    logger.info("app_stopped")


app = FastAPI(
    title="Python Kafka AI — Training API",
    description="Event-driven application with Kafka, Avro, PostgreSQL, and MongoDB.",
    version="0.1.0",
    lifespan=lifespan,
)

app.include_router(department_router)
app.include_router(employee_router)


@app.exception_handler(ResourceNotFoundException)
async def resource_not_found_handler(request: Request, exc: ResourceNotFoundException):
    return JSONResponse(status_code=404, content={"detail": exc.message})


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.error("unhandled_exception", error=str(exc))
    return JSONResponse(status_code=500, content={"detail": "Internal server error"})
