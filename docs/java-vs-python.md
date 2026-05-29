# Java vs Python — Side-by-Side Reference

A practical comparison grounded in this project. Every example is taken from code that actually exists in `kafka-ai-spring-boot` and `kafka-ai-python`. No theory for its own sake — only what you need to design, read, and review Python backends confidently.

---

## The One Mental Model That Changes Everything

Before the code: the philosophical difference between the two stacks.

| | Spring Boot | FastAPI |
|---|---|---|
| **Philosophy** | Framework does everything — you configure it | You compose libraries — you own it |
| **DI** | Spring wires beans at startup, invisibly | You pass dependencies as function arguments |
| **Transactions** | `@Transactional` declares a boundary | You open and close the session explicitly |
| **Routing** | `@GetMapping` tags the method; Spring reads the tag | `@router.get()` wraps the function; it *is* the route |
| **When something breaks** | Dig through framework magic | The bug is in your code, visible in the call stack |

**The aha moment:** FastAPI doesn't assume you want anything. Spring assumes you want everything. Python code is more verbose in some places, but you can always *see what it does*.

---

## 1. Project Structure

| Java (`kafka-ai-spring-boot`) | Python (`kafka-ai-python`) | Notes |
|---|---|---|
| `src/main/java/com/training/kafka/` | `src/` | Flat namespace, no nested package convention |
| `controller/` | `employee/router.py`, `department/router.py` | Domain-first, not layer-first |
| `service/` | `employee/service.py` | Same concept, same name |
| `repository/` | `employee/repository.py` | Same concept |
| `entity/` | `employee/models.py` | SQLAlchemy models |
| `dto/` | `employee/schemas.py` | Pydantic models |
| `kafka/` | `kafka/` | Producer + consumer |
| `config/` | `src/core/config.py` + `src/db/` | Explicit wiring instead of `@Configuration` |
| `exception/` | `src/shared/exceptions.py` | Single file — less ceremony needed |
| `build.gradle` | `pyproject.toml` | |
| `application.yml` | `.env` + `src/core/config.py` | |
| `src/main/avro/message.avsc` | `avro/message.avsc` | Identical file — Avro schema is language-agnostic |

**Key difference:** Java is organised by layer (controller, service, repository). Python here is organised by domain (employee, department). Both work — Python's domain structure keeps everything about one entity together.

---

## 2. Web Framework — FastAPI vs Spring Boot

### Routing

| Java | Python |
|---|---|
| `@RestController` + `@RequestMapping` + `@GetMapping` | `APIRouter` + `@router.get()` |

```
Java                                     │  Python
─────────────────────────────────────────┼─────────────────────────────────────────
@RestController                          │  router = APIRouter(
@RequestMapping("/api/employees")        │      prefix="/api/employees",
public class EmployeeController {        │      tags=["Employees"]
                                         │  )
    @GetMapping("/{id}")                 │
    public ResponseEntity<EmployeeResp>  │  @router.get("/{id}")
    getById(@PathVariable Long id) {     │  async def get_by_id(id: int):
        ...                              │      ...
    }                                    │
}                                        │
```

**What to remember:** In Java, `@GetMapping` is metadata — Spring reads it at startup to build a routing table. In Python, `@router.get()` is a decorator that *wraps your function* at import time. The decorator is the route registration happening live.

---

### Dependency Injection

| Java | Python |
|---|---|
| Constructor injection via `@Autowired` or Lombok `@RequiredArgsConstructor` | Function argument with `Depends()` |

```
Java                                     │  Python
─────────────────────────────────────────┼─────────────────────────────────────────
@Service                                 │  # No class needed — just a function
@RequiredArgsConstructor                 │
public class EmployeeService {           │  def get_service(
    private final EmployeeRepository     │      session: AsyncSession = Depends(get_session)
        repo;                            │  ) -> EmployeeService:
    private final KafkaTemplate          │      return EmployeeService(session)
        kafka;                           │
}                                        │  @router.get("")
                                         │  async def get_all(
@GetMapping("")                          │      service = Depends(get_service)
public List<EmployeeResponse> getAll() { │  ):
    return service.getAll();             │      return await service.get_all()
}                                        │
```

**What to remember:** Spring's DI is application-scoped (singleton beans wired at startup). FastAPI's `Depends()` runs **per request** — every incoming HTTP request creates fresh dependencies. This is why mocking in tests is so clean: you swap the `Depends()` function, not the bean.

---

### App Entry Point & Lifecycle

| Java | Python |
|---|---|
| `@SpringBootApplication` + `main()` | `FastAPI(lifespan=...)` + `@asynccontextmanager` |

```
Java                                     │  Python
─────────────────────────────────────────┼─────────────────────────────────────────
@SpringBootApplication                   │  @asynccontextmanager
public class KafkaTrainingApplication {  │  async def lifespan(app: FastAPI):
    public static void main(             │      producer = EmployeeEventProducer()
        String[] args) {                 │      app.state.producer = producer
        SpringApplication.run(...);      │      consumer_thread.start()   # startup
    }                                    │      yield                      # app runs
}                                        │      stop_event.set()           # shutdown
                                         │
                                         │  app = FastAPI(lifespan=lifespan)
```

**What to remember:** Spring manages startup/shutdown lifecycle through `ApplicationContext`. FastAPI uses an `asynccontextmanager` — everything before `yield` is startup, everything after is shutdown. You own the lifecycle explicitly.

---

## 3. Validation — Pydantic vs Bean Validation

| Java | Python |
|---|---|
| Annotations on DTO fields + `@Valid` on controller parameter | Pydantic `BaseModel` with field validators |

```
Java                                     │  Python
─────────────────────────────────────────┼─────────────────────────────────────────
public class EmployeeRequest {           │  class EmployeeRequest(BaseModel):
    @NotBlank                            │      first_name: str
    private String firstName;            │      last_name: str
                                         │      email: EmailStr
    @Email                               │      salary: Decimal
    private String email;                │      hire_date: date
                                         │      department_id: int
    @NotNull                             │
    @DecimalMin("0.01")                  │      @field_validator("first_name", "last_name")
    private BigDecimal salary;           │      @classmethod
}                                        │      def not_blank(cls, v: str) -> str:
                                         │          if not v.strip():
// Controller:                           │              raise ValueError("must not be blank")
@PostMapping                             │          return v
public ResponseEntity create(            │
    @Valid @RequestBody EmployeeRequest  │      @field_validator("salary")
    request) { ... }                     │      @classmethod
                                         │      def salary_positive(cls, v: Decimal) -> Decimal:
                                         │          if v <= 0:
                                         │              raise ValueError("must be > 0")
                                         │          return v
```

**What to remember:** Java validation happens when the framework reads the annotations (request time). Pydantic validates **at construction** — the moment you write `EmployeeRequest(**data)`, it either returns a valid object or raises `ValidationError`. There is no separate "validate" step. A Pydantic instance is guaranteed to be valid.

Also: `EmailStr` in Pydantic does email format validation automatically — no `@Email` annotation needed.

---

## 4. Configuration — pydantic-settings vs application.yml

| Java | Python |
|---|---|
| `application.yml` + `@Value` / `@ConfigurationProperties` | `.env` file + `pydantic-settings` `BaseSettings` |

```
Java (application.yml)                  │  Python (.env)
─────────────────────────────────────────┼─────────────────────────────────────────
spring:                                  │  POSTGRES_URL=postgresql+asyncpg://...
  datasource:                            │  MONGO_URL=mongodb://localhost:27017
    url: jdbc:postgresql://...           │  KAFKA_BOOTSTRAP_SERVERS=localhost:9092
  kafka:                                 │  KAFKA_TOPIC_EMPLOYEE=employee_topic
    bootstrap-servers: kafka:29092       │
    topic:                               │
      employee: employee_topic           │

Java (usage)                             │  Python (config.py)
─────────────────────────────────────────┼─────────────────────────────────────────
@Value("${spring.kafka.topic.employee}") │  class Settings(BaseSettings):
private String topic;                    │      kafka_topic_employee: str = "employee_topic"
                                         │      postgres_url: str
                                         │      model_config = SettingsConfigDict(
                                         │          env_file=".env")
                                         │
                                         │  settings = Settings()  # reads .env automatically
```

**What to remember:** Spring config is hierarchical YAML; Python config is flat env vars. `pydantic-settings` reads from `.env` files, actual environment variables, or both — automatically, in priority order. In production you just set real env vars; `.env` is for local development only.

---

## 5. Database / ORM — SQLAlchemy vs JPA + Hibernate

### Models

| Java | Python |
|---|---|
| `@Entity` JPA class with Lombok | `Base` subclass with `Mapped` typed columns |

```
Java                                     │  Python
─────────────────────────────────────────┼─────────────────────────────────────────
@Entity                                  │  class Employee(Base):
@Table(name = "employees")               │      __tablename__ = "employees"
@Data                                    │
@NoArgsConstructor                       │      id: Mapped[int] = mapped_column(
public class Employee {                  │          primary_key=True)
    @Id                                  │      first_name: Mapped[str] = mapped_column(
    @GeneratedValue(strategy=IDENTITY)   │          String(255), nullable=False)
    private Long id;                     │      email: Mapped[str] = mapped_column(
                                         │          String(255), unique=True)
    @Column(nullable = false)            │      salary: Mapped[Decimal] = mapped_column(
    private String firstName;            │          Numeric(18, 2))
                                         │      department_id: Mapped[int] = mapped_column(
    @ManyToOne(fetch = LAZY)             │          ForeignKey("departments.id"))
    @JoinColumn(name = "department_id")  │
    private Department department;       │      department: Mapped["Department"] = relationship(
}                                        │          "Department", lazy="joined")
```

**What to remember:** No Lombok in Python — `Mapped[type]` is the modern SQLAlchemy 2.x syntax and it carries full type information. `mapped_column()` replaces `Column()`. The ORM does **not** generate a migration automatically — you use Alembic (see below).

---

### Schema Migrations

| Java | Python |
|---|---|
| `spring.jpa.hibernate.ddl-auto: update` (auto) | Alembic (explicit migration files) |

```
Java (application.yml)                  │  Python
─────────────────────────────────────────┼─────────────────────────────────────────
spring:                                  │  # Generate a migration:
  jpa:                                   │  alembic revision --autogenerate \
    hibernate:                           │    -m "add employees table"
      ddl-auto: update                   │
                                         │  # Apply migrations:
# Hibernate reads your entities          │  alembic upgrade head
# and alters the DB schema automatically │
# (dangerous in production)              │  # Each migration is a Python file you
                                         │  # can read, edit, and version-control
```

**What to remember:** `ddl-auto: update` is convenient but dangerous — Hibernate will silently DROP columns it no longer sees in your entity. Alembic generates explicit migration files you review before running. More ceremony upfront, safer in production.

---

### Queries and Sessions

| Java | Python |
|---|---|
| `@Transactional` on service methods | Explicit `async with AsyncSession` |

```
Java                                     │  Python
─────────────────────────────────────────┼─────────────────────────────────────────
@Service                                 │  class EmployeeRepository:
public class EmployeeService {           │      def __init__(self, session: AsyncSession):
                                         │          self.session = session
    @Transactional(readOnly = true)      │
    public List<Employee> getAll() {     │      async def find_all(self) -> list[Employee]:
        return repo.findAll();           │          result = await self.session.execute(
    }                                    │              select(Employee))
                                         │          return list(result.scalars().all())
    @Transactional                       │
    public Employee save(Employee e) {   │      async def save(self, emp: Employee) -> Employee:
        return repo.save(e);             │          self.session.add(emp)
    }                                    │          await self.session.commit()
}                                        │          await self.session.refresh(emp)
                                         │          return emp
```

**What to remember:** Spring's `@Transactional` is invisible AOP magic — it wraps your method in a transaction proxy. SQLAlchemy gives you a session object you work with directly. The session is not thread-safe and should **never** be shared across async tasks. FastAPI's `Depends(get_session)` creates a fresh session per request automatically.

---

## 6. MongoDB — PyMongo Async vs Spring Data MongoDB

### Document Model

| Java | Python |
|---|---|
| `@Document` class with Spring Data | Plain `dict` inserted into collection |

```
Java                                     │  Python
─────────────────────────────────────────┼─────────────────────────────────────────
@Document(collection = "employee_events")│  # No class needed — just a dict
public class EmployeeEventDocument {     │  document = {
    @Id                                  │      "employee_id": event["id"],
    private String id;                   │      "first_name": event["firstName"],
    private Long employeeId;             │      "salary": str(event["salary"]),
    private String eventType;            │      "event_type": "CREATED",
    private Instant eventTimestamp;      │      "event_timestamp": datetime.now(UTC),
}                                        │      "received_at": datetime.now(UTC),
                                         │  }
                                         │  await collection.insert_one(document)
```

### Queries

| Java | Python |
|---|---|
| Spring Data method name query | PyMongo aggregation pipeline |

```
Java                                     │  Python
─────────────────────────────────────────┼─────────────────────────────────────────
// Method name = query (Spring magic)    │  # Explicit aggregation pipeline
List<EmployeeEventDocument>              │  pipeline = [
    findByEmployeeId(Long id);           │      {"$sort": {"event_timestamp": DESCENDING}},
                                         │      {"$group": {
Optional<EmployeeEventDocument>          │          "_id": "$employee_id",
    findTopByEmployeeIdOrderBy           │          "latest": {"$first": "$$ROOT"}
    EventTimestampDesc(Long id);         │      }},
                                         │      {"$replaceRoot": {"newRoot": "$latest"}},
                                         │      {"$match": {"event_type": {"$ne": "DELETED"}}}
                                         │  ]
                                         │  cursor = collection.aggregate(pipeline)
                                         │  return await cursor.to_list(length=None)
```

**What to remember:** Spring Data generates queries from method names (magic). PyMongo uses explicit MongoDB query syntax — same pipeline you'd write in MongoDB Compass. More verbose, but you know exactly what query is running.

> **Note:** Motor (the async MongoDB library you'd see in older Python projects) was deprecated on **14 May 2025**. This project uses `pymongo` 4.x with its native `AsyncMongoClient`. If you see Motor in other codebases, know it has 1 year of support left.

---

## 7. Kafka — confluent-kafka vs Spring Kafka

### Producer

| Java | Python |
|---|---|
| `KafkaTemplate` with `@Autowired` | `Producer` class instantiated manually |

```
Java                                     │  Python
─────────────────────────────────────────┼─────────────────────────────────────────
@Component                               │  class EmployeeEventProducer:
@RequiredArgsConstructor                 │      def __init__(self):
public class EmployeeEventProducer {     │          self._producer = Producer({
    private final KafkaTemplate          │              "bootstrap.servers":
        <String, EmployeeEvent>          │              settings.kafka_bootstrap_servers
        kafkaTemplate;                   │          })
                                         │
    public void sendEvent(               │      def send_event(self, employee, dept,
        Employee emp, String eventType) {│              event_type: str) -> None:
        Message<EmployeeEvent> msg =     │          self._producer.produce(
            MessageBuilder              │              topic=self._topic,
            .withPayload(avroEvent)      │              key=str(employee.id),
            .setHeader("eventType",      │              value=self._serializer(payload, ctx),
                eventType)              │              headers={"eventType": event_type},
            .build();                    │              on_delivery=self._on_delivery,
        kafkaTemplate.send(msg);         │          )
    }                                    │          self._producer.poll(0)
}                                        │
```

**What to remember:** `KafkaTemplate` is Spring's abstraction — it manages serialization, headers, callbacks behind the scenes. `confluent-kafka`'s `Producer.produce()` is the raw Kafka API — you manage everything explicitly. `poll(0)` is required after `produce()` to trigger the delivery callbacks (fire and not-quite-forget).

---

### Consumer

| Java | Python |
|---|---|
| `@KafkaListener` annotation on a method | A `while True: poll()` loop running in a thread |

```
Java                                     │  Python
─────────────────────────────────────────┼─────────────────────────────────────────
@KafkaListener(                          │  def run(self, stop_event: threading.Event):
    topics = "${kafka.topic.employee}",  │      self._consumer.subscribe([self._topic])
    groupId = "${kafka.consumer.group}"  │
)                                        │      while not stop_event.is_set():
public void consume(                     │          msg = self._consumer.poll(timeout=1.0)
    ConsumerRecord<String,               │          if msg is None:
        EmployeeEvent> record) {         │              continue
    // Spring calls this per message     │          if msg.error():
    EmployeeEventDocument doc =          │              logger.error(...)
        mapper.toDocument(record.value());│              continue
    repo.save(doc);                      │
}                                        │          self._handle(msg)
                                         │          self._consumer.commit(msg)
```

**What to remember:** `@KafkaListener` hides the entire poll loop — Spring creates a `MessageListenerContainer` that handles heartbeats, rebalancing, and offset commits. In Python you **own the loop**. If your `_handle()` method blocks for 10 seconds, Kafka will think the consumer is dead and kick it out of the group. Keep handlers fast; offload slow work to a queue.

---

### Avro — Dynamic vs Compile-Time

| Java | Python |
|---|---|
| Avro Gradle plugin generates typed Java classes at build time | Schema loaded from `.avsc` at runtime; messages are plain dicts |

```
Java                                     │  Python
─────────────────────────────────────────┼─────────────────────────────────────────
// Avro generates this class:            │  # No generated class — load schema at startup:
EmployeeEvent event =                    │  schema_str = Path("avro/message.avsc").read_text()
    EmployeeEvent.newBuilder()           │  serializer = AvroSerializer(sr_client, schema_str)
    .setId(employee.getId())             │
    .setFirstName(employee.getFirstName())│  # Build payload as a plain dict:
    .setSalary(employee.getSalary())     │  payload = {
    .setEventType(EventType.CREATED)     │      "id": employee.id,
    .build();                            │      "firstName": employee.first_name,
                                         │      "salary": employee.salary,
// Compile error if field is wrong       │      "eventType": "CREATED",
                                         │  }
                                         │  # Runtime error if field is wrong
```

**What to remember:** Java's Avro is compile-time safe — a wrong field name is caught at build time. Python's Avro is runtime-validated against the schema when you call the serializer. More flexibility, less safety. In practice, writing tests that round-trip a message catches schema mismatches early.

---

## 8. Error Handling

| Java | Python |
|---|---|
| `@RestControllerAdvice` + `@ExceptionHandler` | `@app.exception_handler()` decorator |

```
Java                                     │  Python
─────────────────────────────────────────┼─────────────────────────────────────────
@RestControllerAdvice                    │  @app.exception_handler(ResourceNotFoundException)
public class GlobalExceptionHandler {    │  async def not_found_handler(
                                         │      request: Request,
    @ExceptionHandler(                   │      exc: ResourceNotFoundException
        ResourceNotFoundException.class) │  ):
    public ResponseEntity<Object>        │      return JSONResponse(
    handleNotFound(                      │          status_code=404,
        ResourceNotFoundException ex) {  │          content={"detail": exc.message}
        return ResponseEntity            │      )
            .status(NOT_FOUND)           │
            .body(Map.of("message",      │  # In the router — explicit, no annotation:
                ex.getMessage()));       │  try:
    }                                    │      return await service.get_by_id(id)
}                                        │  except ResourceNotFoundException as e:
                                         │      raise HTTPException(404, detail=e.message)
```

**What to remember:** Spring's `@RestControllerAdvice` is a global interceptor — any unhandled exception bubbles up to it. FastAPI has both: `app.exception_handler()` for global handlers, and explicit `try/except` in routers. The explicit pattern is more readable — you see the error handling at the call site.

---

## 9. Testing — pytest vs JUnit + Spring Boot Test

### Fixture vs @BeforeEach

| Java | Python |
|---|---|
| `@BeforeEach` method sets up state for all tests in a class | `@pytest.fixture` function injected into test functions that need it |

```
Java                                     │  Python
─────────────────────────────────────────┼─────────────────────────────────────────
@SpringBootTest                          │  @pytest.fixture(scope="session")
@AutoConfigureMockMvc                    │  def postgres_container():
class EmployeeTest {                     │      with PostgresContainer("postgres:16") as pg:
                                         │          yield pg  # teardown after all tests
    @Autowired                           │
    MockMvc mvc;                         │  @pytest_asyncio.fixture(scope="session")
                                         │  async def client(env_setup, setup_db):
    @BeforeEach                          │      from src.main import app
    void setup() {                       │      async with AsyncClient(
        // runs before every test        │          transport=ASGITransport(app=app),
    }                                    │          base_url="http://test"
}                                        │      ) as ac:
                                         │          yield ac
                                         │
                                         │  # Test receives client automatically:
                                         │  async def test_create_department(client):
                                         │      r = await client.post("/api/departments", ...)
```

**What to remember:** pytest fixtures are **dependency injection for tests**. A test function declares what it needs (by parameter name), and pytest provides it. `scope="session"` means the container starts once for all tests and stops at the end. JUnit's `@BeforeEach` runs before every single test — pytest's `scope` gives you control over when setup/teardown happens.

### Testcontainers

| Java | Python |
|---|---|
| `@Container` + `@DynamicPropertySource` static method | `@pytest.fixture` that `yield`s the container |

```
Java                                     │  Python
─────────────────────────────────────────┼─────────────────────────────────────────
@Container                               │  @pytest.fixture(scope="session")
static KafkaContainer kafka =            │  def kafka_container(docker_network):
    new KafkaContainer(                  │      with KafkaContainer(
        DockerImageName.parse(           │          image="confluentinc/cp-kafka:7.6.1"
            "confluentinc/cp-kafka:7.6.1"│      ) as kafka:
        ));                              │          kafka.with_network(docker_network)
                                         │          yield kafka
@DynamicPropertySource                   │
static void props(                       │  # Schema Registry (no first-class support):
    DynamicPropertyRegistry r) {         │  @pytest.fixture(scope="session")
    r.add("spring.kafka.bootstrap",      │  def schema_registry_container(kafka_container):
        kafka::getBootstrapServers);     │      container = DockerContainer(SR_IMAGE) \
}                                        │          .with_network(docker_network) \
                                         │          .with_env("SCHEMA_REGISTRY_...", ...)
                                         │      with container:
                                         │          yield container
```

**What to remember:** Java Testcontainers uses `@Container` to auto-manage lifecycle. Python uses context managers (`with`) inside fixtures. Schema Registry isn't a first-class container in `testcontainers-python` — you use `DockerContainer` (generic) with a shared Docker network, exactly like `GenericContainer` in Java.

---

## 10. Packaging — uv + pyproject.toml vs Gradle

| Java | Python |
|---|---|
| `build.gradle` + Gradle wrapper | `pyproject.toml` + `uv` |

```
Java (build.gradle)                      │  Python (pyproject.toml)
─────────────────────────────────────────┼─────────────────────────────────────────
plugins {                                │  [project]
    id 'org.springframework.boot'        │  name = "kafka-ai-python"
        version '4.0.6'                  │  requires-python = ">=3.12"
}                                        │
                                         │  dependencies = [
dependencies {                           │      "fastapi[standard]>=0.136.3",
    implementation                       │      "confluent-kafka[avro,schemaregistry]>=2.14.0",
        'org.springframework.boot:       │      "sqlalchemy[asyncio]>=2.0.50",
         spring-boot-starter-web'        │      "pymongo>=4.10.0",
    implementation                       │  ]
        'io.confluent:                   │
         kafka-avro-serializer:7.6.1'   │  [tool.uv]
    testImplementation                   │  dev-dependencies = [
        'org.testcontainers:kafka'       │      "pytest>=9.0.3",
}                                        │      "testcontainers[kafka,postgres,mongodb]",
                                         │  ]
```

| Task | Java | Python |
|---|---|---|
| Install deps | `./gradlew build` (downloads) | `uv sync` |
| Run tests | `./gradlew test` | `uv run pytest tests/ -v` |
| Run app | `./gradlew bootRun` | `uv run uvicorn src.main:app --reload` |
| Add a dep | Edit `build.gradle`, re-run | `uv add fastapi` |
| Speed | Gradle daemon: fast after first run | uv: ~10-100x faster than pip, always fast |

---

## 11. Common Java Habits That Break in Python

| Java habit | What happens in Python | Fix |
|---|---|---|
| Expecting type annotations to enforce contracts | `def fn(x: int)` accepts anything at runtime — annotations are hints for tools, not the compiler | Use Pydantic for runtime validation at system boundaries |
| Building deep inheritance hierarchies | Inheritance in Python works but fights the language — duck typing is idiomatic | Prefer composition; define `Protocol` if you need a structural type |
| Thinking `@Transactional` is magic | SQLAlchemy sessions are not magic — you own `commit()` and `rollback()` | Always use `Depends(get_session)` in FastAPI; never share sessions across async tasks |
| `ddl-auto: update` in Spring → works in prod | Alembic is required for production migrations | Generate and review Alembic migration files before deploying |
| `@KafkaListener` handles thread management | `confluent-kafka` consumer runs in your thread — if it blocks, Kafka evicts it | Keep consumer handlers fast; if slow work is needed, publish to an internal queue |
| `System.out.println()` for debugging | Python `print()` works but loses structure | Use `structlog` — every log entry is JSON, searchable in prod |

---

## Quick Reference — At-a-Glance Mapping

| Concept | Java (Spring) | Python (FastAPI) |
|---|---|---|
| HTTP router | `@RestController` + `@GetMapping` | `APIRouter` + `@router.get()` |
| Dependency injection | Constructor + `@Autowired` | Function argument + `Depends()` |
| Request body | `@RequestBody @Valid DTO` | Pydantic `BaseModel` parameter |
| Validation | `@NotBlank`, `@Email` on DTO | `field_validator` in `BaseModel` |
| DB model | `@Entity` JPA class | `Base` SQLAlchemy subclass |
| DB query | `JpaRepository` method names | `session.execute(select(...))` |
| Transaction | `@Transactional` | `await session.commit()` |
| Migrations | `ddl-auto: update` | `alembic upgrade head` |
| MongoDB doc | `@Document` class | Plain `dict` |
| MongoDB query | Spring Data method name | PyMongo aggregation pipeline |
| Kafka producer | `KafkaTemplate.send()` | `Producer.produce()` + `poll(0)` |
| Kafka consumer | `@KafkaListener` method | `while True: consumer.poll()` in thread |
| Avro | Gradle plugin → typed classes | Load `.avsc` at runtime → dicts |
| Config | `application.yml` + `@Value` | `.env` + `pydantic-settings` |
| Error handling | `@RestControllerAdvice` | `app.exception_handler()` |
| Test setup | `@BeforeEach` / `@MockBean` | `@pytest.fixture` |
| Test HTTP client | `MockMvc` | `httpx.AsyncClient` |
| Containers in tests | `@Container` + `@DynamicPropertySource` | `pytest.fixture` + context manager |
| Package manager | Gradle | uv |
| Run app | `./gradlew bootRun` | `uv run uvicorn src.main:app` |
| Run tests | `./gradlew test` | `uv run pytest tests/` |
