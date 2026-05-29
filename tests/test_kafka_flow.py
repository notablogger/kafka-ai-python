"""
Integration tests for the full Kafka event flow:
  POST employee → Kafka CREATED event → consumer → MongoDB → GET returns snapshot
  PUT employee  → Kafka UPDATED event → consumer → MongoDB → GET returns updated
  DELETE employee → Kafka DELETED event → consumer → MongoDB → GET returns 404
"""
import asyncio
import pytest


async def _wait_for(client, url: str, expected_status: int, timeout: int = 20):
    """Poll until the endpoint returns the expected status or timeout expires."""
    for _ in range(timeout):
        r = await client.get(url)
        if r.status_code == expected_status:
            return r
        await asyncio.sleep(1)
    raise TimeoutError(f"Timed out waiting for {url} to return {expected_status}")


@pytest.fixture(scope="module")
async def dept_id(client):
    r = await client.post("/api/departments", json={"name": "KafkaTest", "location": "Tokyo"})
    return r.json()["id"]


@pytest.mark.asyncio
async def test_created_event_appears_in_get(client, dept_id):
    create = await client.post("/api/employees", json={
        "first_name": "Kafka",
        "last_name": "Created",
        "email": "kafka.created@example.com",
        "salary": "80000.00",
        "hire_date": "2024-01-01",
        "department_id": dept_id,
    })
    assert create.status_code == 201
    emp_id = create.json()["id"]

    # Wait for consumer to save the CREATED event to MongoDB
    response = await _wait_for(client, f"/api/employees/{emp_id}", 200)
    data = response.json()
    assert data["first_name"] == "Kafka"
    assert data["event_type"] == "CREATED"


@pytest.mark.asyncio
async def test_updated_event_reflected_in_get(client, dept_id):
    create = await client.post("/api/employees", json={
        "first_name": "Kafka",
        "last_name": "ToUpdate",
        "email": "kafka.update@example.com",
        "salary": "70000.00",
        "hire_date": "2024-02-01",
        "department_id": dept_id,
    })
    emp_id = create.json()["id"]

    # Wait for CREATED event to land
    await _wait_for(client, f"/api/employees/{emp_id}", 200)

    # Update
    await client.put(f"/api/employees/{emp_id}", json={
        "first_name": "Kafka",
        "last_name": "Updated",
        "email": "kafka.update@example.com",
        "salary": "90000.00",
        "hire_date": "2024-02-01",
        "department_id": dept_id,
    })

    # Wait for UPDATED event — poll until salary reflects update
    for _ in range(20):
        r = await client.get(f"/api/employees/{emp_id}")
        if r.status_code == 200 and r.json().get("event_type") == "UPDATED":
            assert r.json()["last_name"] == "Updated"
            return
        await asyncio.sleep(1)
    raise TimeoutError("UPDATED event did not appear in MongoDB")


@pytest.mark.asyncio
async def test_deleted_employee_excluded_from_get(client, dept_id):
    create = await client.post("/api/employees", json={
        "first_name": "Kafka",
        "last_name": "ToDelete",
        "email": "kafka.delete@example.com",
        "salary": "60000.00",
        "hire_date": "2024-03-01",
        "department_id": dept_id,
    })
    emp_id = create.json()["id"]

    # Wait for CREATED event
    await _wait_for(client, f"/api/employees/{emp_id}", 200)

    # Delete
    await client.delete(f"/api/employees/{emp_id}")

    # Wait for DELETED event — employee must disappear from GET
    await _wait_for(client, f"/api/employees/{emp_id}", 404)
