import pytest


@pytest.fixture(scope="module")
async def dept_id(client):
    r = await client.post("/api/departments", json={"name": "Dev", "location": "Amsterdam"})
    return r.json()["id"]


@pytest.mark.asyncio
async def test_create_employee(client, dept_id):
    response = await client.post("/api/employees", json={
        "first_name": "Alice",
        "last_name": "Smith",
        "email": "alice@example.com",
        "salary": "75000.00",
        "hire_date": "2023-01-15",
        "department_id": dept_id,
    })
    assert response.status_code == 201
    data = response.json()
    assert data["first_name"] == "Alice"
    assert data["email"] == "alice@example.com"


@pytest.mark.asyncio
async def test_get_employee_by_id(client, dept_id):
    create = await client.post("/api/employees", json={
        "first_name": "Bob",
        "last_name": "Jones",
        "email": "bob@example.com",
        "salary": "60000.00",
        "hire_date": "2022-06-01",
        "department_id": dept_id,
    })
    emp_id = create.json()["id"]

    response = await client.get(f"/api/employees/{emp_id}")
    assert response.status_code == 200
    assert response.json()["id"] == emp_id


@pytest.mark.asyncio
async def test_update_employee(client, dept_id):
    create = await client.post("/api/employees", json={
        "first_name": "Carol",
        "last_name": "White",
        "email": "carol@example.com",
        "salary": "55000.00",
        "hire_date": "2021-03-10",
        "department_id": dept_id,
    })
    emp_id = create.json()["id"]

    response = await client.put(f"/api/employees/{emp_id}", json={
        "first_name": "Carol",
        "last_name": "White",
        "email": "carol@example.com",
        "salary": "65000.00",
        "hire_date": "2021-03-10",
        "department_id": dept_id,
    })
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_delete_employee(client, dept_id):
    create = await client.post("/api/employees", json={
        "first_name": "Dave",
        "last_name": "Brown",
        "email": "dave@example.com",
        "salary": "50000.00",
        "hire_date": "2020-09-01",
        "department_id": dept_id,
    })
    emp_id = create.json()["id"]

    response = await client.delete(f"/api/employees/{emp_id}")
    assert response.status_code == 204


@pytest.mark.asyncio
async def test_get_nonexistent_employee(client):
    response = await client.get("/api/employees/999999")
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_create_employee_invalid_email(client, dept_id):
    response = await client.post("/api/employees", json={
        "first_name": "Eve",
        "last_name": "Bad",
        "email": "not-an-email",
        "salary": "50000.00",
        "hire_date": "2023-01-01",
        "department_id": dept_id,
    })
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_create_employee_negative_salary(client, dept_id):
    response = await client.post("/api/employees", json={
        "first_name": "Frank",
        "last_name": "Broke",
        "email": "frank@example.com",
        "salary": "-100.00",
        "hire_date": "2023-01-01",
        "department_id": dept_id,
    })
    assert response.status_code == 422
