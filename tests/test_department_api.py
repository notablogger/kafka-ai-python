import pytest


@pytest.mark.asyncio
async def test_create_department(client):
    response = await client.post("/api/departments", json={"name": "Engineering", "location": "London"})
    assert response.status_code == 201
    data = response.json()
    assert data["name"] == "Engineering"
    assert data["location"] == "London"
    assert "id" in data


@pytest.mark.asyncio
async def test_get_all_departments(client):
    response = await client.get("/api/departments")
    assert response.status_code == 200
    assert isinstance(response.json(), list)


@pytest.mark.asyncio
async def test_get_department_by_id(client):
    create = await client.post("/api/departments", json={"name": "Finance", "location": "New York"})
    dept_id = create.json()["id"]

    response = await client.get(f"/api/departments/{dept_id}")
    assert response.status_code == 200
    assert response.json()["id"] == dept_id


@pytest.mark.asyncio
async def test_update_department(client):
    create = await client.post("/api/departments", json={"name": "HR", "location": "Berlin"})
    dept_id = create.json()["id"]

    response = await client.put(f"/api/departments/{dept_id}", json={"name": "Human Resources", "location": "Berlin"})
    assert response.status_code == 200
    assert response.json()["name"] == "Human Resources"


@pytest.mark.asyncio
async def test_delete_department(client):
    create = await client.post("/api/departments", json={"name": "Temp", "location": "Nowhere"})
    dept_id = create.json()["id"]

    response = await client.delete(f"/api/departments/{dept_id}")
    assert response.status_code == 204

    get = await client.get(f"/api/departments/{dept_id}")
    assert get.status_code == 404


@pytest.mark.asyncio
async def test_get_nonexistent_department(client):
    response = await client.get("/api/departments/999999")
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_create_department_validation(client):
    response = await client.post("/api/departments", json={"name": "", "location": "X"})
    assert response.status_code == 422
