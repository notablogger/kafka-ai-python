import datetime
from decimal import Decimal

from pymongo import DESCENDING
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.db.mongo import get_db
from src.employee.models import Employee


class EmployeePostgresRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def find_by_id(self, id: int) -> Employee | None:
        return await self.session.get(Employee, id)

    async def save(self, employee: Employee) -> Employee:
        self.session.add(employee)
        await self.session.commit()
        await self.session.refresh(employee)
        return employee

    async def delete(self, employee: Employee) -> None:
        await self.session.delete(employee)
        await self.session.commit()


class EmployeeMongoRepository:
    def __init__(self):
        self.collection = get_db()["employee_events"]

    async def save_event(self, document: dict) -> None:
        await self.collection.insert_one(document)

    async def find_latest_active_snapshots(self) -> list[dict]:
        """Return the latest event per employee, excluding DELETED employees."""
        pipeline = [
            {"$sort": {"event_timestamp": DESCENDING}},
            {"$group": {"_id": "$employee_id", "latest": {"$first": "$$ROOT"}}},
            {"$replaceRoot": {"newRoot": "$latest"}},
            {"$match": {"event_type": {"$ne": "DELETED"}}},
        ]
        cursor = self.collection.aggregate(pipeline)
        return await cursor.to_list(length=None)

    async def find_latest_active_by_id(self, employee_id: int) -> dict | None:
        doc = await self.collection.find_one(
            {"employee_id": employee_id},
            sort=[("event_timestamp", DESCENDING)],
        )
        if doc and doc.get("event_type") == "DELETED":
            return None
        return doc

    async def find_latest_active_by_department(self, department_id: int) -> list[dict]:
        all_docs = await self.find_latest_active_snapshots()
        return [d for d in all_docs if d.get("department_id") == department_id]
