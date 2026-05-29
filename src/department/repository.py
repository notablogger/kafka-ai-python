from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.department.models import Department


class DepartmentRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def find_all(self) -> list[Department]:
        result = await self.session.execute(select(Department))
        return list(result.scalars().all())

    async def find_by_id(self, id: int) -> Department | None:
        return await self.session.get(Department, id)

    async def save(self, department: Department) -> Department:
        self.session.add(department)
        await self.session.commit()
        await self.session.refresh(department)
        return department

    async def delete(self, department: Department) -> None:
        await self.session.delete(department)
        await self.session.commit()
