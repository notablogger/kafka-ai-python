from sqlalchemy.ext.asyncio import AsyncSession

from src.department.models import Department
from src.department.repository import DepartmentRepository
from src.department.schemas import DepartmentRequest, DepartmentResponse
from src.shared.exceptions import ResourceNotFoundException


class DepartmentService:
    def __init__(self, session: AsyncSession):
        self.repo = DepartmentRepository(session)

    async def get_all(self) -> list[DepartmentResponse]:
        departments = await self.repo.find_all()
        return [self._to_response(d) for d in departments]

    async def get_by_id(self, id: int) -> DepartmentResponse:
        dept = await self._find_or_raise(id)
        return self._to_response(dept)

    async def create(self, request: DepartmentRequest) -> DepartmentResponse:
        dept = Department(name=request.name, location=request.location)
        saved = await self.repo.save(dept)
        return self._to_response(saved)

    async def update(self, id: int, request: DepartmentRequest) -> DepartmentResponse:
        dept = await self._find_or_raise(id)
        dept.name = request.name
        dept.location = request.location
        saved = await self.repo.save(dept)
        return self._to_response(saved)

    async def delete(self, id: int) -> None:
        dept = await self._find_or_raise(id)
        await self.repo.delete(dept)

    async def _find_or_raise(self, id: int) -> Department:
        dept = await self.repo.find_by_id(id)
        if dept is None:
            raise ResourceNotFoundException("Department", id)
        return dept

    def _to_response(self, dept: Department) -> DepartmentResponse:
        return DepartmentResponse(
            id=dept.id,
            name=dept.name,
            location=dept.location,
            employee_count=len(dept.employees) if dept.employees else 0,
        )
