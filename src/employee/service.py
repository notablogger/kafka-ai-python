import datetime
from decimal import Decimal

from sqlalchemy.ext.asyncio import AsyncSession

from src.department.repository import DepartmentRepository
from src.employee.models import Employee
from src.employee.repository import EmployeeMongoRepository, EmployeePostgresRepository
from src.employee.schemas import EmployeeRequest, EmployeeResponse
from src.shared.exceptions import ResourceNotFoundException


class EmployeeService:
    def __init__(self, session: AsyncSession, producer=None):
        self.pg_repo = EmployeePostgresRepository(session)
        self.mongo_repo = EmployeeMongoRepository()
        self.dept_repo = DepartmentRepository(session)
        self.producer = producer

    async def get_all(self) -> list[EmployeeResponse]:
        docs = await self.mongo_repo.find_latest_active_snapshots()
        return [self._doc_to_response(d) for d in docs]

    async def get_by_id(self, id: int) -> EmployeeResponse:
        doc = await self.mongo_repo.find_latest_active_by_id(id)
        if doc is None:
            raise ResourceNotFoundException("Employee", id)
        return self._doc_to_response(doc)

    async def get_by_department(self, department_id: int) -> list[EmployeeResponse]:
        docs = await self.mongo_repo.find_latest_active_by_department(department_id)
        return [self._doc_to_response(d) for d in docs]

    async def create(self, request: EmployeeRequest) -> EmployeeResponse:
        dept = await self.dept_repo.find_by_id(request.department_id)
        if dept is None:
            raise ResourceNotFoundException("Department", request.department_id)

        employee = Employee(
            first_name=request.first_name,
            last_name=request.last_name,
            email=request.email,
            salary=request.salary,
            hire_date=request.hire_date,
            department_id=request.department_id,
        )
        saved = await self.pg_repo.save(employee)

        if self.producer:
            self.producer.send_event(saved, dept, "CREATED")

        return EmployeeResponse(
            id=saved.id,
            first_name=saved.first_name,
            last_name=saved.last_name,
            email=saved.email,
            salary=saved.salary,
            hire_date=saved.hire_date,
            department_id=dept.id,
            department_name=dept.name,
            department_location=dept.location,
        )

    async def update(self, id: int, request: EmployeeRequest) -> EmployeeResponse:
        employee = await self.pg_repo.find_by_id(id)
        if employee is None:
            raise ResourceNotFoundException("Employee", id)

        dept = await self.dept_repo.find_by_id(request.department_id)
        if dept is None:
            raise ResourceNotFoundException("Department", request.department_id)

        employee.first_name = request.first_name
        employee.last_name = request.last_name
        employee.email = request.email
        employee.salary = request.salary
        employee.hire_date = request.hire_date
        employee.department_id = request.department_id
        saved = await self.pg_repo.save(employee)

        if self.producer:
            self.producer.send_event(saved, dept, "UPDATED")

        return EmployeeResponse(
            id=saved.id,
            first_name=saved.first_name,
            last_name=saved.last_name,
            email=saved.email,
            salary=saved.salary,
            hire_date=saved.hire_date,
            department_id=dept.id,
            department_name=dept.name,
            department_location=dept.location,
        )

    async def delete(self, id: int) -> None:
        employee = await self.pg_repo.find_by_id(id)
        if employee is None:
            raise ResourceNotFoundException("Employee", id)

        dept = await self.dept_repo.find_by_id(employee.department_id)

        if self.producer:
            self.producer.send_event(employee, dept, "DELETED")

        await self.pg_repo.delete(employee)

    def _doc_to_response(self, doc: dict) -> EmployeeResponse:
        return EmployeeResponse(
            id=doc["employee_id"],
            first_name=doc["first_name"],
            last_name=doc["last_name"],
            email=doc["email"],
            salary=Decimal(str(doc["salary"])),
            hire_date=doc["hire_date"],
            department_id=doc["department_id"],
            department_name=doc["department_name"],
            department_location=doc["department_location"],
            event_type=doc.get("event_type"),
            event_timestamp=doc.get("event_timestamp"),
        )
