from fastapi import APIRouter, Depends, HTTPException, Request, status
from sqlalchemy.ext.asyncio import AsyncSession

from src.db.postgres import get_session
from src.employee.schemas import EmployeeRequest, EmployeeResponse
from src.employee.service import EmployeeService
from src.shared.exceptions import ResourceNotFoundException

router = APIRouter(prefix="/api/employees", tags=["Employees"])


def get_service(request: Request, session: AsyncSession = Depends(get_session)) -> EmployeeService:
    producer = getattr(request.app.state, "producer", None)
    return EmployeeService(session, producer)


@router.get("", response_model=list[EmployeeResponse])
async def get_all(service: EmployeeService = Depends(get_service)):
    return await service.get_all()


@router.get("/department/{department_id}", response_model=list[EmployeeResponse])
async def get_by_department(department_id: int, service: EmployeeService = Depends(get_service)):
    return await service.get_by_department(department_id)


@router.get("/{id}", response_model=EmployeeResponse)
async def get_by_id(id: int, service: EmployeeService = Depends(get_service)):
    try:
        return await service.get_by_id(id)
    except ResourceNotFoundException as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=e.message)


@router.post("", response_model=EmployeeResponse, status_code=status.HTTP_201_CREATED)
async def create(request: EmployeeRequest, service: EmployeeService = Depends(get_service)):
    try:
        return await service.create(request)
    except ResourceNotFoundException as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=e.message)


@router.put("/{id}", response_model=EmployeeResponse)
async def update(id: int, request: EmployeeRequest, service: EmployeeService = Depends(get_service)):
    try:
        return await service.update(id, request)
    except ResourceNotFoundException as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=e.message)


@router.delete("/{id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete(id: int, service: EmployeeService = Depends(get_service)):
    try:
        await service.delete(id)
    except ResourceNotFoundException as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=e.message)
