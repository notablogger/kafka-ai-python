from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from src.db.postgres import get_session
from src.department.schemas import DepartmentRequest, DepartmentResponse
from src.department.service import DepartmentService
from src.shared.exceptions import ResourceNotFoundException

router = APIRouter(prefix="/api/departments", tags=["Departments"])


def get_service(session: AsyncSession = Depends(get_session)) -> DepartmentService:
    return DepartmentService(session)


@router.get("", response_model=list[DepartmentResponse])
async def get_all(service: DepartmentService = Depends(get_service)):
    return await service.get_all()


@router.get("/{id}", response_model=DepartmentResponse)
async def get_by_id(id: int, service: DepartmentService = Depends(get_service)):
    try:
        return await service.get_by_id(id)
    except ResourceNotFoundException as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=e.message)


@router.post("", response_model=DepartmentResponse, status_code=status.HTTP_201_CREATED)
async def create(request: DepartmentRequest, service: DepartmentService = Depends(get_service)):
    return await service.create(request)


@router.put("/{id}", response_model=DepartmentResponse)
async def update(id: int, request: DepartmentRequest, service: DepartmentService = Depends(get_service)):
    try:
        return await service.update(id, request)
    except ResourceNotFoundException as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=e.message)


@router.delete("/{id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete(id: int, service: DepartmentService = Depends(get_service)):
    try:
        await service.delete(id)
    except ResourceNotFoundException as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=e.message)
