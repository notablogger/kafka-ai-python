import datetime
from decimal import Decimal

from pydantic import BaseModel, EmailStr, field_validator


class EmployeeRequest(BaseModel):
    first_name: str
    last_name: str
    email: EmailStr
    salary: Decimal
    hire_date: datetime.date
    department_id: int

    @field_validator("first_name", "last_name")
    @classmethod
    def not_blank(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("must not be blank")
        return v

    @field_validator("salary")
    @classmethod
    def salary_positive(cls, v: Decimal) -> Decimal:
        if v <= 0:
            raise ValueError("salary must be greater than 0")
        return v


class EmployeeResponse(BaseModel):
    id: int
    first_name: str
    last_name: str
    email: str
    salary: Decimal
    hire_date: datetime.date
    department_id: int
    department_name: str
    department_location: str
    event_type: str | None = None
    event_timestamp: datetime.datetime | None = None

    model_config = {"from_attributes": True}
