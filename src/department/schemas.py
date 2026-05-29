from pydantic import BaseModel, field_validator


class DepartmentRequest(BaseModel):
    name: str
    location: str

    @field_validator("name", "location")
    @classmethod
    def not_blank(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("must not be blank")
        return v


class DepartmentResponse(BaseModel):
    id: int
    name: str
    location: str
    employee_count: int = 0

    model_config = {"from_attributes": True}
