from sqlalchemy import String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from src.db.postgres import Base


class Department(Base):
    __tablename__ = "departments"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    location: Mapped[str] = mapped_column(String(255), nullable=False)

    employees: Mapped[list["Employee"]] = relationship(  # noqa: F821
        "Employee", back_populates="department", cascade="all, delete-orphan", lazy="select"
    )
