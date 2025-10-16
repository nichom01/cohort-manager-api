from fastapi import FastAPI

from app.api.v1 import (
    cohort,
    demographic,
    distribution,
    exception,
    orchestration,
    participant_management,
    transformation,
    user,
    validation,
)
from app.core.config import config
from app.core.logging import setup_logging
from app.db.schema import Base, engine

setup_logging()
Base.metadata.create_all(bind=engine)

app = FastAPI(title=config.app_name)


# Register routes
app.include_router(user.router, prefix="/api/v1")
app.include_router(cohort.router, prefix="/api/v1/cohort")
app.include_router(demographic.router, prefix="/api/v1/demographic")
app.include_router(
    participant_management.router, prefix="/api/v1/participant-management"
)
app.include_router(validation.router, prefix="/api/v1/validation")
app.include_router(transformation.router, prefix="/api/v1/transformation")
app.include_router(distribution.router, prefix="/api/v1/distribution")
app.include_router(exception.router, prefix="/api/v1/exception")
app.include_router(orchestration.router, prefix="/api/v1/orchestration")
