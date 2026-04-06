from fastapi import FastAPI

from project.app.routes.ingest import router as ingest_router
from project.app.routes.publications import router as publications_router

app = FastAPI(
    title="CONVOCATORIA Hexagonal API",
    version="1.0.0",
    description="API de entrada para pipeline ETL hexagonal de reconciliacion bibliografica.",
)

app.include_router(ingest_router)
app.include_router(publications_router)


@app.get("/health", tags=["General"])
def health() -> dict:
    return {"status": "ok"}
