from fastapi import FastAPI,HTTPException
from app.router import route
 
app = FastAPI(
    title="BigQuery Schema Accelerator",
    version="1.0.0"
)
 
@app.post("/process-schema")
def process_schema(payload: dict):
    try:
        return route(payload)
    except RuntimeError as e:
        # STRICT policy failure → client error, not server crash
        raise HTTPException(
            status_code=409,   # Conflict
            detail={
                "status": "ERROR",
                "decision": "BREAKING",
                "message": str(e),
            }
        )