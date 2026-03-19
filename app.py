from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from db import fetch_fuel_rows
from logic import process_fuel_data
import tempfile
import time
import os

app = FastAPI(title="Fuel Service API")


@app.get("/")
def home():
    return {"message": "Fuel Service API is running"}


@app.get("/fuel/export")
def export_fuel_data(
    domain: str = Query(..., description="Domain/server name, e.g. WAY6223"),
    dateFrom: str = Query(..., description="Start date, e.g. 2026-02-17"),
    dateTo: str = Query(..., description="End date, e.g. 2026-02-18"),
    idMezzo: int | None = Query(None, description="Vehicle ID_MEZZO"),
    targa: str | None = Query(None, description="License plate / TARGA"),
):
    # Validation
    if idMezzo is None and (targa is None or not targa.strip()):
        raise HTTPException(
            status_code=400,
            detail="Provide either idMezzo or targa."
        )

    try:
        start_time = time.time()

        raw_df = fetch_fuel_rows(
            domain=domain,
            date_from=dateFrom,
            date_to=dateTo,
            id_mezzo=idMezzo,
            targa=targa,
        )

        processed_df = process_fuel_data(raw_df)

        end_time = time.time()
        elapsed_seconds = end_time - start_time

        # Create temp CSV file
        temp_dir = tempfile.gettempdir()

        search_value = str(idMezzo) if idMezzo is not None else targa.strip()
        safe_search_value = str(search_value).replace(" ", "_").replace("/", "_").replace("\\", "_")

        filename = f"fuel_analysis_{domain}_{safe_search_value}.csv"
        file_path = os.path.join(temp_dir, filename)

        processed_df.to_csv(file_path, index=False)

        print(f"Request completed in {elapsed_seconds:.2f} seconds")
        print(f"CSV created at: {file_path}")

        return FileResponse(
            path=file_path,
            media_type="text/csv",
            filename=filename
        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal error: {str(e)}")