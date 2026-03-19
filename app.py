from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.templating import Jinja2Templates
from db import fetch_fuel_rows
from logic import process_fuel_data
from config import DOMAIN_CONFIG
import tempfile
import time
import os
from datetime import datetime

app = FastAPI(title="Fuel Service API")

templates = Jinja2Templates(directory="templates")


def validate_common_inputs(
    domain: str,
    dateFrom: str,
    dateTo: str,
    idMezzo: int | None,
    targa: str | None,
):
    domain = domain.strip().upper()

    if targa is not None:
        targa = targa.strip()
        if targa == "":
            targa = None

    if domain not in DOMAIN_CONFIG:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid domain '{domain}'. Allowed domains: {', '.join(DOMAIN_CONFIG.keys())}"
        )

    if idMezzo is None and targa is None:
        raise HTTPException(
            status_code=400,
            detail="Provide either idMezzo or targa."
        )

    try:
        parsed_date_from = datetime.strptime(dateFrom, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail="dateFrom must be in format YYYY-MM-DD."
        )

    try:
        parsed_date_to = datetime.strptime(dateTo, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail="dateTo must be in format YYYY-MM-DD."
        )

    if parsed_date_from > parsed_date_to:
        raise HTTPException(
            status_code=400,
            detail="dateFrom cannot be later than dateTo."
        )

    return domain, targa


@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "domains": list(DOMAIN_CONFIG.keys()),
        },
    )


@app.get("/health")
def health():
    return {"message": "Fuel Service API is running"}


@app.get("/fuel/data")
def get_fuel_data(
    domain: str = Query(..., description="Domain/server name, e.g. WAY6223"),
    dateFrom: str = Query(..., description="Start date in format YYYY-MM-DD"),
    dateTo: str = Query(..., description="End date in format YYYY-MM-DD"),
    idMezzo: int | None = Query(None, description="Vehicle ID_MEZZO"),
    targa: str | None = Query(None, description="License plate / TARGA"),
    limit: int | None = Query(None, description="Optional max number of rows to return"),
):
    domain, targa = validate_common_inputs(domain, dateFrom, dateTo, idMezzo, targa)

    if limit is not None and limit <= 0:
        raise HTTPException(status_code=400, detail="limit must be greater than 0.")

    try:
        df = fetch_fuel_rows(
            domain=domain,
            date_from=dateFrom,
            date_to=dateTo,
            id_mezzo=idMezzo,
            targa=targa,
        )

        json_df = df[["ID_MEZZO", "TARGA", "FuelValue", "DATAEVENTO"]].copy()

        json_df = json_df.rename(columns={
            "ID_MEZZO": "idMezzo",
            "TARGA": "targa",
            "FuelValue": "fuelLevel",
            "DATAEVENTO": "dataEvento",
        })

        json_df["dataEvento"] = json_df["dataEvento"].astype(str)

        if limit is not None:
            json_df = json_df.head(limit)

        return json_df.to_dict(orient="records")

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except KeyError:
        raise HTTPException(status_code=500, detail="Expected columns were not found in fetched data.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal error: {str(e)}")


@app.get("/fuel/export")
def export_fuel_data(
    domain: str = Query(..., description="Domain/server name, e.g. WAY6223"),
    dateFrom: str = Query(..., description="Start date in format YYYY-MM-DD"),
    dateTo: str = Query(..., description="End date in format YYYY-MM-DD"),
    idMezzo: int | None = Query(None, description="Vehicle ID_MEZZO"),
    targa: str | None = Query(None, description="License plate / TARGA"),
):
    domain, targa = validate_common_inputs(domain, dateFrom, dateTo, idMezzo, targa)

    try:
        total_start = time.time()

        fetch_start = time.time()
        raw_df = fetch_fuel_rows(
            domain=domain,
            date_from=dateFrom,
            date_to=dateTo,
            id_mezzo=idMezzo,
            targa=targa,
        )
        fetch_end = time.time()

        process_start = time.time()
        processed_df = process_fuel_data(raw_df)
        process_end = time.time()

        total_end = time.time()

        sql_fetch_seconds = fetch_end - fetch_start
        processing_seconds = process_end - process_start
        total_seconds = total_end - total_start

        raw_rows = len(raw_df)
        final_rows = len(processed_df)
        suspected_refuels = 0
        if "IsPossibleRefuel" in processed_df.columns:
            suspected_refuels = int(processed_df["IsPossibleRefuel"].fillna(0).sum())

        temp_dir = tempfile.gettempdir()

        search_value = str(idMezzo) if idMezzo is not None else targa
        safe_search_value = str(search_value).replace(" ", "_").replace("/", "_").replace("\\", "_")

        filename = f"fuel_analysis_{domain}_{safe_search_value}.csv"
        file_path = os.path.join(temp_dir, filename)

        processed_df.to_csv(file_path, index=False)

        print(f"Request completed in {total_seconds:.2f} seconds")
        print(f"CSV created at: {file_path}")

        headers = {
            "X-SQL-Fetch-Time": f"{sql_fetch_seconds:.2f}",
            "X-Processing-Time": f"{processing_seconds:.2f}",
            "X-Total-Time": f"{total_seconds:.2f}",
            "X-Raw-Rows": str(raw_rows),
            "X-Final-Rows": str(final_rows),
            "X-Suspected-Refuels": str(suspected_refuels),
        }

        return FileResponse(
            path=file_path,
            media_type="text/csv",
            filename=filename,
            headers=headers,
        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal error: {str(e)}")