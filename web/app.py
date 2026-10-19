"""
Grontia web app: evaluate an address, or a listing PDF the user uploads, and
say whether the asking price or rent is in line with the open data.

    uvicorn web.app:app --host 0.0.0.0 --port 8000   (from /opt/grontia)

Nothing the user sends is written anywhere: the PDF is parsed in memory and
the evaluation goes straight back to the browser (see the listing data policy).
"""
from __future__ import annotations

import logging
from argparse import Namespace
from pathlib import Path

import requests
from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from evaluation.__main__ import run
from . import listing

logger = logging.getLogger("grontia.web")

MAX_UPLOAD_BYTES = 15 * 1024 * 1024

app = FastAPI(title="Grontia", docs_url="/api/docs", openapi_url="/api/openapi.json")


class EvaluateRequest(BaseModel):
    postcode: str = Field(pattern=r"^\s*\d{4}\s?[A-Za-z]{2}\s*$")
    number: int = Field(gt=0)
    suffix: str | None = Field(None, max_length=6)
    asking_price: float | None = Field(None, gt=0)
    rent: float | None = Field(None, gt=0)
    deposit: float | None = Field(None, ge=0)
    woz: float | None = Field(None, gt=0)
    woz_year: int | None = Field(None, ge=2000, le=2100)
    energy_label: str | None = Field(None, pattern=r"^[A-Ga-g]\+{0,4}$")
    comparable_price: float | None = Field(None, gt=0)
    comparable_date: str | None = Field(None, pattern=r"^\d{4}-\d{2}(-\d{2})?$")
    comparable_m2: float | None = Field(None, gt=0)


@app.post("/api/evaluate")
def evaluate(req: EvaluateRequest) -> dict:
    fields = req.model_dump()
    fields["postcode"] = fields["postcode"].replace(" ", "").upper()
    fields["suffix"] = (fields["suffix"] or "").strip() or None
    try:
        return run(Namespace(**fields, out=None, with_geometry=True))
    except ValueError as e:
        raise HTTPException(404, str(e))
    except requests.RequestException as e:
        logger.exception("Upstream source failed")
        raise HTTPException(502, f"An open-data source did not answer: {e}")


@app.post("/api/listing")
async def parse_listing(file: UploadFile = File(...)) -> dict:
    data = await file.read(MAX_UPLOAD_BYTES + 1)
    if len(data) > MAX_UPLOAD_BYTES:
        raise HTTPException(413, "The PDF is larger than 15 MB")
    if not data.startswith(b"%PDF"):
        raise HTTPException(415, "The file is not a PDF")
    try:
        fields = listing.parse_listing(data)
    except Exception:
        logger.exception("Could not read the PDF")
        raise HTTPException(422, "The PDF could not be read")
    finally:
        del data
    return {"fields": fields}


@app.get("/api/health")
def health() -> dict:
    return {"status": "ok"}


app.mount("/", StaticFiles(directory=Path(__file__).parent / "static", html=True), name="static")
