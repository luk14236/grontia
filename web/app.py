"""
Grontia web app: evaluate an address, or a listing PDF the user uploads, and
say whether the asking price or rent is in line with the open data.

    uvicorn web.app:app --host 0.0.0.0 --port 8000   (from /opt/grontia)

Nothing the user sends is written anywhere: the PDF is parsed in memory and
the evaluation goes straight back to the browser (see the listing data policy).

Public deployment (Heroku) settings, all optional:
    GRONTIA_RATE_LIMIT      checks per visitor per 10 minutes (default 10)
    GRONTIA_WEB_PASSWORD    when set, the whole site asks for this password
"""
from __future__ import annotations

import base64
import logging
import os
import secrets
import threading
import time
from argparse import Namespace
from collections import defaultdict, deque
from pathlib import Path

import requests
from fastapi import Depends, FastAPI, File, HTTPException, Request, UploadFile
from fastapi.responses import Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from evaluation.__main__ import run
from . import listing

logger = logging.getLogger("grontia.web")

MAX_UPLOAD_BYTES = 15 * 1024 * 1024
RATE_LIMIT = int(os.getenv("GRONTIA_RATE_LIMIT", "10"))
RATE_WINDOW_S = 600
PASSWORD = os.getenv("GRONTIA_WEB_PASSWORD")

app = FastAPI(title="Grontia", docs_url="/api/docs", openapi_url="/api/openapi.json")

_hits: dict[str, deque] = defaultdict(deque)
_hits_lock = threading.Lock()


def client_ip(request: Request) -> str:
    # Heroku's router appends the real client address last; earlier entries
    # can be sent by the client itself
    forwarded = request.headers.get("x-forwarded-for")
    return forwarded.split(",")[-1].strip() if forwarded else request.client.host


def rate_limit(request: Request) -> None:
    """Each check costs MotherDuck compute and calls to public APIs."""
    ip, now = client_ip(request), time.monotonic()
    with _hits_lock:
        hits = _hits[ip]
        while hits and now - hits[0] > RATE_WINDOW_S:
            hits.popleft()
        if len(hits) >= RATE_LIMIT:
            retry = int(RATE_WINDOW_S - (now - hits[0])) + 1
            raise HTTPException(429, "Too many checks from your connection", headers={"Retry-After": str(retry)})
        hits.append(now)
        for stale in [k for k, v in _hits.items() if not v]:
            del _hits[stale]


@app.middleware("http")
async def password_gate(request: Request, call_next):
    if PASSWORD and request.url.path != "/api/health":
        user_pass = ""
        auth = request.headers.get("authorization", "")
        if auth.lower().startswith("basic "):
            try:
                user_pass = base64.b64decode(auth[6:]).decode()
            except ValueError:
                pass
        if not secrets.compare_digest(user_pass.partition(":")[2], PASSWORD):
            return Response(status_code=401, headers={"WWW-Authenticate": 'Basic realm="Grontia"'})
    return await call_next(request)


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


@app.post("/api/evaluate", dependencies=[Depends(rate_limit)])
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


@app.post("/api/listing", dependencies=[Depends(rate_limit)])
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
