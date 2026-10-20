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
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
from pathlib import Path
from urllib.parse import urlparse

import requests
from fastapi import Depends, FastAPI, File, HTTPException, Request, UploadFile
from fastapi.responses import Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from evaluation import sources
from evaluation.__main__ import _optional, run
from . import listing

logger = logging.getLogger("grontia.web")

MAX_UPLOAD_BYTES = 15 * 1024 * 1024
SOURCE_NAMES = {"pdok.nl": "PDOK (the Dutch address and building register)", "cbs.nl": "CBS (Statistics Netherlands)",
                "overheid.nl": "The official notices service", "google.com": "The news service"}
RATE_LIMIT = int(os.getenv("GRONTIA_RATE_LIMIT", "10"))
RATE_WINDOW_S = 600
PASSWORD = os.getenv("GRONTIA_WEB_PASSWORD")
# Paging through official notices stops after this many seconds (Heroku cuts
# requests at 30 s; Amsterdam alone publishes ~30k notices in six months)
NEWS_BUDGET_S = float(os.getenv("GRONTIA_NEWS_BUDGET_S", "14"))
NEWS_MONTHS = 6

app = FastAPI(title="Grontia", docs_url="/api/docs", openapi_url="/api/openapi.json")

_hits: dict[tuple[str, str], deque] = defaultdict(deque)
_hits_lock = threading.Lock()


def client_ip(request: Request) -> str:
    # Heroku's router appends the real client address last; earlier entries
    # can be sent by the client itself
    forwarded = request.headers.get("x-forwarded-for")
    return forwarded.split(",")[-1].strip() if forwarded else request.client.host


def rate_limiter(bucket: str):
    """Each check costs MotherDuck compute and calls to public APIs. Buckets keep
    the news request that follows every check from eating the check budget."""
    def rate_limit(request: Request) -> None:
        _count(client_ip(request), bucket)
    return rate_limit


def _count(ip: str, bucket: str) -> None:
    now = time.monotonic()
    with _hits_lock:
        hits = _hits[(ip, bucket)]
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


@app.post("/api/evaluate", dependencies=[Depends(rate_limiter("check"))])
def evaluate(req: EvaluateRequest) -> dict:
    fields = req.model_dump()
    fields["postcode"] = fields["postcode"].replace(" ", "").upper()
    fields["suffix"] = (fields["suffix"] or "").strip() or None
    try:
        # news (official notices, headlines) is not shown on the site, and in a
        # big city the notices search alone outlasts Heroku's 30 s limit
        return run(Namespace(**fields, out=None, with_geometry=True, with_news=False))
    except ValueError as e:
        raise HTTPException(404, str(e))
    except requests.RequestException as e:
        logger.exception("Upstream source failed")
        # name the source, not the URL: the page shows this message to the visitor
        host = urlparse(getattr(e.request, "url", "") or "").hostname or ""
        source = next((name for key, name in SOURCE_NAMES.items() if key in host), "An open-data source")
        raise HTTPException(502, f"{source} is not answering right now. Try again in a minute.")


@app.post("/api/listing", dependencies=[Depends(rate_limiter("check"))])
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


@app.get("/api/news", dependencies=[Depends(rate_limiter("news"))])
def news(municipality: str, postcode: str, street: str, place: str) -> dict:
    """Official notices near the address and recent headlines for the place,
    loaded by the page after the check itself so the verdict isn't kept waiting."""
    since = (date.today() - timedelta(days=30 * NEWS_MONTHS)).isoformat()
    postcode = postcode.replace(" ", "").upper()
    with ThreadPoolExecutor(max_workers=2) as pool:
        notices = pool.submit(_optional, "official notices", sources.official_notices_within,
                              municipality, postcode, street, since, NEWS_BUDGET_S)
        headlines = pool.submit(_optional, "headlines", sources.news_headlines, place)
        return {"official_notices": notices.result(), "headlines": headlines.result(), "since": since}


@app.get("/api/health")
def health() -> dict:
    return {"status": "ok"}


app.mount("/", StaticFiles(directory=Path(__file__).parent / "static", html=True), name="static")
