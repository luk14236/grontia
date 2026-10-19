"""
Pull the factual fields out of a listing PDF the user uploads (Funda, Pararius
and similar, Dutch or English).

Per the listing data policy the file never touches disk and neither the file
nor its text leaves this module: only the fields below go back to the
browser, where the user confirms them before anything is evaluated.
"""
from __future__ import annotations

import io
import re

from pypdf import PdfReader

MAX_PAGES = 20

POSTCODE = r"(\d{4})\s?([A-Z]{2})"
# "Mgr vdn Tillaartstraat 9 A, 5461 KX Veghel" (also across a line break)
ADDRESS = re.compile(
    r"([A-Z][\w'.\-]*(?: [\w'.\-]+){0,5}?)\s+(\d{1,5})(?:\s*-?\s*([A-Za-z]{1,2}|\d{1,4}))?\s*,?\s*\n?\s*" + POSTCODE
)
EURO = r"€\s?([\d.,]+)"
SALE_PRICE = [
    re.compile(r"(?:Vraagprijs|Koopprijs|Asking price|Price)\D{0,20}?" + EURO, re.I),
    re.compile(EURO + r"\s*(?:k\.k\.|v\.o\.n\.|kosten koper|vrij op naam)", re.I),
]
RENT = [
    re.compile(r"(?:Huurprijs|Huur|Rental price|Rent)\D{0,20}?" + EURO, re.I),
    re.compile(EURO + r"\s*(?:per maand|p/?m|p\.m\.|/\s?maand|/\s?mnd|per month|/\s?month)", re.I),
]
DEPOSIT = re.compile(r"(?:Waarborgsom|Borg|Deposit)\D{0,20}?" + EURO, re.I)
FLOOR_AREA = re.compile(r"(?:Woonoppervlakte|Gebruiksoppervlakte wonen|Wonen|Living area|Floor area)\D{0,25}?(\d{2,4})\s?m[²2]", re.I)
ENERGY_LABEL = re.compile(r"(?:Energielabel|Energy label|Energieklasse)\s*:?\s*([A-G](?:\+{1,4})?)(?![a-z])", re.I)


def _amount(text: str) -> float | None:
    """Dutch and English money notation: 400.000 / 1.250,00 / 1,250.00 / 1250"""
    t = text.strip(".,")
    if re.fullmatch(r"\d{1,3}(\.\d{3})+(,\d{1,2})?", t):
        t = t.replace(".", "").replace(",", ".")
    elif re.fullmatch(r"\d{1,3}(,\d{3})+(\.\d{1,2})?", t):
        t = t.replace(",", "")
    else:
        t = t.replace(",", ".")
    try:
        return float(t)
    except ValueError:
        return None


def _first(patterns, text: str) -> float | None:
    for p in patterns if isinstance(patterns, list) else [patterns]:
        for m in p.finditer(text):
            value = _amount(m.group(1))
            if value:
                return value
    return None


def extract_text(pdf_bytes: bytes) -> str:
    reader = PdfReader(io.BytesIO(pdf_bytes))
    return "\n".join((page.extract_text() or "") for page in reader.pages[:MAX_PAGES])


def extract_fields(text: str) -> dict:
    fields: dict = {}
    if m := ADDRESS.search(text):
        street, number, suffix, pc4, pc2 = m.groups()
        fields.update(street=street.strip(), number=int(number), postcode=f"{pc4}{pc2}")
        if suffix:
            fields["suffix"] = suffix
    elif m := re.search(POSTCODE, text):
        fields["postcode"] = "".join(m.groups())

    rent = _first(RENT, text)
    price = _first(SALE_PRICE, text)
    # a rental listing also shows "€ 1.500 per maand" next to words like "prijs"
    if rent and (not price or price < 20000):
        fields["listing_type"] = "rent"
        fields["rent"] = rent
        if deposit := _first(DEPOSIT, text):
            fields["deposit"] = deposit
    elif price:
        fields["listing_type"] = "sale"
        fields["asking_price"] = price

    if m := FLOOR_AREA.search(text):
        fields["floor_area_m2"] = int(m.group(1))
    if m := ENERGY_LABEL.search(text):
        fields["energy_label"] = m.group(1).upper()
    return fields


def parse_listing(pdf_bytes: bytes) -> dict:
    return extract_fields(extract_text(pdf_bytes))
