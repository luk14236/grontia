"use strict";

const LOCATIESERVER = "https://api.pdok.nl/bzk/locatieserver/search/v3_1";
const NL_BOUNDS = [[50.75, 3.36], [53.55, 7.23]];
const $ = (sel) => document.querySelector(sel);

let lang = "en";
let lastResult = null;
const t = (key) => I18N[lang][key];

// ---------------------------------------------------------------- language

function initialLang() {
  try {
    const saved = localStorage.getItem("grontia.lang");
    if (saved === "en" || saved === "nl") return saved;
  } catch (e) { /* storage blocked */ }
  return (navigator.language || "").toLowerCase().startsWith("nl") ? "nl" : "en";
}

function applyLang(next) {
  lang = next;
  try { localStorage.setItem("grontia.lang", lang); } catch (e) { /* storage blocked */ }
  document.documentElement.lang = lang;
  document.querySelectorAll("[data-i18n]").forEach((el) => { el.textContent = t(el.dataset.i18n); });
  document.querySelectorAll("[data-i18n-placeholder]").forEach((el) => { el.placeholder = t(el.dataset.i18nPlaceholder); });
  document.querySelectorAll("[data-i18n-aria]").forEach((el) => el.setAttribute("aria-label", t(el.dataset.i18nAria)));
  renderIntro();
  setMode(document.querySelector('input[name="mode"]:checked').value);
  if (lastResult) renderSummary(lastResult);
  document.querySelectorAll(".hint").forEach((h) => h.setAttribute("aria-label", t("hintLabel")));
  if (lastResult && !$("#detail").hidden) renderDetail(lastResult);
  document.querySelectorAll("[data-lang]").forEach((b) => b.setAttribute("aria-pressed", String(b.dataset.lang === lang)));
  if (marker) marker.bindTooltip(t("marker"));
  if (lastResult) renderResult(lastResult);
  renderAbout();
}

const locale = () => (lang === "nl" ? "nl-NL" : "en-GB");
const eur = (n) => new Intl.NumberFormat(locale(), { style: "currency", currency: "EUR", maximumFractionDigits: 0 }).format(n);
const num = (n, d = 0) => new Intl.NumberFormat(locale(), { maximumFractionDigits: d }).format(n);
const pct = (n) => `${n > 0 ? "+" : ""}${num(n, 1)}%`;
const esc = (s) => String(s ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));

// ---------------------------------------------------------------- hints (ⓘ with an explanation on hover, focus or tap)

// key is a dotted path into the language strings, e.g. "hints.postcode"
const lookup = (key) => key.split(".").reduce((o, k) => (o || {})[k], I18N[lang]);
const hintButton = (key) => `<button type="button" class="hint" data-hint="${key}"></button>`;

const tooltip = document.getElementById("tooltip");
let tipFor = null;
function showTip(btn) {
  tipFor = btn;
  tooltip.textContent = lookup(btn.dataset.hint) || "";
  tooltip.hidden = false;
  btn.setAttribute("aria-describedby", "tooltip");
  const r = btn.getBoundingClientRect(), tw = Math.min(300, window.innerWidth - 24);
  tooltip.style.width = `${tw}px`;
  const left = Math.min(Math.max(12, r.left + r.width / 2 - tw / 2), window.innerWidth - tw - 12);
  const below = r.bottom + 8 + tooltip.offsetHeight < window.innerHeight;
  tooltip.style.left = `${left}px`;
  tooltip.style.top = `${below ? r.bottom + 8 : r.top - tooltip.offsetHeight - 8}px`;
}
function hideTip() {
  if (tipFor) tipFor.removeAttribute("aria-describedby");
  tipFor = null;
  tooltip.hidden = true;
}
document.addEventListener("mouseover", (e) => { const b = e.target.closest(".hint"); if (b) showTip(b); });
document.addEventListener("mouseout", (e) => { if (e.target.closest(".hint")) hideTip(); });
document.addEventListener("focusin", (e) => { const b = e.target.closest(".hint"); if (b) showTip(b); });
document.addEventListener("focusout", (e) => { if (e.target.closest(".hint")) hideTip(); });
document.addEventListener("click", (e) => {
  const b = e.target.closest(".hint");
  if (b) {  // touch screens have no hover: a tap toggles the hint (and must not toggle a <details>)
    e.preventDefault(); e.stopPropagation();
    if (tipFor === b) hideTip(); else showTip(b);
  } else if (tipFor) hideTip();
}, true);
document.addEventListener("keydown", (e) => {
  if (e.key !== "Escape") return;
  if (tipFor) hideTip();
  else if (!$("#detail").hidden) toggleDetail(false);
});
document.addEventListener("scroll", () => tipFor && hideTip(), true);

// ---------------------------------------------------------------- map

const map = L.map("map", { zoomControl: false, attributionControl: true }).fitBounds(NL_BOUNDS);
L.control.zoom({ position: "topright" }).addTo(map);
L.tileLayer("https://service.pdok.nl/brt/achtergrondkaart/wmts/v2_0/standaard/EPSG:3857/{z}/{x}/{y}.png", {
  maxZoom: 19, minZoom: 6,
  attribution: 'Kaart © <a href="https://www.kadaster.nl">Kadaster</a> / <a href="https://www.pdok.nl">PDOK</a>',
}).addTo(map);

let marker = null;
let buurtLayer = null;

// keep what we zoom to clear of the panel (left on desktop, bottom sheet on phones)
function panelPadding() {
  const panel = $("#panel").getBoundingClientRect();
  return window.innerWidth > 720
    ? { paddingTopLeft: [panel.right + 24, 24], paddingBottomRight: [24, 24] }
    : { paddingTopLeft: [24, 24], paddingBottomRight: [24, window.innerHeight - panel.top + 24] };
}

function focusAddress(lat, lon) {
  if (marker) marker.setLatLng([lat, lon]);
  else marker = L.circleMarker([lat, lon], { radius: 9, weight: 3, color: "#ffffff", fillColor: "#d71920", fillOpacity: 1 }).addTo(map);
  marker.bindTooltip(t("marker"));
  const d = 0.0012; // ~130 m around the address
  map.flyToBounds([[lat - d, lon - d], [lat + d, lon + d]], { ...panelPadding(), maxZoom: 18, duration: 1.2 });
}

function showBuurt(geometry, lat, lon) {
  if (buurtLayer) buurtLayer.remove();
  if (!geometry) return;
  buurtLayer = L.geoJSON(geometry, { style: { color: "#003da5", weight: 2, fillOpacity: 0.08, dashArray: "4 4" } }).addTo(map);
  if (marker) marker.bringToFront();
  map.flyToBounds(buurtLayer.getBounds().extend([lat, lon]), { ...panelPadding(), maxZoom: 17, duration: 1.2 });
}

// ---------------------------------------------------------------- address search (PDOK Locatieserver)

const search = $("#search");
const list = $("#suggestions");
let suggestTimer = null;
let suggestions = [];
let active = -1;

search.addEventListener("input", () => {
  clearTimeout(suggestTimer);
  const q = search.value.trim();
  if (q.length < 3) return closeSuggestions();
  suggestTimer = setTimeout(() => suggest(q), 250);
});

async function suggest(q) {
  try {
    const url = `${LOCATIESERVER}/suggest?q=${encodeURIComponent(q)}&fq=type:adres&rows=6`;
    const docs = (await (await fetch(url)).json()).response.docs;
    suggestions = docs;
    active = -1;
    list.innerHTML = docs.map((d, i) => `<li role="option" id="sg-${i}" data-i="${i}">${esc(d.weergavenaam)}</li>`).join("");
    list.hidden = docs.length === 0;
    search.setAttribute("aria-expanded", String(!list.hidden));
  } catch (e) { closeSuggestions(); }
}

function closeSuggestions() {
  list.hidden = true;
  search.setAttribute("aria-expanded", "false");
}

search.addEventListener("keydown", (e) => {
  if (list.hidden) return;
  if (e.key === "ArrowDown" || e.key === "ArrowUp") {
    e.preventDefault();
    active = (active + (e.key === "ArrowDown" ? 1 : -1) + suggestions.length) % suggestions.length;
    list.querySelectorAll("li").forEach((li, i) => li.setAttribute("aria-selected", String(i === active)));
    search.setAttribute("aria-activedescendant", `sg-${active}`);
  } else if (e.key === "Enter" && active >= 0) {
    e.preventDefault();
    choose(suggestions[active]);
  } else if (e.key === "Escape") {
    closeSuggestions();
  }
});

list.addEventListener("mousedown", (e) => {
  const li = e.target.closest("li");
  if (li) { e.preventDefault(); choose(suggestions[Number(li.dataset.i)]); }
});
search.addEventListener("blur", () => setTimeout(closeSuggestions, 150));

async function choose(doc) {
  closeSuggestions();
  search.value = doc.weergavenaam;
  const d = (await (await fetch(`${LOCATIESERVER}/lookup?id=${encodeURIComponent(doc.id)}&fl=*`)).json()).response.docs[0];
  if (!d) return;
  $("#postcode").value = d.postcode || "";
  $("#number").value = d.huisnummer || "";
  $("#suffix").value = [d.huisletter, d.huisnummertoevoeging].filter(Boolean).join("");
  placeFromCentroid(d.centroide_ll);
}

function placeFromCentroid(wkt) {
  const m = /POINT\(([-\d.]+) ([-\d.]+)\)/.exec(wkt || "");
  if (m) focusAddress(Number(m[2]), Number(m[1]));
}

// postcode + number typed by hand, or filled from a PDF: still zoom to it
async function geocodeForm() {
  const pc = $("#postcode").value.replace(/\s/g, "").toUpperCase();
  const nr = $("#number").value.trim();
  if (!/^\d{4}[A-Z]{2}$/.test(pc) || !/^\d+$/.test(nr)) return;
  try {
    const url = `${LOCATIESERVER}/free?q=${encodeURIComponent(`postcode:${pc} and huisnummer:${nr}`)}&fq=type:adres&rows=1&fl=centroide_ll,weergavenaam`;
    const d = (await (await fetch(url)).json()).response.docs[0];
    if (d) { placeFromCentroid(d.centroide_ll); if (!search.value) search.value = d.weergavenaam; }
  } catch (e) { /* the map just stays where it is */ }
}
["#postcode", "#number"].forEach((sel) => $(sel).addEventListener("change", geocodeForm));

// ---------------------------------------------------------------- about

const GITHUB = "https://github.com/luk14236/grontia";

function renderAbout() {
  const a = t("about");
  const sections = a.sections.map((sec) => `<section><h2>${esc(sec.h)}</h2>${
    sec.items ? `<ul>${sec.items.map((i) => `<li>${esc(i)}</li>`).join("")}</ul>` : `<p>${esc(sec.p)}</p>`}</section>`).join("");
  $("#about").innerHTML = `
    <h2>${esc(a.title)}</h2>
    <p class="intro">${esc(a.intro)}</p>
    ${sections}
    <p class="meta"><a href="${GITHUB}" target="_blank" rel="noopener">${esc(a.source)}</a> · ${esc(a.credits)}</p>
    <button type="button" class="primary" id="about-back">${esc(t("backBtn"))}</button>`;
  $("#about-back").addEventListener("click", () => toggleAbout(false));
}

function toggleAbout(open) {
  $("#about").hidden = !open;
  $("#tool").hidden = open;
  $("#about-btn").setAttribute("aria-expanded", String(open));
  $("#panel").scrollTop = 0;
}
$("#about-btn").addEventListener("click", () => toggleAbout($("#about").hidden));

// ---------------------------------------------------------------- tabs, mode, language

function selectTab(name) {
  ["address", "pdf"].forEach((n) => {
    $(`#tab-${n}`).setAttribute("aria-selected", String(n === name));
    $(`#pane-${n}`).hidden = n !== name;
  });
}
$("#tab-address").addEventListener("click", () => selectTab("address"));
$("#tab-pdf").addEventListener("click", () => selectTab("pdf"));

function setMode(mode) {
  document.querySelector(`input[name="mode"][value="${mode}"]`).checked = true;
  document.querySelectorAll("[data-mode]").forEach((el) => { el.hidden = el.dataset.mode !== mode; });
  $("#submit").textContent = t(mode === "rent" ? "evaluateRent" : "evaluateSale");
}

// ---------------------------------------------------------------- intro, progress, summary, sheet

function renderIntro() {
  $("#intro-list").innerHTML = t("introItems").map((x) => `<li>${esc(x)}</li>`).join("");
  $("#examples").innerHTML = t("examples").map((x, i) => `<button type="button" class="example" data-i="${i}">${esc(x.label)}</button>`).join("");
  $("#examples").querySelectorAll("button").forEach((b) => b.addEventListener("click", () => {
    const x = t("examples")[Number(b.dataset.i)];
    $("#form").reset();
    $("#postcode").value = x.pc; $("#number").value = x.nr; $("#suffix").value = x.suffix || "";
    setMode(x.mode);
    $(x.mode === "rent" ? "#rent" : "#asking_price").value = x.amount;
    search.value = "";
    $("#form").requestSubmit();
  }));
}

let progressTimer = null;
function showProgress(on) {
  clearInterval(progressTimer);
  $("#progress").hidden = !on;
  if (!on) return;
  let i = 0;
  const step = () => { $("#progress-step").textContent = t("steps")[Math.min(i++, t("steps").length - 1)]; };
  step();
  progressTimer = setInterval(step, 2200);
}

// after a result the form folds into one line, so the result starts at the top
function showSearch(open) {
  $("#search-area").hidden = !open;
  $("#summary-bar").hidden = open || !lastResult;
  $("#panel").classList.toggle("wide", Boolean(lastResult));
}

function renderSummary({ data, mode }) {
  const amt = mode === "rent" ? data.user_inputs.rent : data.user_inputs.asking_price;
  $("#summary-address").textContent = t("summaryDetail")(mode, amt ? eur(amt) : "");
}

$("#edit-search").addEventListener("click", () => {
  showSearch(true);
  $("#intro").hidden = true;
  $("#panel").scrollTop = 0;
  $("#postcode").focus();
});

// phones: the panel is a bottom sheet with three heights; the handle cycles them
const SHEET = ["sheet-half", "sheet-full", "sheet-peek"];
function setSheet(state) {
  SHEET.forEach((c) => $("#panel").classList.toggle(c, c === state));
}
$("#sheet-handle").addEventListener("click", () => {
  const now = SHEET.find((c) => $("#panel").classList.contains(c)) || "sheet-half";
  setSheet(SHEET[(SHEET.indexOf(now) + 1) % SHEET.length]);
});
setSheet("sheet-half");
document.querySelectorAll('input[name="mode"]').forEach((r) => r.addEventListener("change", () => setMode(r.value)));
document.querySelectorAll("[data-lang]").forEach((b) => b.addEventListener("click", () => applyLang(b.dataset.lang)));

// ---------------------------------------------------------------- listing PDF

const drop = $("#drop");
["dragenter", "dragover"].forEach((ev) => drop.addEventListener(ev, (e) => { e.preventDefault(); drop.classList.add("over"); }));
["dragleave", "drop"].forEach((ev) => drop.addEventListener(ev, () => drop.classList.remove("over")));
drop.addEventListener("drop", (e) => { e.preventDefault(); if (e.dataTransfer.files[0]) readPdf(e.dataTransfer.files[0]); });
$("#pdf-input").addEventListener("change", (e) => { if (e.target.files[0]) readPdf(e.target.files[0]); e.target.value = ""; });

async function readPdf(file) {
  const status = $("#pdf-status");
  status.className = "status";
  status.textContent = t("pdfReading");
  const body = new FormData();
  body.append("file", file);
  try {
    const res = await fetch("/api/listing", { method: "POST", body });
    if (res.status === 429) throw new Error(t("tooMany"));
    const data = await res.json();
    if (!res.ok) throw new Error(data.detail || t("errorGeneric"));
    const f = data.fields;
    if (!f.postcode && !f.asking_price && !f.rent) {
      status.className = "status error";
      status.textContent = t("pdfNothing");
      return;
    }
    status.textContent = "";
    $("#form").reset();
    const set = (id, v) => { if (v !== undefined && v !== null) $(id).value = v; };
    set("#postcode", f.postcode); set("#number", f.number); set("#suffix", f.suffix);
    set("#asking_price", f.asking_price); set("#rent", f.rent); set("#deposit", f.deposit);
    set("#energy_label", f.energy_label);
    search.value = f.street && f.number ? `${f.street} ${f.number}${f.suffix || ""}, ${f.postcode || ""}` : "";
    setMode(f.listing_type === "rent" ? "rent" : "sale");
    $("#pdf-banner").hidden = false;
    selectTab("address");
    geocodeForm();
  } catch (e) {
    status.className = "status error";
    status.textContent = e.message;
  }
}

// ---------------------------------------------------------------- evaluate

// "€ 1.250,00", "400.000", "1,250" -> number
function amount(raw) {
  let s = String(raw || "").replace(/[^\d.,]/g, "");
  if (!s) return null;
  if (/^\d{1,3}(\.\d{3})+(,\d{1,2})?$/.test(s)) s = s.replace(/\./g, "").replace(",", ".");
  else if (/^\d{1,3}(,\d{3})+(\.\d{1,2})?$/.test(s)) s = s.replace(/,/g, "");
  else s = s.replace(",", ".");
  const n = Number(s);
  return Number.isFinite(n) && n > 0 ? n : null;
}

$("#form").addEventListener("submit", async (e) => {
  e.preventDefault();
  const status = $("#form-status");
  const postcode = $("#postcode").value.replace(/\s/g, "").toUpperCase();
  const number = parseInt($("#number").value, 10);
  if (!/^\d{4}[A-Z]{2}$/.test(postcode) || !(number > 0)) {
    status.className = "status error";
    status.textContent = t("needAddress");
    return;
  }
  const mode = document.querySelector('input[name="mode"]:checked').value;
  const body = {
    postcode, number,
    suffix: $("#suffix").value.trim() || null,
    asking_price: mode === "sale" ? amount($("#asking_price").value) : null,
    rent: mode === "rent" ? amount($("#rent").value) : null,
    deposit: mode === "rent" ? amount($("#deposit").value) : null,
    energy_label: $("#energy_label").value.trim().toUpperCase() || null,
    woz: amount($("#woz").value),
    woz_year: parseInt($("#woz_year").value, 10) || null,
    comparable_price: amount($("#comparable_price").value),
    comparable_date: $("#comparable_date").value.trim() || null,
    comparable_m2: amount($("#comparable_m2").value),
  };
  $("#submit").disabled = true;
  status.className = "status";
  status.textContent = "";
  $("#intro").hidden = true;
  toggleDetail(false);
  showProgress(true);
  try {
    const res = await fetch("/api/evaluate", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) });
    if (res.status === 429) throw new Error(t("tooMany"));
    const data = await res.json();
    if (!res.ok) throw new Error(typeof data.detail === "string" ? data.detail : t("errorGeneric"));
    status.textContent = "";
    lastResult = { data, mode, news: null };
    renderSummary(lastResult);
    showSearch(false);
    setSheet("sheet-full");
    renderResult(lastResult);
    lastResult.newsLoaded = loadNews(lastResult);
    $("#panel").scrollTop = 0;
    // let the wider panel settle before zooming so the address clears it
    setTimeout(() => focusAddress(data.address.lat, data.address.lon), 250);
    setTimeout(() => showBuurt(data.neighbourhood.buurt_geometry, data.address.lat, data.address.lon), 1500);
  } catch (err) {
    status.className = "status error";
    status.textContent = err.message || t("errorGeneric");
  } finally {
    showProgress(false);
    $("#submit").disabled = false;
  }
});

// ---------------------------------------------------------------- result

function verdictCard(kind, title, badge, headline, details, extra = "") {
  return `<article class="verdict ${kind}">
    <h3>${esc(title)}</h3>
    <span class="badge">${esc(badge)}</span>
    <p class="headline">${esc(headline)}</p>
    ${extra}
    ${details.filter(Boolean).map((d) => `<p class="detail">${esc(d)}</p>`).join("")}
  </article>`;
}

function rangeScale(range, asked) {
  const values = [range.low, range.high, range.central, asked].filter((v) => v);
  const lo = Math.min(...values) * 0.96;
  const hi = Math.max(...values) * 1.04;
  const x = (v) => `${(100 * (v - lo)) / (hi - lo)}%`;
  // keep the label inside the scale when its value sits near either end
  const lbl = (v) => {
    const p = (100 * (v - lo)) / (hi - lo);
    const shift = p < 15 ? "0" : p > 85 ? "-100%" : "-50%";
    return `<span class="lbl" style="left:${x(v)};transform:translateX(${shift})">${eur(v)}</span>`;
  };
  return `<div class="scale" aria-hidden="true">
    <div class="track"></div>
    <div class="band" style="left:${x(range.low)};width:calc(${x(range.high)} - ${x(range.low)})"></div>
    <div class="tick central" style="left:${x(range.central)}"></div>
    ${asked ? `<div class="tick asked" style="left:${x(asked)}"></div>${lbl(asked)}` : lbl(range.central)}
  </div>`;
}

function purchaseCard(p) {
  const r = p.range;
  const basis = t("basis")(t("basisName")[p.central_basis] || p.central_basis, eur(p.price_per_m2_central));
  const check = p.asking_price_check;
  if (!check) {
    return verdictCard("info", t("valueTitle"), t("badge").estimate, t("valueOnly")(eur(r.central), eur(r.low), eur(r.high)), [basis], rangeScale(r));
  }
  const kind = { in_range: "ok", below_range: "ok", above_range: "bad" }[check.verdict];
  // "above"/"below" already carry the direction, so only in_range shows the sign
  const diff = check.verdict === "in_range" ? pct(check.vs_central_pct) : `${num(Math.abs(check.vs_central_pct), 1)}%`;
  const headline = { in_range: t("priceIn"), above_range: t("priceAbove"), below_range: t("priceBelow") }[check.verdict](diff);
  return verdictCard(kind, t("purchaseTitle"), t("badge")[check.verdict], headline,
    [t("priceRange")(eur(r.low), eur(r.high)), basis], rangeScale(r, check.asking_price) + caption("info.charts.scale"));
}

function rentCard(rent, outlook) {
  const s = rent.scenarios;
  const market = outlook ? t("rentMarket")(eur(outlook.market_rent.low), eur(outlook.market_rent.high)) : "";
  const typ = s.typical;
  const pts = t("rentPoints")(typ.total_points, t("segment")[typ.segment] || typ.segment);
  // a free-sector scenario has no legal maximum: its table price must not read as one
  const cap = eur((rent.segment_limits || {}).middle_max_rent || typ.max_rent_eur);
  const borderline = ["low", "typical", "high"].some((k) => s[k].segment === "free") && s.low.segment !== "free";
  const scen = typ.segment === "free" ? t("rentScenariosFree")(typ.total_points, cap)
    : s.high.segment === "free" ? t("rentScenariosHighFree")(eur(s.low.max_rent_eur), eur(typ.max_rent_eur))
    : t("rentScenarios")(eur(s.low.max_rent_eur), eur(typ.max_rent_eur), eur(s.high.max_rent_eur));
  const c = rent.rent_check;
  if (!c) {
    const free = typ.segment === "free";
    return verdictCard("info", t("legalRentTitle"), t("badge")[free ? "free_sector" : "estimate"], free ? t("rentFree") : scen, [free ? scen : "", pts, market]);
  }
  const kind = { within_limit: "ok", above_limit: "bad", free_sector: "info", depends_on_details: "warn" }[c.verdict];
  const headline = {
    within_limit: () => t("rentWithin")(eur(c.max_rent_typical)),
    above_limit: () => t("rentAbove")(eur(c.difference_eur), eur(c.max_rent_typical)),
    free_sector: () => t("rentFree"),
    depends_on_details: () => borderline ? t("rentBorderline")(s.low.total_points, s.high.total_points, cap)
      : t("rentDepends")(eur(s.low.max_rent_eur), eur(s.high.max_rent_eur)),
  }[c.verdict]();
  const deposit = c.deposit_ok === undefined ? "" : (c.deposit_ok ? t("depositOk") : t("depositHigh"))(c.deposit_max_months);
  return verdictCard(kind, t("rentTitle"), t("badge")[c.verdict], headline, [scen, pts, market, deposit]);
}

// ---------------------------------------------------------------- small SVG charts

const W = 340, H = 140, PAD = { l: 46, r: 16, t: 10, b: 22 };

// lines: [{ values: [...], cls: "base" }], all the same length as xLabels
function lineChart(lines, xLabels, fmt) {
  const all = lines.flatMap((l) => l.values).filter((v) => v != null);
  let lo = Math.min(...all), hi = Math.max(...all);
  const span = hi - lo || hi || 1;
  lo -= span * 0.08; hi += span * 0.08;
  const x = (i) => PAD.l + (i * (W - PAD.l - PAD.r)) / Math.max(1, xLabels.length - 1);
  const y = (v) => PAD.t + ((hi - v) * (H - PAD.t - PAD.b)) / (hi - lo);
  const grid = [0, 0.5, 1].map((f) => lo + f * (hi - lo)).map((v) =>
    `<line class="grid" x1="${PAD.l}" x2="${W - PAD.r}" y1="${y(v)}" y2="${y(v)}"/><text class="axis" x="${PAD.l - 6}" y="${y(v) + 4}" text-anchor="end">${esc(fmt(v))}</text>`).join("");
  const ticks = xLabels.map((l, i) => (i === 0 || i === xLabels.length - 1 || i === Math.floor(xLabels.length / 2))
    ? `<text class="axis" x="${x(i)}" y="${H - 6}" text-anchor="${i === 0 ? "start" : i === xLabels.length - 1 ? "end" : "middle"}">${esc(l)}</text>` : "").join("");
  const paths = lines.map((l) => {
    const pts = l.values.map((v, i) => (v == null ? null : `${x(i)},${y(v)}`)).filter(Boolean);
    const last = l.values.length - 1;
    return `<polyline class="line ${l.cls}" points="${pts.join(" ")}"/><circle class="dot ${l.cls}" cx="${x(last)}" cy="${y(l.values[last])}" r="3"/>`;
  }).join("");
  return `<svg class="chart" viewBox="0 0 ${W} ${H}" role="img">${grid}${ticks}${paths}</svg>`;
}

function barChart(values, labels, fmt, highlightLast = false) {
  const hi = Math.max(...values) * 1.1 || 1;
  const bw = (W - PAD.l - PAD.r) / values.length;
  const y = (v) => PAD.t + ((hi - v) * (H - PAD.t - PAD.b)) / hi;
  const bars = values.map((v, i) => `<rect class="bar${highlightLast && i === values.length - 1 ? " last" : ""}" x="${PAD.l + i * bw + 2}" y="${y(v)}" width="${bw - 4}" height="${H - PAD.b - y(v)}"/>`).join("");
  const ticks = labels.map((l, i) => (i === 0 || i === labels.length - 1 || i === Math.floor(labels.length / 2))
    ? `<text class="axis" x="${PAD.l + i * bw + bw / 2}" y="${H - 6}" text-anchor="middle">${esc(l)}</text>` : "").join("");
  const grid = [0, 0.5, 1].map((f) => f * hi).map((v) =>
    `<line class="grid" x1="${PAD.l}" x2="${W - PAD.r}" y1="${y(v)}" y2="${y(v)}"/><text class="axis" x="${PAD.l - 6}" y="${y(v) + 4}" text-anchor="end">${esc(fmt(v))}</text>`).join("");
  return `<svg class="chart" viewBox="0 0 ${W} ${H}" role="img">${grid}${bars}${ticks}</svg>`;
}

const kEur = (v) => (v >= 1e6 ? `€${num(v / 1e6, 2)}M` : `€${num(v / 1000)}k`);
const signed = (v, d = 1) => `${v > 0 ? "+" : ""}${num(v, d)}`;

function factGrid(rows) {
  return `<div class="facts">${rows.filter(Boolean).map(([k, v, s, hint]) =>
    `<div class="fact"><span>${esc(k)}${hint ? hintButton(hint) : ""}</span><b>${esc(v)}</b>${s ? `<small>${esc(s)}</small>` : ""}</div>`).join("")}</div>`;
}

// "How to read this" under every chart: visible on screen and in the PDF
const caption = (key) => `<p class="caption"><b>${esc(t("howToRead"))}:</b> ${esc(lookup(key))}</p>`;

// ---------------------------------------------------------------- result sections

function outlookBody(o) {
  if (!o) return "";
  const g = o.growth_pct_per_year;
  const year = new Date().getFullYear();
  const years = Array.from({ length: o.years + 1 }, (_, i) => String(year + i));
  const chart = lineChart(["low", "base", "high"].map((k) => ({ values: o.value_paths[k], cls: k })), years, kEur);
  const legend = ["low", "base", "high"].map((k) =>
    `<span class="key ${k}">${esc(t("scenario")[k])} ${num(g[k], 1)}% ${esc(t("perYear"))}</span>`).join("");
  const table = (title, r, note) => `<div class="returns"><h4>${esc(title)}</h4>
    <table><thead><tr><th></th>${["low", "base", "high"].map((k) => `<th>${esc(t("scenario")[k])}</th>`).join("")}</tr></thead>
    <tbody><tr><td>${esc(t("valueIn")(year + o.years))}${hintButton("info.returns.value")}</td>${["low", "base", "high"].map((k) => `<td>${kEur(r.scenarios[k].value_after)}</td>`).join("")}</tr>
    <tr><td>${esc(t("totalReturn"))}${hintButton("info.returns.total")}</td>${["low", "base", "high"].map((k) => {
      const s = r.scenarios[k];
      return `<td class="${s.total_return < 0 ? "neg" : "pos"}">${s.total_return < 0 ? "−" : "+"}${kEur(Math.abs(s.total_return))}<small>${signed(s.total_return_pct)}%</small></td>`;
    }).join("")}</tr></tbody></table><p class="detail">${esc(note)}</p></div>`;
  const landlordNote = t("landlordNote")(eur(o.landlord.transfer_tax), eur(o.landlord_monthly_rent), `${num(o.gross_yield_pct, 1)}%`)
    + (o.rent_regulated ? " " + t("regulatedNote")(eur(o.legal_max_rent)) : "");
  // two columns on wide screens: the chart and its reading on the left, the returns on the right
  return `<div class="split">
    <div><p class="detail">${esc(t("outlookIntro")(eur(o.price), t("priceBasis")[o.price_basis]))}</p>
      ${chart}<div class="legend">${legend}</div>${caption("info.charts.outlook")}
      ${g.latest_quarter_yoy != null && g.trend ? `<p class="detail">${esc(t("growthNote")(signed(g.latest_quarter_yoy) + "%", t("trend")[g.trend]))}</p>` : ""}</div>
    <div>${table(t("ownerTitle"), o.owner_occupier, t("ownerNote")(eur(o.owner_occupier.transfer_tax), eur(o.starter_exemption_max_value)))}
      ${table(t("landlordTitle"), o.landlord, landlordNote)}
      <p class="detail">${esc(t("excludedNote"))}</p></div></div>`;
}

function overviewBody(d) {
  const a = d.address;
  const kf = d.neighbourhood.key_figures || {};
  const b = kf[a.buurt_code] || {};
  const gm = kf[a.municipality_code] || {};
  const sizes = d.neighbourhood.dwelling_sizes || {};
  const ap = d.neighbourhood.area_properties || {};
  const dist = [["gp", b.km_to_gp], ["supermarket", b.km_to_supermarket], ["school", b.km_to_school], ["daycare", b.km_to_daycare]]
    .filter(([, v]) => v != null).map(([k, v]) => `${t(k)} ${num(v, 1)} km`).join(" · ");
  const rows = [
    b.population && [t("population"), num(b.population), ap.gemiddelde_huishoudsgrootte ? t("householdSize")(num(ap.gemiddelde_huishoudsgrootte, 1)) : "", "info.facts.population"],
    b.avg_income_per_resident_eur && [t("income"), eur(b.avg_income_per_resident_eur), gm.avg_income_per_resident_eur ? t("vsMunicipality")(eur(gm.avg_income_per_resident_eur)) : "", "info.facts.income"],
    b.avg_woz_value_eur && [t("avgWoz"), eur(b.avg_woz_value_eur), gm.avg_woz_value_eur ? t("vsMunicipality")(eur(gm.avg_woz_value_eur)) : "", "info.facts.avgWoz"],
    b.dwellings && [t("homes"), num(b.dwellings), b.pct_apartments != null ? t("apartments")(num(b.pct_apartments)) : "", "info.facts.homes"],
    b.pct_owner_occupied != null && [t("tenure"), `${num(b.pct_owner_occupied)}% / ${num(b.pct_rental)}%`, b.pct_social_housing != null ? t("social")(num(b.pct_social_housing)) : "", "info.facts.tenure"],
    b.pct_built_last_10_years != null && [t("newBuild"), `${num(b.pct_built_last_10_years)}%`, b.pct_vacant != null ? t("vacant")(num(b.pct_vacant)) : "", "info.facts.newBuild"],
    sizes.median_m2 && [t("homeSize"), `${num(sizes.median_m2)} m²`, t("homeSizeValue")(num(sizes.median_m2), num(sizes.mean_m2)), "info.facts.homeSize"],
    b.urbanity_class && [t("urbanity"), t("urbanityClass")[b.urbanity_class] || String(b.urbanity_class), "", "info.facts.urbanity"],
    d.dwelling.floor_area_m2 && [t("floorArea"), `${num(d.dwelling.floor_area_m2)} m²`, d.dwelling.build_year ? `${t("buildYear")} ${d.dwelling.build_year}` : "", "info.facts.floorArea"],
  ];
  const body = factGrid(rows) + (dist ? `<p class="detail"><b>${esc(t("distances"))}:</b> ${esc(dist)}</p>` : "");
  return body;
}

function livabilityBody(d) {
  const series = (d.neighbourhood.livability || {})[d.address.buurt_code] || [];
  const liv = series.slice(-1)[0];
  if (!liv) return "";
  const dims = Object.keys(t("dims"));
  const maxAbs = Math.max(0.05, ...dims.map((k) => Math.abs(liv[k] || 0)));
  const bars = dims.map((k) => {
    const v = liv[k] || 0;
    const w = (50 * Math.abs(v)) / maxAbs;
    return `<div class="dim"><span>${esc(t("dims")[k])}${hintButton(`info.dims.${k}`)}</span><div class="dim-track"><div class="dim-bar ${v < 0 ? "neg" : "pos"}" style="${v < 0 ? `right:50%` : `left:50%`};width:${w}%"></div></div></div>`;
  }).join("");
  const trendLine = liv.change_since_previous != null && liv.change_since_2014 != null
    ? `<p class="detail">${esc(t("livabilityTrend")(signed(liv.change_since_previous, 3), signed(liv.change_since_2014, 3)))}${hintButton("info.livTrend")}</p>` : "";
  const glossary = `<h4>${esc(t("dimsTitle"))}</h4><dl class="glossary">${dims.map((k) =>
    `<div><dt>${esc(t("dims")[k])}</dt><dd>${esc(t("info").dims[k])}</dd></div>`).join("")}</dl>`;
  const body = factGrid([[t("livabilityTitle"), `${num(liv.livability_score, 2)}`, t("livabilityValue")(Math.round(liv.national_percentile)), "info.livScore"]])
    + trendLine + `<div class="dims">${bars}</div>${caption("info.charts.dims")}` + glossary;
  return body;
}

function safetyBody(d) {
  const crime = d.neighbourhood.crime || {};
  if (crime.error || !crime.by_region) return "";
  const a = d.address;
  const by = crime.by_region;
  const here = by[a.buurt_code] || {};
  const rate = (c) => (by[c] && by[c].per_1000_residents_last_12m != null ? num(by[c].per_1000_residents_last_12m, 1) : "–");
  const rows = [[t("here"), rate(a.buurt_code), here.change_pct != null ? t("crimeChange")(signed(here.change_pct) + "%") : ""],
                [t("municipality"), rate(a.municipality_code), ""], [t("netherlands"), rate("NL00"), ""]];
  const thisYear = String(new Date().getFullYear());
  const yearly = Object.entries(here.yearly || {}).filter(([y]) => y !== thisYear);  // current year is partial
  const chart = yearly.length > 2 ? barChart(yearly.map(([, v]) => v), yearly.map(([y]) => y), (v) => num(v)) : "";
  const types = (crime.top_types_last_12m || []).slice(0, 5)
    .map((x) => `<li>${esc(x.type.replace(/^[\d.]+\s*/, ""))} <span class="count">${num(x.count)}</span></li>`).join("");
  const body = `<p class="detail">${esc(t("crimeRate"))}${hintButton("info.facts.crimeRate")}</p>${factGrid(rows)}${chart ? chart + caption("info.charts.safetyYears") : ""}
    ${types ? `<h4>${esc(t("crimeTypes"))}${hintButton("info.facts.crimeTypes")}</h4><ul class="types">${types}</ul>` : ""}
    <p class="detail">${esc(t("crimeCentreNote"))}</p>`;
  return body;
}

function pricesBody(d) {
  const ap = d.purchase.appreciation || {};
  const sales = ap.municipality_avg_sale_price || [];
  if (sales.length < 3) return "";
  const chart = lineChart([{ values: sales.map((x) => x.avg_sale_price_eur), cls: "base" }], sales.map((x) => String(x.year)), kEur);
  const q = (ap.province_index_recent_quarters || [])[0];
  const body = `<p class="detail">${esc(t("pricesAvg"))} (${esc(d.address.municipality_name)})</p>${chart}${caption("info.charts.salePrices")}
    ${ap.municipality_avg_sale_price_cagr_5y_pct != null ? `<p class="detail">${esc(t("pricesCagr")(signed(ap.municipality_avg_sale_price_cagr_5y_pct) + "%"))}</p>` : ""}
    ${q ? `<p class="detail">${esc(t("pricesQuarter")(q.period_code.replace("KW", " Q"), signed(q.price_index_yoy_pct) + "%"))}</p>` : ""}`;
  return body;
}

function newsBody(news, limits = { notices: 8, headlines: 6 }) {
  if (!news) return `<p class="detail">${esc(t("newsLoading"))}</p>`;
  const fmtDate = (s) => (s ? new Date(s).toLocaleDateString(locale(), { day: "numeric", month: "short", year: "numeric" }) : "");
  const link = (url, text) => (url ? `<a href="${esc(url)}" target="_blank" rel="noopener">${esc(text)}</a>` : esc(text));
  const n = news.official_notices || {};
  let notices;
  if (n.error) notices = `<p class="detail">${esc(t("newsUnavailable"))}</p>`;
  else {
    const items = (n.items || []).slice(0, limits.notices);
    notices = items.length
      ? `<ul class="news">${items.map((x) => `<li>${link(x.url, x.title)}<small>${esc(fmtDate(x.date))} · ${esc(t("scope")[x.scope] || x.scope)}</small></li>`).join("")}</ul>`
      : `<p class="detail">${esc(t("noticesNone"))}</p>`;
    if (n.complete === false) notices += `<p class="detail">${esc(t("noticesPartial")(fmtDate(n.searched_back_to)))}</p>`;
  }
  const h = news.headlines;
  const headlines = h && h.error ? `<p class="detail">${esc(t("newsUnavailable"))}</p>`
    : (h || []).length ? `<ul class="news">${h.slice(0, limits.headlines).map((x) => `<li>${link(x.url, x.title)}<small>${esc(x.source || "")} · ${esc(fmtDate(x.date))}</small></li>`).join("")}</ul>`
    : `<p class="detail">${esc(t("headlinesNone"))}</p>`;
  // two scrolling columns, so a city with dozens of notices doesn't stretch the page
  return `<div class="split news-cols">
    <div><h4>${esc(t("noticesTitle"))}</h4><div class="scroll-box">${notices}</div></div>
    <div><h4>${esc(t("headlinesTitle"))}</h4><div class="scroll-box">${headlines}</div></div></div>`;
}

async function loadNews(state) {
  const a = state.data.address;
  const q = new URLSearchParams({ municipality: a.municipality_name, postcode: a.postcode, street: a.street, place: a.place });
  try {
    const res = await fetch(`/api/news?${q}`);
    state.news = res.ok ? await res.json() : { official_notices: { error: res.status }, headlines: { error: res.status } };
  } catch (e) {
    state.news = { official_notices: { error: "network" }, headlines: { error: "network" } };
  }
  if (lastResult === state) {
    const old = document.getElementById("d-news");
    if (old) old.outerHTML = cardHtml({ id: "news", title: t("newsTitle"), body: newsBody(state.news, { notices: 40, headlines: 15 }) });
  }
}

// four headline numbers with a state colour, before any detail
function glance(d) {
  const G = t("glance"), a = d.address;
  const tiles = [];
  const check = d.purchase.asking_price_check;
  tiles.push(["value", eur(d.purchase.range.central),
    check ? t("badge")[check.verdict] : `${eur(d.purchase.price_per_m2_central)}/m²`,
    check ? (check.verdict === "above_range" ? "bad" : "ok") : "neutral"]);
  const typ = d.rent.scenarios.typical, rc = d.rent.rent_check;
  tiles.push(["rent", typ.segment === "free" ? t("badge").free_sector : eur(typ.max_rent_eur),
    rc ? t("badge")[rc.verdict] : `${G.perMonth} · ${t("segment")[typ.segment]}`,
    rc ? ({ within_limit: "ok", above_limit: "bad", depends_on_details: "warn" }[rc.verdict] || "neutral") : "neutral"]);
  const liv = ((d.neighbourhood.livability || {})[a.buurt_code] || []).slice(-1)[0];
  if (liv && liv.national_percentile != null) {
    const p = Math.round(liv.national_percentile);
    tiles.push(["livability", `${p}/100`, G.percentile(Math.min(p, 99)), p >= 60 ? "ok" : p < 40 ? "bad" : "neutral"]);
  }
  const by = (d.neighbourhood.crime || {}).by_region || {};
  const here = (by[a.buurt_code] || {}).per_1000_residents_last_12m, nl = (by.NL00 || {}).per_1000_residents_last_12m;
  if (here != null && nl) {
    const x = here / nl;
    tiles.push(["crime", num(here, 0), `${G.per1000} · ${G.vsNl(num(x, 1))}`, x <= 1 ? "ok" : x > 2 ? "bad" : "warn"]);
  }
  return `<div class="glance">${tiles.map(([k, v, sub, st]) =>
    `<div class="tile ${st}"><span>${esc(G[k])}${hintButton(`glanceHints.${k}`)}</span><b>${esc(v)}</b><small>${esc(sub)}</small></div>`).join("")}</div>`;
}

function renderResult({ data, mode }) {
  const a = data.address;
  const primary = mode === "rent" ? rentCard(data.rent, data.outlook) : purchaseCard(data.purchase);
  // BAG "woonfunctie" = residential use; offices, shops etc. get a warning
  const use = data.dwelling.use || "";
  const notAHome = use && !use.split(",").includes("woonfunctie")
    ? `<div class="banner warn" role="alert">${esc(t("notAHome")(use))}</div>` : "";
  $("#result").innerHTML = `
    <div class="where"><div><h2>${esc(a.display_name)}</h2><p>${esc([a.buurt_name, a.municipality_name, a.province_name].filter(Boolean).join(" · "))}</p></div></div>
    ${glance(data)}
    ${notAHome}
    ${primary}
    <div class="result-actions">
      <button type="button" class="primary" id="open-detail">${esc(t("openDetail"))}</button>
      <button type="button" class="secondary" id="export-pdf">${esc(t("exportPdf"))}</button>
    </div>`;
  $("#result").hidden = false;
  document.querySelectorAll("#result .hint").forEach((h) => h.setAttribute("aria-label", t("hintLabel")));
  $("#open-detail").addEventListener("click", () => toggleDetail(true));
  $("#export-pdf").addEventListener("click", () => exportReport(lastResult));
}

// ---------------------------------------------------------------- start

applyLang(initialLang());
setMode("sale");
