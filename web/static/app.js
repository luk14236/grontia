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
    if (saved === "en" || saved === "pt") return saved;
  } catch (e) { /* storage blocked */ }
  return (navigator.language || "").toLowerCase().startsWith("pt") ? "pt" : "en";
}

function applyLang(next) {
  lang = next;
  try { localStorage.setItem("grontia.lang", lang); } catch (e) { /* storage blocked */ }
  document.documentElement.lang = lang === "pt" ? "pt-BR" : "en";
  document.querySelectorAll("[data-i18n]").forEach((el) => { el.textContent = t(el.dataset.i18n); });
  document.querySelectorAll("[data-i18n-placeholder]").forEach((el) => { el.placeholder = t(el.dataset.i18nPlaceholder); });
  document.querySelectorAll("[data-lang]").forEach((b) => b.setAttribute("aria-pressed", String(b.dataset.lang === lang)));
  if (marker) marker.bindTooltip(t("marker"));
  if (lastResult) renderResult(lastResult);
  renderAbout();
}

const locale = () => (lang === "pt" ? "pt-BR" : "en-GB");
const eur = (n) => new Intl.NumberFormat(locale(), { style: "currency", currency: "EUR", maximumFractionDigits: 0 }).format(n);
const num = (n, d = 0) => new Intl.NumberFormat(locale(), { maximumFractionDigits: d }).format(n);
const pct = (n) => `${n > 0 ? "+" : ""}${num(n, 1)}%`;
const esc = (s) => String(s ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));

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
  else marker = L.circleMarker([lat, lon], { radius: 9, weight: 3, color: "#ffffff", fillColor: "#0b5cad", fillOpacity: 1 }).addTo(map);
  marker.bindTooltip(t("marker"));
  const d = 0.0012; // ~130 m around the address
  map.flyToBounds([[lat - d, lon - d], [lat + d, lon + d]], { ...panelPadding(), maxZoom: 18, duration: 1.2 });
}

function showBuurt(geometry, lat, lon) {
  if (buurtLayer) buurtLayer.remove();
  if (!geometry) return;
  buurtLayer = L.geoJSON(geometry, { style: { color: "#0b5cad", weight: 2, fillOpacity: 0.08, dashArray: "4 4" } }).addTo(map);
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
}
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
  status.textContent = t("loading");
  try {
    const res = await fetch("/api/evaluate", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) });
    if (res.status === 429) throw new Error(t("tooMany"));
    const data = await res.json();
    if (!res.ok) throw new Error(typeof data.detail === "string" ? data.detail : t("errorGeneric"));
    status.textContent = "";
    lastResult = { data, mode };
    renderResult(lastResult);
    focusAddress(data.address.lat, data.address.lon);
    setTimeout(() => showBuurt(data.neighbourhood.buurt_geometry, data.address.lat, data.address.lon), 1300);
    $("#result").scrollIntoView({ behavior: "smooth", block: "start" });
  } catch (err) {
    status.className = "status error";
    status.textContent = err.message || t("errorGeneric");
  } finally {
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
  return `<div class="scale" aria-hidden="true">
    <div class="track"></div>
    <div class="band" style="left:${x(range.low)};width:calc(${x(range.high)} - ${x(range.low)})"></div>
    <div class="tick central" style="left:${x(range.central)}"></div>
    ${asked ? `<div class="tick asked" style="left:${x(asked)}"></div><span class="lbl" style="left:${x(asked)}">${eur(asked)}</span>` : `<span class="lbl" style="left:${x(range.central)}">${eur(range.central)}</span>`}
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
    [t("priceRange")(eur(r.low), eur(r.high)), basis], rangeScale(r, check.asking_price));
}

function rentCard(rent) {
  const s = rent.scenarios;
  const typ = s.typical;
  const pts = t("rentPoints")(typ.total_points, t("segment")[typ.segment] || typ.segment);
  const scen = t("rentScenarios")(eur(s.low.max_rent_eur), eur(typ.max_rent_eur), eur(s.high.max_rent_eur));
  const c = rent.rent_check;
  if (!c) {
    const free = typ.segment === "free";
    return verdictCard("info", t("legalRentTitle"), t("badge")[free ? "free_sector" : "estimate"], free ? t("rentFree") : scen, [free ? scen : "", pts]);
  }
  const kind = { within_limit: "ok", above_limit: "bad", free_sector: "info", depends_on_details: "warn" }[c.verdict];
  const headline = {
    within_limit: () => t("rentWithin")(eur(c.max_rent_typical)),
    above_limit: () => t("rentAbove")(eur(c.difference_eur), eur(c.max_rent_typical)),
    free_sector: () => t("rentFree"),
    depends_on_details: () => t("rentDepends")(eur(s.low.max_rent_eur), eur(s.high.max_rent_eur)),
  }[c.verdict]();
  const deposit = c.deposit_ok === undefined ? "" : (c.deposit_ok ? t("depositOk") : t("depositHigh"))(c.deposit_max_months);
  return verdictCard(kind, t("rentTitle"), t("badge")[c.verdict], headline, [scen, pts, deposit]);
}

function facts(d) {
  const a = d.address;
  const kf = d.neighbourhood.key_figures || {};
  const buurt = kf[a.buurt_code] || {};
  const out = [];
  const liv = ((d.neighbourhood.livability || {})[a.buurt_code] || []).slice(-1)[0];
  if (liv && liv.national_percentile != null) {
    out.push([t("livability"), `${num(liv.livability_score, 2)}`, t("livabilityValue")(Math.round(liv.national_percentile))]);
  }
  const crime = (d.neighbourhood.crime || {}).by_region || {};
  if (crime[a.buurt_code] && crime[a.buurt_code].per_1000_residents_last_12m != null) {
    out.push([t("crime"), num(crime[a.buurt_code].per_1000_residents_last_12m, 1), t("crimeValue")(num((crime.NL00 || {}).per_1000_residents_last_12m || 0, 1))]);
  }
  if (buurt.population) out.push([t("population"), num(buurt.population), a.buurt_name]);
  if (buurt.avg_woz_value_eur) out.push([t("avgWoz"), eur(buurt.avg_woz_value_eur), a.buurt_name]);
  if (d.dwelling.floor_area_m2) out.push([t("floorArea"), `${num(d.dwelling.floor_area_m2)} m²`, ""]);
  if (d.dwelling.build_year) out.push([t("buildYear"), String(d.dwelling.build_year), ""]);
  return `<h3>${esc(t("neighbourhood"))}</h3><div class="facts">${out.map(([k, v, s]) =>
    `<div class="fact"><span>${esc(k)}</span><b>${esc(v)}</b>${s ? `<small>${esc(s)}</small>` : ""}</div>`).join("")}</div>`;
}

function renderResult({ data, mode }) {
  const a = data.address;
  const primary = mode === "rent" ? rentCard(data.rent) : purchaseCard(data.purchase);
  const secondary = mode === "rent" ? purchaseCard({ ...data.purchase, asking_price_check: null }) : rentCard({ ...data.rent, rent_check: null });
  const assumptions = data.rent.assumptions || [];
  $("#result").innerHTML = `
    <div class="where"><h2>${esc(a.display_name)}</h2><p>${esc([a.buurt_name, a.municipality_name, a.province_name].filter(Boolean).join(" · "))}</p></div>
    ${primary}
    ${facts(data)}
    ${secondary}
    ${assumptions.length ? `<details><summary>${esc(t("assumptions"))}</summary><ul>${assumptions.map((x) => `<li>${esc(x)}</li>`).join("")}</ul></details>` : ""}`;
  $("#result").hidden = false;
}

// ---------------------------------------------------------------- start

applyLang(initialLang());
setMode("sale");
