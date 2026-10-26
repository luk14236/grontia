"use strict";
// The detailed view and the PDF report come from the same cards: every
// section with its explanation, the neighbourhood outline, the valuation and
// WWS tables, all news and the sources. The detailed view shows them on
// screen next to the summary panel; "Export PDF" prints them (A4).
// Uses the helpers and section bodies in app.js.

// Neighbourhood outline + address as an SVG (map tiles don't print reliably)
function outlineMap(geometry, lat, lon) {
  if (!geometry) return "";
  const polys = geometry.type === "MultiPolygon" ? geometry.coordinates : [geometry.coordinates];
  const pts = polys.flatMap((p) => p[0]).concat([[lon, lat]]);
  const k = Math.cos((lat * Math.PI) / 180);  // keep shapes true at this latitude
  const xs = pts.map((p) => p[0] * k), ys = pts.map((p) => p[1]);
  const minX = Math.min(...xs), maxX = Math.max(...xs), minY = Math.min(...ys), maxY = Math.max(...ys);
  const Wm = 480, Hm = 300, pad = 14;
  const sc = Math.min((Wm - 2 * pad) / (maxX - minX || 1), (Hm - 2 * pad) / (maxY - minY || 1));
  const X = (p) => pad + (p[0] * k - minX) * sc, Y = (p) => pad + (maxY - p[1]) * sc;
  const d = polys.map((rings) => rings.map((r) => "M" + r.map((p) => `${X(p).toFixed(1)},${Y(p).toFixed(1)}`).join("L") + "Z").join("")).join("");
  return `<svg class="outline" viewBox="0 0 ${Wm} ${Hm}" role="img">
    <path d="${d}" fill-rule="evenodd"/><circle cx="${X([lon, lat])}" cy="${Y([lon, lat])}" r="6"/></svg>`;
}

// "2026MM09", 12 -> "2025-10 – 2026-09"
function crimeWindow(latest, months) {
  const [y, m] = latest.split("MM").map(Number);
  const start = new Date(y, m - months, 1);
  return `${start.getFullYear()}-${String(start.getMonth() + 1).padStart(2, "0")} – ${y}-${String(m).padStart(2, "0")}`;
}

function table(head, rows) {
  return `<div class="table-wrap"><table class="r-table"><thead><tr>${head.map((h) => `<th>${esc(h)}</th>`).join("")}</tr></thead>
    <tbody>${rows.map((r) => `<tr>${r.map((c) => `<td>${c}</td>`).join("")}</tr>`).join("")}</tbody></table></div>`;
}

function inputsText(inputs) {
  // nested index records are spelled out elsewhere; keep the flat inputs
  return Object.entries(inputs || {}).filter(([, v]) => v === null || typeof v !== "object").map(([k, v]) =>
    `${k.replace(/_/g, " ")}: ${typeof v === "number" && !k.endsWith("year") ? num(v, v < 10 ? 3 : 1) : v}`).join(" · ");
}

// [{ id, title, body, wide }] in reading order; empty bodies are dropped
function detailCards({ data, mode, news }, { onlyImportantNews = false } = {}) {
  const R = t("report");
  const a = data.address, dw = data.dwelling, p = data.purchase, rent = data.rent;
  const primary = mode === "rent" ? rentCard(rent, data.outlook) : purchaseCard(p);
  const secondary = mode === "rent" ? purchaseCard({ ...p, asking_price_check: null }) : rentCard({ ...rent, rent_check: null }, data.outlook);
  const use = dw.use || "";
  const notAHome = use && !use.split(",").includes("woonfunctie") ? `<div class="banner warn">${esc(t("notAHome")(use))}</div>` : "";

  const osm = `https://www.openstreetmap.org/?mlat=${a.lat}&mlon=${a.lon}#map=17/${a.lat}/${a.lon}`;
  const location = outlineMap(data.neighbourhood.buurt_geometry, a.lat, a.lon)
    + `<p class="detail">${esc(R.locationNote(a.buurt_name))} ${num(a.lat, 5)}, ${num(a.lon, 5)} · <a href="${osm}" target="_blank" rel="noopener">${esc(R.openMap)}</a></p>`;

  const home = table([R.home, ""], [
    [esc(t("floorArea")), `${num(dw.floor_area_m2)} m²`], [esc(t("buildYear")), esc(dw.build_year)],
    [esc(R.use), esc(use)], [esc(R.units), esc(dw.units_in_building)],
    [esc(R.apartment), esc(dw.is_apartment ? R.yes : R.no)],
  ]);

  const ref = p.neighbourhood_reference || {};
  const lvl = p.area_level, sp = p.size_vs_peers || {};
  const valuation = `<div class="kpis">${[[t("glance").value, eur(p.range.central)], ["−", eur(p.range.low)], ["+", eur(p.range.high)]]
      .map(([k, v]) => `<div><span>${esc(k)}</span><b>${v}</b></div>`).join("")}</div>`
    + table([R.method, R.value, R.vsCentre, R.inputs],
      p.estimates.map((e) => [esc(R.methods[e.method] || e.method) + hintButton(`info.methods.${e.method}`), eur(e.value),
        e.method === p.central_basis ? esc(R.centre) : `${signed((p.cross_checks_pct || {})[e.method])}%`,
        `<small>${esc(inputsText(e.inputs))}</small>`]))
    + [t("priceRange")(eur(p.range.low), eur(p.range.high))
        + (p.confidence ? " " + t("confidenceLine")(t("confidence")[p.confidence], p.band_pct) : ""),
       t("basis")(t("basisName")[p.central_basis] || p.central_basis, eur(p.price_per_m2_central)),
       lvl && R.areaLevel(eur(lvl.avg_woz_value_eur), lvl.woz_value_date, num(lvl.factor, 3),
         lvl.index_now.period_code.replace("KW", " Q"), eur(lvl.level_eur)),
       sp.count && R.peers(R.homeTypes[sp.home_type], num(sp.count), num(sp.median_m2), num(sp.p25_m2), num(sp.p75_m2), num(sp.floor_area_m2)),
       ...(p.warnings || []).map((w) => t("valueWarnings")[w]),
       t("unseen"),
       { low: t("sharpenLow")(10), medium: t("sharpenMedium") }[p.confidence],
      ].filter(Boolean).map((x) => `<p class="detail">${esc(x)}</p>`).join("");

  const sc = rent.scenarios, keys = ["low", "typical", "high"];
  const wwsTable = table([R.rubric, ...keys.map((k) => t("scenario")[k === "typical" ? "base" : k])], [
    ...Object.keys(sc.typical.points_by_rubric).map((r) => [esc(R.rubrics[r] || r) + hintButton(`info.rubrics.${r}`), ...keys.map((k) => num(sc[k].points_by_rubric[r], 1))]),
    [`<b>${esc(R.total)}</b>`, ...keys.map((k) => `<b>${num(sc[k].total_points)}</b>`)],
    [esc(R.segment), ...keys.map((k) => esc(t("segment")[sc[k].segment] || sc[k].segment))],
    [esc(R.maxRent), ...keys.map((k) => `<b>${eur(sc[k].max_rent_eur)}</b>`)],
  ]);
  const wws = `<div class="split"><div>${wwsTable}<p class="detail">${esc(R.rulesFrom(rent.rules_valid_from, rent.rules_source))}</p></div>
    <div><h4>${esc(t("assumptions"))}</h4><ul class="r-list">${(rent.assumptions || []).map((x) => `<li>${esc(x)}</li>`).join("")}</ul></div></div>`;

  const woz = (p.appreciation || {}).municipality_avg_woz || [];
  const prices = `<div class="split"><div>${pricesBody(data)}</div>${woz.length > 2 ? `<div><p class="detail">${esc(R.wozHistory)}</p>`
    + lineChart([{ values: woz.map((x) => x.avg_woz_value_eur), cls: "high" }], woz.map((x) => String(x.year)), kEur) + caption("info.charts.woz") + "</div>" : ""}</div>`;

  const buurtLiv = ((data.neighbourhood.livability || {})[a.buurt_code] || []).slice(-1)[0] || {};
  const crime = data.neighbourhood.crime || {};
  const kf = (data.neighbourhood.key_figures || {})[a.buurt_code] || {};
  const periods = R.periodRows({
    index: (ref.index_latest_period || "").replace("KW", " Q"), kf: (kf.period_code || "").slice(0, 4),
    crime: crime.latest_period ? crimeWindow(crime.latest_period, crime.window_months) : "–",
    liv: buurtLiv.year || "–", rules: rent.rules_valid_from, costs: (data.outlook || {}).costs_valid_from || "–",
  });
  const sources = `<h4>${esc(R.periods)}</h4>${table(["", ""], periods.map(([k, v]) => [esc(k), esc(v)]))}
    <ul class="r-list">${R.method_notes.map((x) => `<li>${esc(x)}</li>`).join("")}</ul>
    ${table(["", "", ""], R.sourceList.map(([n, w, u]) => [`<b>${esc(n)}</b>`, esc(w), `<a href="${esc(u)}" target="_blank" rel="noopener">${esc(u.replace(/^https:\/\//, ""))}</a>`]))}`;

  return [
    { id: "verdict", title: R.summary, body: notAHome + primary + secondary },
    { id: "location", title: R.location, body: location },
    { id: "home", title: R.home, body: home },
    { id: "valuation", title: R.valuation, body: valuation },
    { id: "rent", title: R.rentDetails, body: wws },
    { id: "outlook", title: t("outlookTitle"), body: outlookBody(data.outlook), wide: true },
    { id: "overview", title: t("overviewTitle"), body: overviewBody(data), wide: true },
    { id: "livability", title: t("livabilityTitle"), body: livabilityBody(data) },
    { id: "safety", title: t("safetyTitle"), body: safetyBody(data) },
    { id: "prices", title: t("pricesTitle"), body: prices },
    { id: "news", title: t("newsTitle"), body: newsBody(news, { onlyImportant: onlyImportantNews }) },
    { id: "sources", title: R.sources, body: sources, wide: true },
  ].filter((c) => c.body);
}

function cardHtml(c) {
  const explain = t("explain")[c.id];
  return `<article class="card${c.wide ? " wide" : ""}" id="d-${c.id}"${c.span ? ` style="--span:${c.span}"` : ""}>
    <header><h2>${esc(c.title)}</h2>${explain ? hintButton(`explain.${c.id}`) : ""}</header>
    ${explain ? `<p class="explain">${esc(explain)}</p>` : ""}
    ${c.body}</article>`;
}

// ---------------------------------------------------------------- detailed view (screen)

// Cards grouped into sectors on a 12-column grid; the spans pair cards of
// similar height so the grid has no holes. One column on narrow screens.
const SECTORS = [
  ["verdict", [["verdict", 7], ["location", 5]]],
  ["home", [["home", 5], ["valuation", 7], ["rent", 12]]],
  ["outlook", [["outlook", 12]]],
  ["area", [["overview", 12], ["livability", 6], ["safety", 6], ["prices", 12]]],
  ["news", [["news", 12]]],
  ["sources", [["sources", 12]]],
];

function sectorsHtml(cards, sectors = SECTORS) {
  const S = t("sectors");
  return sectors.map(([id, list]) => {
    const present = list.filter(([cid]) => cards[cid]);
    if (!present.length) return "";
    return `<section class="sector" id="s-${id}">
      <header class="sector-head"><h2>${esc(S[id][0])}</h2><p>${esc(S[id][1])}</p></header>
      <div class="sector-grid">${present.map(([cid, span]) => cardHtml({ ...cards[cid], span })).join("")}</div></section>`;
  }).join("");
}

function renderDetail(state) {
  const a = state.data.address;
  const cards = Object.fromEntries(detailCards(state).map((c) => [c.id, c]));
  const sectors = sectorsHtml(cards);
  const S = t("sectors");
  $("#detail").innerHTML = `
    <div class="detail-head">
      <img src="brand/grontia-symbol.svg" alt="" width="40" height="40">
      <div class="detail-title"><p class="r-eyebrow">${esc(t("detailTitle"))}</p><h1>${esc(a.display_name)}</h1>
        <p>${esc([a.buurt_name, a.municipality_name, a.province_name].filter(Boolean).join(" · "))}</p></div>
      <div class="detail-actions">
        <button type="button" class="secondary" id="detail-pdf">${esc(t("exportPdf"))}</button>
        <button type="button" class="primary small" id="detail-close">${esc(t("closeDetail"))}</button>
      </div>
    </div>
    <div class="detail-body">
      <nav class="sector-nav" aria-label="${esc(t("sectorNav"))}"><p>${esc(t("sectorNav"))}</p>
        ${SECTORS.map(([id]) => `<a href="#s-${id}" data-target="s-${id}">${esc(S[id][0])}</a>`).join("")}</nav>
      <div class="detail-main">${sectors}</div>
    </div>`;
  $("#detail-close").addEventListener("click", () => toggleDetail(false));
  $("#detail-pdf").addEventListener("click", () => exportReport(state));
  const links = [...$("#detail").querySelectorAll(".sector-nav a")];
  links.forEach((l) => l.addEventListener("click", (e) => {
    e.preventDefault();
    document.getElementById(l.dataset.target).scrollIntoView({ behavior: "smooth", block: "start" });
  }));
  // highlight the sector being read (the first one until the reader scrolls)
  if (links[0]) links[0].classList.add("active");
  const spy = new IntersectionObserver((entries) => entries.forEach((en) => {
    if (en.isIntersecting) links.forEach((l) => l.classList.toggle("active", l.dataset.target === en.target.id));
  }), { root: $("#detail"), rootMargin: "-30% 0px -60% 0px" });
  $("#detail").querySelectorAll(".sector").forEach((sec) => spy.observe(sec));
}

function toggleDetail(open) {
  if (open && lastResult) renderDetail(lastResult);
  $("#detail").hidden = !open;
  document.body.classList.toggle("detail-open", open);
  if (open) $("#detail").scrollTop = 0;
}

// ---------------------------------------------------------------- PDF report (print)

// Every explanation in one place at the end of the PDF (hints don't print)
function glossaryCard() {
  const I = t("info"), R = t("report");
  const group = (title, entries) => `<h4>${esc(title)}</h4><dl class="glossary">${entries.map(([k, v]) =>
    `<div><dt>${esc(k)}</dt><dd>${esc(v)}</dd></div>`).join("")}</dl>`;
  const body = [
    group(t("livabilityTitle"), [[t("livabilityTitle"), I.livScore], ...Object.keys(I.dims).map((k) => [t("dims")[k], I.dims[k]])]),
    group(t("overviewTitle"), Object.keys(I.facts).map((k) => [({ crimeRate: t("safetyTitle"), crimeTypes: t("crimeTypes") })[k] || t(k), I.facts[k]])),
    group(R.valuation, Object.keys(I.methods).map((k) => [R.methods[k], I.methods[k]])),
    group(R.rentDetails, Object.keys(I.rubrics).map((k) => [R.rubrics[k], I.rubrics[k]])),
    group(t("outlookTitle"), [[t("valueIn")(new Date().getFullYear() + 5), I.returns.value], [t("totalReturn"), I.returns.total]]),
  ].join("");
  return `<section class="sector" id="s-glossary"><header class="sector-head"><h2>${esc(t("glossaryTitle"))}</h2></header>
    <div class="sector-grid"><article class="card" id="d-glossary">${body}</article></div></section>`;
}

// PDF order: the answer first, the parts that can run long last
const PRINT_SECTORS = [
  ["home", [["home", 5], ["valuation", 7], ["rent", 12]]],
  ["outlook", [["outlook", 12]]],
  ["area", [["overview", 12], ["livability", 6], ["safety", 6], ["prices", 12]]],
  ["sources", [["sources", 12]]],
];

function buildReport(state) {
  const R = t("report");
  const { data } = state;
  const a = data.address;
  const today = new Date().toLocaleDateString(locale(), { day: "numeric", month: "long", year: "numeric" });
  // the PDF keeps only the news that matters (safety, traffic, city development)
  const cards = Object.fromEntries(detailCards(state, { onlyImportantNews: true }).map((c) => [c.id, c]));
  const S = t("sectors");
  const contents = PRINT_SECTORS.map(([id]) => `<li>${esc(S[id][0])}</li>`).join("")
    + `<li>${esc(t("glossaryTitle"))}</li><li>${esc(S.news[0])}</li>`;
  return `<section class="r-cover">
      <div class="r-cover-head"><img src="brand/grontia-wordmark.svg" alt="Grontia" class="r-wordmark">
        <div><p class="r-eyebrow">${esc(R.title)}</p><h1>${esc(a.display_name)}</h1>
        <p>${esc([a.buurt_name, a.wijk_name, a.municipality_name, a.province_name].filter(Boolean).join(" · "))}</p>
        <p>${esc(R.generated(today))}</p></div></div>
      ${glance(data)}
      <div class="r-cover-grid">
        <div class="card" style="--span:12">${cards.verdict.body}</div>
        ${cards.location ? cardHtml(cards.location) : ""}
      </div>
      <div class="r-contents"><p class="r-eyebrow">${esc(t("sectorNav"))}</p><ol>${contents}</ol></div>
    </section>
    ${sectorsHtml(cards, PRINT_SECTORS)}
    ${glossaryCard()}
    ${sectorsHtml(cards, [["news", [["news", 12]]]])}
    <footer class="r-foot"><p>${esc(t("disclaimer"))}</p><p>${esc(R.project)}</p></footer>`;
}

async function exportReport(state) {
  if (!state) return;
  const buttons = ["#export-pdf", "#detail-pdf"].map((s) => $(s)).filter(Boolean);
  const labels = buttons.map((b) => b.textContent);
  buttons.forEach((b) => { b.disabled = true; b.textContent = t("exportPreparing"); });
  try {
    if (state.newsLoaded) await state.newsLoaded;  // the report includes the news
    $("#report").innerHTML = buildReport(state);
    const footer = `Grontia · ${state.data.address.display_name}`.replace(/["\\]/g, "");
    $("#report-page-style").textContent = `@page { @bottom-left { content: "${footer}"; font: 8pt Arial, sans-serif; color: #56627a; } }`;
    // the default PDF file name comes from the page title
    document.title = `Grontia - ${state.data.address.display_name}`;
    await new Promise((r) => setTimeout(r, 50));
    window.print();
  } finally {
    document.title = "Grontia";
    buttons.forEach((b, i) => { b.disabled = false; b.textContent = labels[i]; });
  }
}
