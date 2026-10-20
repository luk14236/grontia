"use strict";
// Printable report: every section open, the extra detail tables, the
// neighbourhood outline, all news and the sources. "Export PDF" fills
// #report and opens the browser's print window ("Save as PDF"); vector
// charts and links survive into the PDF. Uses the helpers in app.js.

// Neighbourhood outline + address as an SVG (no map tiles: they don't print reliably)
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

function table(head, rows) {
  return `<table class="r-table"><thead><tr>${head.map((h) => `<th>${esc(h)}</th>`).join("")}</tr></thead>
    <tbody>${rows.map((r) => `<tr>${r.map((c) => `<td>${c}</td>`).join("")}</tr>`).join("")}</tbody></table>`;
}

function inputsText(inputs) {
  return Object.entries(inputs || {}).map(([k, v]) =>
    `${k.replace(/_/g, " ")}: ${typeof v === "number" ? num(v, v < 10 ? 3 : 1) : v}`).join(" · ");
}

function rSection(title, body) {
  return `<section class="r-sec"><h2>${esc(title)}</h2>${body}</section>`;
}

function buildReport({ data, mode, news }) {
  const R = t("report");
  const a = data.address, dw = data.dwelling, p = data.purchase, rent = data.rent;
  const today = new Date().toLocaleDateString(locale(), { day: "numeric", month: "long", year: "numeric" });
  const primary = mode === "rent" ? rentCard(rent, data.outlook) : purchaseCard(p);
  const secondary = mode === "rent" ? purchaseCard({ ...p, asking_price_check: null }) : rentCard({ ...rent, rent_check: null }, data.outlook);
  const use = dw.use || "";
  const notAHome = use && !use.split(",").includes("woonfunctie") ? `<div class="banner warn">${esc(t("notAHome")(use))}</div>` : "";

  const osm = `https://www.openstreetmap.org/?mlat=${a.lat}&mlon=${a.lon}#map=17/${a.lat}/${a.lon}`;
  const location = outlineMap(data.neighbourhood.buurt_geometry, a.lat, a.lon)
    + `<p class="detail">${esc(R.locationNote(a.buurt_name))} ${num(a.lat, 5)}, ${num(a.lon, 5)} · <a href="${osm}">${esc(R.openMap)}</a></p>`;

  const home = table([R.home, ""], [
    [esc(t("floorArea")), `${num(dw.floor_area_m2)} m²`], [esc(t("buildYear")), esc(dw.build_year)],
    [esc(R.use), esc(use)], [esc(R.units), esc(dw.units_in_building)],
    [esc(R.apartment), esc(dw.is_apartment ? R.yes : R.no)],
  ]);

  const ref = p.neighbourhood_reference || {};
  const valuation = table([R.method, R.value, R.inputs],
      p.estimates.map((e) => [esc(R.methods[e.method] || e.method), eur(e.value), `<small>${esc(inputsText(e.inputs))}</small>`]))
    + `<p class="detail">${esc(t("priceRange")(eur(p.range.low), eur(p.range.high)))} ${esc(t("basis")(t("basisName")[p.central_basis] || p.central_basis, eur(p.price_per_m2_central)))}</p>`
    + (ref.est_current_avg_sale_price_eur ? `<p class="detail">${esc(R.reference(eur(ref.est_current_avg_sale_price_eur), eur(ref.avg_woz_value_eur), num(ref.woz_ratio_to_municipality, 3), (ref.index_latest_period || "").replace("KW", " Q")))}</p>` : "");

  const sc = rent.scenarios, keys = ["low", "typical", "high"];
  const rubrics = Object.keys(sc.typical.points_by_rubric);
  const wws = table([R.rubric, ...keys.map((k) => t("scenario")[k === "typical" ? "base" : k])], [
    ...rubrics.map((r) => [esc(R.rubrics[r] || r), ...keys.map((k) => num(sc[k].points_by_rubric[r], 1))]),
    [`<b>${esc(R.total)}</b>`, ...keys.map((k) => `<b>${num(sc[k].total_points)}</b>`)],
    [esc(R.segment), ...keys.map((k) => esc(t("segment")[sc[k].segment] || sc[k].segment))],
    [esc(R.maxRent), ...keys.map((k) => eur(sc[k].max_rent_eur))],
  ]) + `<p class="detail">${esc(R.rulesFrom(rent.rules_valid_from, rent.rules_source))}</p>`
    + `<h4>${esc(t("assumptions"))}</h4><ul class="r-list">${(rent.assumptions || []).map((x) => `<li>${esc(x)}</li>`).join("")}</ul>`;

  const woz = (p.appreciation || {}).municipality_avg_woz || [];
  const wozChart = woz.length > 2 ? `<p class="detail">${esc(R.wozHistory)}</p>`
    + lineChart([{ values: woz.map((x) => x.avg_woz_value_eur), cls: "high" }], woz.map((x) => String(x.year)), kEur) : "";

  const buurtLiv = ((data.neighbourhood.livability || {})[a.buurt_code] || []).slice(-1)[0] || {};
  const crime = data.neighbourhood.crime || {};
  const kf = (data.neighbourhood.key_figures || {})[a.buurt_code] || {};
  const periods = R.periodRows({
    index: (ref.index_latest_period || "").replace("KW", " Q"), kf: (kf.period_code || "").slice(0, 4),
    crime: crime.latest_period ? `${crime.window_months} months to ${crime.latest_period.replace("MM", "-")}` : "–",
    liv: buurtLiv.year || "–", rules: rent.rules_valid_from, costs: (data.outlook || {}).costs_valid_from || "–",
  });
  const sources = `<h4>${esc(R.periods)}</h4>${table(["", ""], periods.map(([k, v]) => [esc(k), esc(v)]))}
    <ul class="r-list">${R.method_notes.map((x) => `<li>${esc(x)}</li>`).join("")}</ul>
    ${table(["", "", ""], R.sourceList.map(([n, w, u]) => [`<b>${esc(n)}</b>`, esc(w), `<a href="${esc(u)}">${esc(u.replace(/^https:\/\//, ""))}</a>`]))}`;

  return `<header class="r-head"><img src="logo.svg" alt="" width="56" height="56">
      <div><p class="r-eyebrow">Grontia · ${esc(R.title)}</p><h1>${esc(a.display_name)}</h1>
      <p>${esc([a.buurt_name, a.wijk_name, a.municipality_name, a.province_name].filter(Boolean).join(" · "))}</p>
      <p class="detail">${esc(R.generated(today))}</p></div></header>
    ${rSection(R.summary, notAHome + primary + secondary)}
    ${rSection(R.location, location)}
    ${rSection(R.home, home)}
    ${rSection(R.valuation, valuation)}
    ${rSection(R.rentDetails, wws)}
    ${outlookSection(data.outlook)}
    ${overviewSection(data)}
    ${livabilitySection(data)}
    ${safetySection(data)}
    ${pricesSection(data)}${wozChart ? `<div class="r-extra">${wozChart}</div>` : ""}
    ${newsSection(news, { notices: 40, headlines: 15 })}
    ${rSection(R.sources, sources)}
    <footer class="r-foot"><p>${esc(t("disclaimer"))}</p><p>${esc(R.project)}</p></footer>`;
}

async function exportReport(state) {
  if (!state) return;
  const btn = $("#export-pdf");
  const label = btn.textContent;
  btn.disabled = true;
  btn.textContent = t("exportPreparing");
  try {
    if (state.newsLoaded) await state.newsLoaded;  // the report includes the news
    $("#report").innerHTML = buildReport(state);
    // the default PDF file name comes from the page title
    document.title = `Grontia - ${state.data.address.display_name}`;
    await new Promise((r) => setTimeout(r, 50));
    window.print();
  } finally {
    document.title = "Grontia";
    btn.disabled = false;
    btn.textContent = label;
  }
}
