const fmtInt = (n) => Number(n || 0).toLocaleString(undefined, { maximumFractionDigits: 0 });
const fmtBRL = (n) => Number(n || 0).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
const fmtPct = (n) => (Number(n || 0) * 100).toFixed(2) + "%";

function showToast(msg) {
  const t = document.getElementById("toast");
  if (!t) return;
  t.textContent = msg;
  t.style.display = "block";
}

async function fetchJson(url) {
  const r = await fetch(url);
  if (!r.ok) {
    const txt = await r.text();
    throw new Error(`${url} -> ${r.status} ${r.statusText} | ${txt.slice(0, 160)}`);
  }
  return r.json();
}

let trendChart, txYearChart, catChart, payChart, dowChart, hourChart;
let currentGrain = "day";

const destroy = (c) => { try { if (c) c.destroy(); } catch { } };

const baseLegend = {
  labels: {
    boxWidth: 10, boxHeight: 10,
    usePointStyle: true, pointStyle: "circle",
    font: { size: 10, weight: "700" }
  }
};

function legendPosForDonut() {
  return window.innerWidth <= 520 ? "bottom" : "left";
}

async function loadKPI() {
  const d = await fetchJson("/api/kpi-overview");
  document.getElementById("kpiOrders").textContent = fmtInt(d.orders_cnt);
  document.getElementById("kpiGMV").textContent = fmtBRL(d.gmv_brl);
  document.getElementById("kpiCustomers").textContent = fmtInt(d.unique_customers);
  document.getElementById("kpiReview").textContent = Number(d.avg_review || 0).toFixed(3);
}

async function loadTrend() {
  const d = await fetchJson(`/api/sales-trend?grain=${currentGrain}`);
  destroy(trendChart);

  trendChart = new Chart(document.getElementById("trendChart"), {
    data: {
      labels: d.labels,
      datasets: [
        { type: "line", label: "Orders", data: d.orders, yAxisID: "y2", tension: .35, pointRadius: 0, borderWidth: 2 },
        { type: "bar", label: "Transaction Value (R$)", data: d.gmv_brl, yAxisID: "y1", borderRadius: 8 }
      ]
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      interaction: { mode: "index", intersect: false },
      plugins: { legend: { position: "left", ...baseLegend } },
      scales: {
        y1: { position: "left", ticks: { callback: (v) => fmtBRL(v) } },
        y2: { position: "right", grid: { drawOnChartArea: false }, ticks: { callback: (v) => fmtInt(v) } }
      }
    }
  });
}

async function loadTxYear() {
  const d = await fetchJson("/api/transaction-by-year");
  destroy(txYearChart);

  txYearChart = new Chart(document.getElementById("txYearChart"), {
    type: "bar",
    data: { labels: d.labels, datasets: [{ label: "GMV (R$)", data: d.gmv_brl, borderRadius: 10 }] },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: { y: { ticks: { callback: (v) => fmtBRL(v) } } }
    }
  });
}

async function loadCategoriesBar() {
  const d = await fetchJson("/api/best-category-by-orders?limit=8");
  destroy(catChart);

  catChart = new Chart(document.getElementById("catChart"), {
    type: "bar",
    data: { labels: d.labels, datasets: [{ label: "Orders", data: d.orders, borderRadius: 10 }] },
    options: {
      indexAxis: "y",
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: { x: { ticks: { callback: (v) => fmtInt(v) } } }
    }
  });
}

async function loadDOW() {
  const d = await fetchJson("/api/orders-revenue-by-dow");
  destroy(dowChart);

  dowChart = new Chart(document.getElementById("dowChart"), {
    data: {
      labels: d.labels,
      datasets: [
        { type: "bar", label: "Orders", data: d.orders, borderRadius: 8 },
        { type: "line", label: "Revenue (R$)", data: d.gross_revenue, yAxisID: "y2", tension: .35, pointRadius: 0, borderWidth: 2 }
      ]
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { position: "top", ...baseLegend } },
      scales: {
        y: { ticks: { callback: (v) => fmtInt(v) } },
        y2: { position: "right", grid: { drawOnChartArea: false }, ticks: { callback: (v) => fmtBRL(v) } }
      }
    }
  });
}

async function loadHour() {
  const d = await fetchJson("/api/orders-revenue-by-hour");
  destroy(hourChart);

  hourChart = new Chart(document.getElementById("hourChart"), {
    data: {
      labels: d.labels,
      datasets: [
        { type: "bar", label: "Orders", data: d.orders, borderRadius: 8 },
        { type: "line", label: "Revenue (R$)", data: d.gross_revenue, yAxisID: "y2", tension: .35, pointRadius: 0, borderWidth: 2 }
      ]
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { position: "top", ...baseLegend } },
      scales: {
        y: { ticks: { callback: (v) => fmtInt(v) } },
        y2: { position: "right", grid: { drawOnChartArea: false }, ticks: { callback: (v) => fmtBRL(v) } }
      }
    }
  });
}

async function loadPayments() {
  const d = await fetchJson("/api/payment-mix");
  destroy(payChart);

  payChart = new Chart(document.getElementById("payChart"), {
    type: "doughnut",
    data: { labels: d.labels, datasets: [{ label: "Count", data: d.counts }] },
    options: {
      responsive: true, maintainAspectRatio: false,
      cutout: "62%",
      layout: { padding: { left: 10, right: 10, top: 6, bottom: 6 } },
      plugins: { legend: { position: legendPosForDonut(), ...baseLegend } }
    }
  });
}

async function loadStateTable() {
  const d = await fetchJson("/api/state-key-metrics?limit=5");
  const tb = document.getElementById("stateTbody");
  tb.innerHTML = "";

  for (const r of d.rows) {
    tb.innerHTML += `
      <tr>
        <td><span class="pill">${r.state}</span></td>
        <td class="num">${fmtInt(r.total_orders)}</td>
        <td class="num">${fmtBRL(r.gross_revenue)}</td>
        <td class="num">${fmtBRL(r.aov)}</td>
      </tr>`;
  }

  document.getElementById("stateTfoot").innerHTML = `
    <td>Total</td>
    <td class="num">${fmtInt(d.total.total_orders)}</td>
    <td class="num">${fmtBRL(d.total.gross_revenue)}</td>
    <td class="num">${fmtBRL(d.total.aov)}</td>`;
}

async function loadCategoryTable() {
  const d = await fetchJson("/api/category-key-metrics?limit=8");
  const tb = document.getElementById("catTbody");
  tb.innerHTML = "";

  for (const r of d.rows) {
    const cancel = (r.cancel_rate === null || r.cancel_rate === undefined) ? "—" : fmtPct(r.cancel_rate);
    tb.innerHTML += `
      <tr>
        <td>${r.category}</td>
        <td class="num">${fmtInt(r.total_orders)}</td>
        <td class="num">${fmtBRL(r.gross_revenue)}</td>
        <td class="num">${fmtBRL(r.aov)}</td>
        <td class="num">${Number(r.avg_basket_size || 0).toFixed(2)}</td>
        <td class="num">${cancel}</td>
      </tr>`;
  }

  const totalCancel = (d.total.cancel_rate === null || d.total.cancel_rate === undefined) ? "—" : fmtPct(d.total.cancel_rate);
  document.getElementById("catTfoot").innerHTML = `
    <td>Total</td>
    <td class="num">${fmtInt(d.total.total_orders)}</td>
    <td class="num">${fmtBRL(d.total.gross_revenue)}</td>
    <td class="num">${fmtBRL(d.total.aov)}</td>
    <td class="num">${Number(d.total.avg_basket_size || 0).toFixed(2)}</td>
    <td class="num">${totalCancel}</td>`;
}

function setGrain(grain) {
  currentGrain = grain;
  document.getElementById("btnDay").classList.toggle("active", grain === "day");
  document.getElementById("btnHour").classList.toggle("active", grain === "hour");
  loadTrend().catch(e => showToast(e.message));
}

function debounce(fn, ms = 250) {
  let t; return (...args) => { clearTimeout(t); t = setTimeout(() => fn(...args), ms); };
}
const onResize = debounce(() => {
  try {
    if (payChart) loadPayments().catch(() => {});
    trendChart && trendChart.resize();
    txYearChart && txYearChart.resize();
    catChart && catChart.resize();
    dowChart && dowChart.resize();
    hourChart && hourChart.resize();
  } catch {}
}, 300);

async function initDashboard() {
  try {
    document.getElementById("btnDay").addEventListener("click", () => setGrain("day"));
    document.getElementById("btnHour").addEventListener("click", () => setGrain("hour"));
    window.addEventListener("resize", onResize);

    await Promise.all([
      loadKPI(),
      loadTrend(),
      loadTxYear(),
      loadCategoriesBar(),
      loadDOW(),
      loadHour(),
      loadPayments(),
      loadStateTable(),
      loadCategoryTable()
    ]);
  } catch (e) {
    showToast("โหลดข้อมูลบางส่วนไม่สำเร็จ: " + e.message);
    console.error(e);
  }
}
document.addEventListener("DOMContentLoaded", initDashboard);