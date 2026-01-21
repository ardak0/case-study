const API = "http://127.0.0.1:8000";
const username = localStorage.getItem("username");

document.getElementById("user").innerText = `Logged in as: ${username}`;

const roleBadge = document.getElementById("roleBadge");
if (username.includes("admin")) {
  roleBadge.innerText = "ADMIN";
  roleBadge.className = "role-badge admin";
} else if (username.includes("guest")) {
  roleBadge.innerText = "GUEST";
  roleBadge.className = "role-badge guest";
} else {
  roleBadge.innerText = "USER";
  roleBadge.className = "role-badge user";
}

let chartInstance = null;

function renderChart(type, labels, values, labelName) {
  const ctx = document.getElementById("chart").getContext("2d");
  if (chartInstance) chartInstance.destroy();

  chartInstance = new Chart(ctx, {
    type,
    data: {
      labels,
      datasets: [
        {
          label: labelName,
          data: values,
        },
      ],
    },
    options: {
      responsive: true,
      plugins: {
        legend: { display: true },
      },
      scales: {
        y: { beginAtZero: true },
      },
    },
  });
}

async function requestJson(endpoint) {
  const output = document.getElementById("output");
  output.innerText = "Loading...";

  const res = await fetch(`${API}${endpoint}`, {
    headers: { "X-User": username },
  });

  const text = await res.text();
  output.innerText = text;

  // Try parse JSON for charting
  try {
    return JSON.parse(text);
  } catch {
    return null;
  }
}

async function loadRevenue() {
  const data = await requestJson("/metrics/revenue-by-country");
  if (!data) return;

  // data: [{country, revenue}, ...]
  const labels = data.map((x) => x.country);
  const values = data.map((x) => Number(x.revenue));

  renderChart("bar", labels, values, "Revenue");
}

async function loadDaily() {
  // show last 30 days
  const data = await requestJson("/metrics/daily-revenue?limit=30");
  if (!data) return;

  // data: [{date/revenue} or {order_date/revenue} depending on your API
  const dateKey = data[0]?.order_date ? "order_date" : "date";

  const labels = data
    .map((x) => x[dateKey])
    .slice()
    .reverse();
  const values = data
    .map((x) => Number(x.revenue))
    .slice()
    .reverse();

  renderChart("line", labels, values, "Daily Revenue");
}

async function loadUsers() {
  // This will 403 for non-admin, which is intended
  await requestJson("/admin/users");
}

function logout() {
  localStorage.removeItem("username");
  window.location.href = "index.html";
}
