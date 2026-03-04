function formatNumber(value, digits = 5) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return "--";
  }
  return Number(value).toLocaleString(undefined, {
    minimumFractionDigits: 0,
    maximumFractionDigits: digits,
  });
}

const state = {
  instruments: [],
  selectedInstrument: "",
  selectedGroup: "",
};

function currentIdentifier() {
  const el = document.getElementById("capitalIdentifier");
  if (!el) return "";
  return el.value.trim();
}

function titleCase(value) {
  if (!value) return "";
  if (value === "metals") return "Gold/Silver";
  return value.charAt(0).toUpperCase() + value.slice(1);
}

function renderTrades(containerId, trades) {
  const container = document.getElementById(containerId);
  if (!trades || !trades.length) {
    container.innerHTML = '<p class="muted">No trades</p>';
    return;
  }

  const rows = trades
    .map(
      (trade) => `
        <tr>
          <td>${trade.action}</td>
          <td>${trade.quantity}</td>
          <td>${trade.status}</td>
          <td>${formatNumber(trade.filled, 2)}</td>
          <td>${formatNumber(trade.avg_fill_price, 5)}</td>
        </tr>`
    )
    .join("");

  container.innerHTML = `
    <table>
      <thead>
        <tr>
          <th>Side</th>
          <th>Qty</th>
          <th>Status</th>
          <th>Filled</th>
          <th>Avg</th>
        </tr>
      </thead>
      <tbody>${rows}</tbody>
    </table>`;
}

function setFeedback(message, tone = "warn") {
  const el = document.getElementById("orderFeedback");
  el.textContent = message || "";
  el.className = `msg-${tone}`;
}

function renderInstrumentOptions(group, preferredKey = "") {
  const instrumentSelect = document.getElementById("instrument");
  const groupItems = state.instruments.filter((item) => item.group === group);

  if (!groupItems.length) {
    instrumentSelect.innerHTML = "";
    state.selectedInstrument = "";
    return;
  }

  let selectedKey = preferredKey;
  if (!groupItems.some((item) => item.key === selectedKey)) {
    selectedKey = groupItems[0].key;
  }
  state.selectedInstrument = selectedKey;

  instrumentSelect.innerHTML = groupItems
    .map(
      (item) =>
        `<option value="${item.key}" ${item.key === selectedKey ? "selected" : ""}>${item.label}</option>`
    )
    .join("");
}

function syncSelectors(instruments, selectedInstrument) {
  if (!instruments || !instruments.length) {
    return;
  }
  state.instruments = instruments;

  const classSelect = document.getElementById("assetClass");
  const groups = [...new Set(instruments.map((item) => item.group))];
  let selectedKey = selectedInstrument || state.selectedInstrument;
  if (!instruments.some((item) => item.key === selectedKey)) {
    selectedKey = instruments[0].key;
  }
  const selectedGroup =
    instruments.find((item) => item.key === selectedKey)?.group || groups[0];
  state.selectedGroup = selectedGroup;

  classSelect.innerHTML = groups
    .map(
      (group) =>
        `<option value="${group}" ${group === selectedGroup ? "selected" : ""}>${titleCase(group)}</option>`
    )
    .join("");
  renderInstrumentOptions(selectedGroup, selectedKey);
}

async function refreshDashboard() {
  const query = state.selectedInstrument
    ? `?instrument=${encodeURIComponent(state.selectedInstrument)}`
    : "";
  const identifier = currentIdentifier();
  const headers = {};
  if (identifier) headers["X-CAPITAL-IDENTIFIER"] = identifier;
  const response = await fetch(`/api/dashboard${query}`, { headers });
  const data = await response.json();
  if (!data.ok) {
    setFeedback(data.message || "Failed to load dashboard", "bad");
    return;
  }

  syncSelectors(data.available_instruments || [], data.selected_instrument?.key);
  document.getElementById("symbol").textContent =
    data.selected_instrument?.label || data.symbol || "Instrument";
  document.getElementById("price").textContent = formatNumber(data.price, 5);
  document.getElementById("marketMessage").textContent = data.market?.message || "";

  const marketBadge = document.getElementById("marketBadge");
  if (data.market?.is_open === true) {
    marketBadge.textContent = "OPEN";
    marketBadge.className = "badge open";
  } else if (data.market?.is_open === false) {
    marketBadge.textContent = "CLOSED";
    marketBadge.className = "badge closed";
  } else {
    marketBadge.textContent = "UNAVAILABLE";
    marketBadge.className = "badge neutral";
  }

  renderTrades("pendingTrades", data.trades?.pending || []);
  renderTrades("ongoingTrades", data.trades?.ongoing || []);
  renderTrades("completedTrades", data.trades?.completed || []);
}

async function submitOrder(event) {
  event.preventDefault();
  const action = document.getElementById("action").value;
  const quantity = Number(document.getElementById("quantity").value);
  const instrument = state.selectedInstrument;
  const identifier = currentIdentifier();

  if (!Number.isFinite(quantity) || quantity <= 0) {
    setFeedback("Quantity must be greater than zero.", "bad");
    return;
  }
  if (!instrument) {
    setFeedback("Select an instrument first.", "bad");
    return;
  }

  setFeedback("Submitting order...", "warn");
  const response = await fetch("/api/orders/market", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ action, quantity, instrument, identifier }),
  });
  const data = await response.json();
  if (!data.ok) {
    if (data.error === "market_closed") {
      setFeedback(data.message || "Market is closed.", "bad");
    } else {
      setFeedback(data.message || "Order failed.", "bad");
    }
    await refreshDashboard();
    return;
  }

  setFeedback("Market order submitted successfully.", "good");
  await refreshDashboard();
}

document.getElementById("orderForm").addEventListener("submit", submitOrder);
document.getElementById("capitalIdentifier").addEventListener("change", (event) => {
  localStorage.setItem("capitalIdentifier", event.target.value.trim());
  refreshDashboard().catch((err) => setFeedback(err.message, "bad"));
});
document.getElementById("assetClass").addEventListener("change", (event) => {
  state.selectedGroup = event.target.value;
  renderInstrumentOptions(state.selectedGroup);
  refreshDashboard().catch((err) => setFeedback(err.message, "bad"));
});
document.getElementById("instrument").addEventListener("change", (event) => {
  state.selectedInstrument = event.target.value;
  refreshDashboard().catch((err) => setFeedback(err.message, "bad"));
});
document.getElementById("capitalIdentifier").value =
  localStorage.getItem("capitalIdentifier") || "";
refreshDashboard().catch((err) => setFeedback(err.message, "bad"));
setInterval(() => {
  refreshDashboard().catch(() => {});
}, 5000);
