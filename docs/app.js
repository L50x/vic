// PUBLISHED CSV LINKS
const CHANGELOG_CSV = "PASTE_CHANGELOG_CSV_URL";
const MENU_CSV = "PASTE_MENU_CSV_URL";

function showTab(id) {
  document.querySelectorAll(".tab").forEach(t => t.classList.remove("active"));
  document.getElementById(id).classList.add("active");
}

async function loadTable(csvUrl, tableId) {
  const res = await fetch(csvUrl);
  const text = await res.text();
  const rows = text.trim().split("\n").map(r => r.split(","));

  const table = document.getElementById(tableId);
  table.innerHTML = "";

  rows.forEach((row, i) => {
    const tr = document.createElement("tr");
    row.forEach(cell => {
      const el = document.createElement(i === 0 ? "th" : "td");

      if (cell.startsWith("http")) {
        const a = document.createElement("a");
        a.href = cell;
        a.target = "_blank";
        a.textContent = "link";
        el.appendChild(a);
      } else {
        el.textContent = cell.replace(/^"|"$/g, "");
      }

      tr.appendChild(el);
    });
    table.appendChild(tr);
  });
}

loadTable(CHANGELOG_CSV, "changelog-table");
loadTable(MENU_CSV, "menu-table");
