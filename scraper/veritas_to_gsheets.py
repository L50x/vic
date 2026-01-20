import re
import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials

URL = "https://veritasthca.com/2023/06/17/live-rosin-menu/"
SPREADSHEET_ID = "PUT_YOUR_SHEET_ID_HERE"

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# ---------------- auth ----------------

creds = Credentials.from_service_account_file(
    "credentials.json", scopes=SCOPES
)
gc = gspread.authorize(creds)
sh = gc.open_by_key(SPREADSHEET_ID)

# Ensure sheet order
try:
    changelog_ws = sh.worksheet("changelog")
except gspread.WorksheetNotFound:
    changelog_ws = sh.add_worksheet("changelog", rows=1000, cols=10)

try:
    current_ws = sh.worksheet("current_menu")
except gspread.WorksheetNotFound:
    current_ws = sh.add_worksheet("current_menu", rows=1000, cols=10)

sh.reorder_worksheets([changelog_ws, current_ws])

# ---------------- helpers ----------------

def clean_text(el):
    return el.get_text(" ", strip=True) if el else ""

def parse_grams(text):
    if not text:
        return None
    if "SOLD OUT" in text.upper():
        return 0
    m = re.search(r"(\d+)", text)
    return int(m.group(1)) if m else None

def parse_price(text):
    if not text:
        return None
    m = re.search(r"\$([\d.]+)", text)
    return float(m.group(1)) if m else None

# ---------------- scrape ----------------

def fetch_menu():
    soup = BeautifulSoup(requests.get(URL).text, "html.parser")
    table = soup.select_one("figure table")
    rows = table.select("tr")

    records = []
    current_section = "Unknown"

    for row in rows:
        cells = row.find_all("td")
        if not cells:
            continue

        if len(cells) == 1 and "Tier" in clean_text(cells[0]):
            current_section = clean_text(cells[0])
            continue

        name_cell = cells[0]
        name = clean_text(name_cell)
        link = name_cell.find("a")["href"] if name_cell.find("a") else None

        if not name:
            continue

        tier = clean_text(cells[1]) if len(cells) > 1 else ""
        stock_text = clean_text(cells[2]) if len(cells) > 2 else ""
        price_text = clean_text(cells[4]) if len(cells) > 4 else ""

        stock = parse_grams(stock_text)
        price = parse_price(price_text)

        records.append({
            "id": f"{current_section}|{tier}|{name}".lower(),
            "section": current_section,
            "strain": name,
            "tier": tier,
            "stock": stock,
            "sold_out": stock == 0,
            "price": price,
            "link": link,
            "last_seen": datetime.utcnow().isoformat()
        })

    return pd.DataFrame(records)

# ---------------- gsheets sync ----------------

def update_sheets(new_df):
    timestamp = datetime.utcnow().isoformat()

    try:
        old_df = pd.DataFrame(current_ws.get_all_records())
    except Exception:
        old_df = pd.DataFrame()

    # write headers if empty
    if current_ws.row_count == 0 or not current_ws.get_all_values():
        current_ws.append_row(list(new_df.columns))

    old_df = old_df.set_index("id") if not old_df.empty else old_df
    new_df = new_df.set_index("id")

    changelog_rows = []

    # NEW
    for idx in new_df.index.difference(old_df.index):
        r = new_df.loc[idx]
        changelog_rows.append([
            timestamp, "NEW_ITEM", r.strain, r.link, None, None, None
        ])

    # REMOVED
    for idx in old_df.index.difference(new_df.index):
        r = old_df.loc[idx]
        changelog_rows.append([
            timestamp, "REMOVED", r.strain, r.get("link"), None, None, None
        ])

    # CHANGES
    for idx in new_df.index.intersection(old_df.index):
        o, n = old_df.loc[idx], new_df.loc[idx]
        for field in ["stock", "price", "sold_out"]:
            if o[field] != n[field]:
                changelog_rows.append([
                    timestamp, "FIELD_CHANGE", n.strain, n.link,
                    field, o[field], n[field]
                ])

    # overwrite current menu
    current_ws.clear()
    current_ws.append_row(list(new_df.reset_index().columns))
    current_ws.append_rows(new_df.reset_index().values.tolist())

    # append changelog
    if changelog_rows:
        if not changelog_ws.get_all_values():
            changelog_ws.append_row([
                "timestamp", "change_type", "strain",
                "link", "field", "old_value", "new_value"
            ])
        changelog_ws.append_rows(changelog_rows)

# ---------------- main ----------------

def main():
    df = fetch_menu()
    update_sheets(df)

if __name__ == "__main__":
    main()
