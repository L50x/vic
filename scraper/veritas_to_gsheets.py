import re
import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials

URL = "https://veritasthca.com/2023/06/17/live-rosin-menu/"
SPREADSHEET_ID = "17goBwXxZlBoLlOa9astP6uWdF5YS0wBB9mvLN1whaoI"

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# ---------------- auth ----------------

creds = Credentials.from_service_account_file(
    "scraper/credentials.json", scopes=SCOPES
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
        return 0
    if "SOLD OUT" in text.upper():
        return 0
    m = re.search(r"(\d+)", text)
    return int(m.group(1)) if m else 0

def parse_price(text):
    if not text:
        return 0.0
    m = re.search(r"\$([\d.]+)", text)
    return float(m.group(1)) if m else 0.0

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

        # Check if this is a section header
        if len(cells) == 1 or (len(cells) > 0 and "Tier" in clean_text(cells[0]) and clean_text(cells[1]) in ["", "Tier", "Tier Level"]):
            current_section = clean_text(cells[0])
            continue

        name_cell = cells[0]
        name = clean_text(name_cell)
        link = name_cell.find("a")["href"] if name_cell.find("a") else ""

        # Skip header rows and empty rows
        if not name or name.lower() in ["name", "strain"]:
            continue

        tier = clean_text(cells[1]) if len(cells) > 1 else ""
        stock_text = clean_text(cells[2]) if len(cells) > 2 else ""
        price_text = clean_text(cells[4]) if len(cells) > 4 else ""

        stock = parse_grams(stock_text)
        price = parse_price(price_text)

        records.append({
            "id": f"{current_section}|{tier}|{name}".lower().replace(" ", "_"),
            "section": current_section,
            "strain": name,
            "tier": tier,
            "stock": stock,
            "sold_out": "true" if stock == 0 else "false",
            "price": price,
            "link": link,
            "last_seen": datetime.utcnow().isoformat()
        })

    return records

# ---------------- formatting ----------------

def format_sheet(worksheet, num_rows):
    """Apply formatting to make the sheet look better"""
    
    # Freeze header row
    worksheet.freeze(rows=1)
    
    # Format header row - bold, background color, text color
    worksheet.format('A1:I1', {
        "backgroundColor": {"red": 0.2, "green": 0.2, "blue": 0.2},
        "textFormat": {
            "foregroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0},
            "fontSize": 11,
            "bold": True
        },
        "horizontalAlignment": "CENTER"
    })
    
    # Auto-resize columns
    worksheet.columns_auto_resize(0, 8)
    
    # Format sold_out column with conditional colors
    if num_rows > 1:
        # Red background for "true" (sold out)
        worksheet.format(f'F2:F{num_rows}', {
            "backgroundColor": {"red": 1.0, "green": 0.9, "blue": 0.9}
        }, condition={
            "type": "TEXT_CONTAINS",
            "values": [{"userEnteredValue": "true"}]
        })
        
        # Green background for "false" (in stock)
        worksheet.format(f'F2:F{num_rows}', {
            "backgroundColor": {"red": 0.9, "green": 1.0, "blue": 0.9}
        }, condition={
            "type": "TEXT_CONTAINS",
            "values": [{"userEnteredValue": "false"}]
        })
        
        # Format stock numbers with color coding
        # Red for 0
        worksheet.format(f'E2:E{num_rows}', {
            "backgroundColor": {"red": 1.0, "green": 0.8, "blue": 0.8},
            "textFormat": {"bold": True}
        }, condition={
            "type": "NUMBER_EQ",
            "values": [{"userEnteredValue": "0"}]
        })
        
        # Yellow for low stock (1-20)
        worksheet.format(f'E2:E{num_rows}', {
            "backgroundColor": {"red": 1.0, "green": 1.0, "blue": 0.8}
        }, condition={
            "type": "NUMBER_BETWEEN",
            "values": [
                {"userEnteredValue": "1"},
                {"userEnteredValue": "20"}
            ]
        })
        
        # Center align specific columns
        worksheet.format(f'D2:G{num_rows}', {
            "horizontalAlignment": "CENTER"
        })
        
        # Format price as currency
        worksheet.format(f'G2:G{num_rows}', {
            "numberFormat": {
                "type": "CURRENCY",
                "pattern": "$#,##0.00"
            }
        })

# ---------------- gsheets sync ----------------

def update_sheets(records):
    timestamp = datetime.utcnow().isoformat()

    # Get old data
    try:
        old_data = current_ws.get_all_records()
    except Exception:
        old_data = []

    old_dict = {r["id"]: r for r in old_data if "id" in r}
    new_dict = {r["id"]: r for r in records}

    changelog_rows = []

    # NEW items
    for item_id, item in new_dict.items():
        if item_id not in old_dict:
            changelog_rows.append([
                timestamp, "NEW_ITEM", item["strain"], item["link"], "", "", ""
            ])

    # REMOVED items
    for item_id, item in old_dict.items():
        if item_id not in new_dict:
            changelog_rows.append([
                timestamp, "REMOVED", item["strain"], item.get("link", ""), "", "", ""
            ])

    # CHANGED items
    for item_id in set(old_dict.keys()) & set(new_dict.keys()):
        old_item = old_dict[item_id]
        new_item = new_dict[item_id]
        
        for field in ["stock", "price", "sold_out"]:
            old_val = str(old_item.get(field, ""))
            new_val = str(new_item.get(field, ""))
            if old_val != new_val:
                changelog_rows.append([
                    timestamp, "FIELD_CHANGE", new_item["strain"],
                    new_item["link"], field, old_val, new_val
                ])

    # Overwrite current menu
    current_ws.clear()
    
    # Write headers
    headers = ["ID", "Section", "Strain", "Tier", "Stock (g)", "Sold Out", "Price", "Link", "Last Seen"]
    current_ws.append_row(headers)
    
    # Write data rows
    data_rows = []
    for record in records:
        row = [
            record["id"],
            record["section"],
            record["strain"],
            record["tier"],
            record["stock"],
            record["sold_out"],
            record["price"],
            record["link"],
            record["last_seen"]
        ]
        data_rows.append(row)
    
    if data_rows:
        current_ws.append_rows(data_rows)
        
    # Apply formatting
    format_sheet(current_ws, len(data_rows) + 1)

    # Append changelog
    if changelog_rows:
        if not changelog_ws.get_all_values():
            changelog_ws.append_row([
                "Timestamp", "Change Type", "Strain",
                "Link", "Field", "Old Value", "New Value"
            ])
            # Format changelog header
            changelog_ws.format('A1:G1', {
                "backgroundColor": {"red": 0.2, "green": 0.2, "blue": 0.2},
                "textFormat": {
                    "foregroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0},
                    "fontSize": 11,
                    "bold": True
                },
                "horizontalAlignment": "CENTER"
            })
            changelog_ws.freeze(rows=1)
            
        changelog_ws.append_rows(changelog_rows)
        changelog_ws.columns_auto_resize(0, 6)

# ---------------- main ----------------

def main():
    records = fetch_menu()
    print(f"Fetched {len(records)} menu items")
    update_sheets(records)
    print("Successfully updated spreadsheet with formatting!")

if __name__ == "__main__":
    main()
