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

# Ensure sheet order and rename sheets
try:
    changelog_ws = sh.worksheet("changelog")
    changelog_ws.update_title("Changelog")
except gspread.WorksheetNotFound:
    try:
        changelog_ws = sh.worksheet("Changelog")
    except gspread.WorksheetNotFound:
        changelog_ws = sh.add_worksheet("Changelog", rows=1000, cols=10)

try:
    current_ws = sh.worksheet("current_menu")
    current_ws.update_title("Current Menu")
except gspread.WorksheetNotFound:
    try:
        current_ws = sh.worksheet("Current Menu")
    except gspread.WorksheetNotFound:
        current_ws = sh.add_worksheet("Current Menu", rows=1000, cols=10)

sh.reorder_worksheets([changelog_ws, current_ws])

# ---------------- helpers ----------------

def clean_text(el):
    return el.get_text(" ", strip=True) if el else ""

def parse_grams(text):
    if not text:
        return "SOLD OUT"
    if "SOLD OUT" in text.upper():
        return "SOLD OUT"
    m = re.search(r"(\d+)", text)
    if m:
        grams = int(m.group(1))
        return "SOLD OUT" if grams == 0 else f"{grams}g"
    return "SOLD OUT"

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
    worksheet.format('A1:E1', {
        "backgroundColor": {"red": 0.2, "green": 0.2, "blue": 0.2},
        "textFormat": {
            "foregroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0},
            "fontSize": 11,
            "bold": True
        },
        "horizontalAlignment": "CENTER"
    })
    
    # Auto-resize columns
    worksheet.columns_auto_resize(0, 4)
    
    if num_rows > 1:
        # Batch format stock cells based on their values
        sold_out_cells = []
        low_stock_cells = []
        normal_stock_cells = []
        
        # Read all stock values at once
        stock_values = worksheet.batch_get([f'B2:B{num_rows}'])[0]
        
        for row_num, cell_data in enumerate(stock_values, start=2):
            if not cell_data:
                continue
            cell_value = cell_data[0] if cell_data else ""
            
            if cell_value == "SOLD OUT":
                sold_out_cells.append(f'B{row_num}')
            elif cell_value and 'g' in str(cell_value):
                try:
                    stock_num = int(str(cell_value).replace('g', ''))
                    if 1 <= stock_num <= 20:
                        low_stock_cells.append(f'B{row_num}')
                    else:
                        normal_stock_cells.append(f'B{row_num}')
                except:
                    normal_stock_cells.append(f'B{row_num}')
        
        # Batch format sold out cells
        if sold_out_cells:
            for cell in sold_out_cells:
                worksheet.format(cell, {
                    "backgroundColor": {"red": 1.0, "green": 0.8, "blue": 0.8},
                    "textFormat": {"bold": True, "foregroundColor": {"red": 0.8, "green": 0.0, "blue": 0.0}},
                    "horizontalAlignment": "CENTER"
                })
        
        # Batch format low stock cells
        if low_stock_cells:
            for cell in low_stock_cells:
                worksheet.format(cell, {
                    "backgroundColor": {"red": 1.0, "green": 1.0, "blue": 0.8},
                    "horizontalAlignment": "CENTER"
                })
        
        # Batch format normal stock cells
        if normal_stock_cells:
            for cell in normal_stock_cells:
                worksheet.format(cell, {
                    "horizontalAlignment": "CENTER"
                })
        
        # Center align tier and price columns
        worksheet.format(f'C2:C{num_rows}', {
            "horizontalAlignment": "CENTER"
        })
        
        # Format strain column (A) as blue hyperlinks
        worksheet.format(f'A2:A{num_rows}', {
            "textFormat": {
                "foregroundColor": {"red": 0.06, "green": 0.4, "blue": 0.8},
                "underline": True
            }
        })
        
        # Format price as currency
        worksheet.format(f'D2:D{num_rows}', {
            "numberFormat": {
                "type": "CURRENCY",
                "pattern": "$#,##0.00"
            },
            "horizontalAlignment": "CENTER"
        })

# ---------------- gsheets sync ----------------

def update_sheets(records):
    timestamp = datetime.utcnow().isoformat()

    # Get old data
    try:
        old_data = current_ws.get_all_records()
    except Exception:
        old_data = []

    old_dict = {r.get("id", ""): r for r in old_data if r.get("id")}
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
                timestamp, "REMOVED", item.get("strain", ""), item.get("link", ""), "", "", ""
            ])

    # CHANGED items
    for item_id in set(old_dict.keys()) & set(new_dict.keys()):
        old_item = old_dict[item_id]
        new_item = new_dict[item_id]
        
        for field in ["stock", "price"]:
            old_val = str(old_item.get(field, ""))
            new_val = str(new_item.get(field, ""))
            if old_val != new_val:
                changelog_rows.append([
                    timestamp, "FIELD_CHANGE", new_item["strain"],
                    new_item["link"], field, old_val, new_val
                ])

    # Overwrite current menu
    current_ws.clear()
    
    # Write headers (removed ID, Section, Sold Out, Link)
    headers = ["Strain", "Stock", "Tier", "Price", "Last Seen"]
    current_ws.append_row(headers)
    
    # Write data rows
    data_rows = []
    for record in records:
        row = [
            record["strain"],  # Will be converted to hyperlink below
            record["stock"],
            record["tier"],
            record["price"],
            record["last_seen"]
        ]
        data_rows.append(row)
    
    if data_rows:
        current_ws.append_rows(data_rows)
        
        # Batch update strain names to hyperlinks
        batch_data = []
        for i, record in enumerate(records, start=2):  # Start at row 2 (after header)
            if record["link"]:
                batch_data.append({
                    'range': f'A{i}',
                    'values': [[f'=HYPERLINK("{record["link"]}", "{record["strain"]}")']]
                })
        
        # Update all hyperlinks in one batch request
        if batch_data:
            current_ws.batch_update(batch_data)
        
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
