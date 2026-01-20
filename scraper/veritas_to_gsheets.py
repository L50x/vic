import re
import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials
import time

URL = "https://veritasthca.com/2023/06/17/live-rosin-menu/"
SPREADSHEET_ID = "17goBwXxZlBoLlOa9astP6uWdF5YS0wBB9mvLN1whaoI"

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# Rate limiting: 60 write requests per minute = 1 request per second safe rate
REQUEST_DELAY = 1.5  # seconds between requests

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

time.sleep(REQUEST_DELAY)

try:
    current_ws = sh.worksheet("current_menu")
    current_ws.update_title("Current Menu")
except gspread.WorksheetNotFound:
    try:
        current_ws = sh.worksheet("Current Menu")
    except gspread.WorksheetNotFound:
        current_ws = sh.add_worksheet("Current Menu", rows=1000, cols=10)

time.sleep(REQUEST_DELAY)

sh.reorder_worksheets([changelog_ws, current_ws])
time.sleep(REQUEST_DELAY)

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

def format_sheet_simple(worksheet, num_rows):
    """Apply simple formatting without conditional coloring to avoid rate limits"""
    
    print("Freezing header row...")
    worksheet.freeze(rows=1)
    time.sleep(REQUEST_DELAY)
    
    print("Formatting header...")
    worksheet.format('A1:E1', {
        "backgroundColor": {"red": 0.2, "green": 0.2, "blue": 0.2},
        "textFormat": {
            "foregroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0},
            "fontSize": 11,
            "bold": True
        },
        "horizontalAlignment": "CENTER"
    })
    time.sleep(REQUEST_DELAY)
    
    print("Auto-resizing columns...")
    worksheet.columns_auto_resize(0, 4)
    time.sleep(REQUEST_DELAY)
    
    if num_rows > 1:
        print("Centering stock column...")
        worksheet.format(f'B2:B{num_rows}', {
            "horizontalAlignment": "CENTER"
        })
        time.sleep(REQUEST_DELAY)
        
        print("Centering tier column...")
        worksheet.format(f'C2:C{num_rows}', {
            "horizontalAlignment": "CENTER"
        })
        time.sleep(REQUEST_DELAY)
        
        print("Formatting strain links...")
        worksheet.format(f'A2:A{num_rows}', {
            "textFormat": {
                "foregroundColor": {"red": 0.06, "green": 0.4, "blue": 0.8},
                "underline": True
            }
        })
        time.sleep(REQUEST_DELAY)
        
        print("Formatting prices...")
        worksheet.format(f'D2:D{num_rows}', {
            "numberFormat": {
                "type": "CURRENCY",
                "pattern": "$#,##0.00"
            },
            "horizontalAlignment": "CENTER"
        })
        time.sleep(REQUEST_DELAY)

# ---------------- gsheets sync ----------------

def update_sheets(records):
    timestamp = datetime.utcnow().isoformat()

    # Get old data
    print("Reading existing data...")
    try:
        old_data = current_ws.get_all_records()
    except Exception:
        old_data = []
    
    time.sleep(REQUEST_DELAY)

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
    print("Clearing current menu...")
    current_ws.clear()
    time.sleep(REQUEST_DELAY)
    
    # Write headers
    headers = ["Strain", "Stock", "Tier", "Price", "Last Seen"]
    print("Writing headers...")
    current_ws.append_row(headers)
    time.sleep(REQUEST_DELAY)
    
    # Write data rows
    data_rows = []
    for record in records:
        row = [
            record["strain"],
            record["stock"],
            record["tier"],
            record["price"],
            record["last_seen"]
        ]
        data_rows.append(row)
    
    if data_rows:
        print(f"Writing {len(data_rows)} rows...")
        current_ws.append_rows(data_rows)
        time.sleep(REQUEST_DELAY)
        
        # Batch update strain names to hyperlinks
        print("Creating hyperlinks...")
        for i, record in enumerate(records, start=2):
            if record["link"]:
                # Escape any double quotes in the strain name
                strain_escaped = record["strain"].replace('"', '""')
                formula = f'=HYPERLINK("{record["link"]}","{strain_escaped}")'
                current_ws.update(
                    f'A{i}',
                    [[formula]],
                    value_input_option='USER_ENTERED'
                )
                time.sleep(0.15)  # Small delay between updates
        time.sleep(REQUEST_DELAY)
        
    # Apply formatting
    print("Applying formatting...")
    format_sheet_simple(current_ws, len(data_rows) + 1)

    # Append changelog
    if changelog_rows:
        print("Updating changelog...")
        if not changelog_ws.get_all_values():
            changelog_ws.append_row([
                "Timestamp", "Change Type", "Strain",
                "Link", "Field", "Old Value", "New Value"
            ])
            time.sleep(REQUEST_DELAY)
            
            changelog_ws.format('A1:G1', {
                "backgroundColor": {"red": 0.2, "green": 0.2, "blue": 0.2},
                "textFormat": {
                    "foregroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0},
                    "fontSize": 11,
                    "bold": True
                },
                "horizontalAlignment": "CENTER"
            })
            time.sleep(REQUEST_DELAY)
            
            changelog_ws.freeze(rows=1)
            time.sleep(REQUEST_DELAY)
            
        changelog_ws.append_rows(changelog_rows)
        time.sleep(REQUEST_DELAY)
        
        changelog_ws.columns_auto_resize(0, 6)
        time.sleep(REQUEST_DELAY)

# ---------------- main ----------------

def main():
    print("Fetching menu from website...")
    records = fetch_menu()
    print(f"Fetched {len(records)} menu items")
    
    print("Updating Google Sheets...")
    update_sheets(records)
    
    print("✅ Successfully updated spreadsheet!")

if __name__ == "__main__":
    main()
