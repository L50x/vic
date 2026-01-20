import re
import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials
import time
import functools

URL = "https://veritasthca.com/2023/06/17/live-rosin-menu/"
SPREADSHEET_ID = "17goBwXxZlBoLlOa9astP6uWdF5YS0wBB9mvLN1whaoI"

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# Rate limiting: 60 write requests per minute = 1 request per second
# Using 1.1 seconds to be safe (54 requests/minute max)
REQUEST_DELAY = 1.1  # seconds between requests
MIN_REQUEST_INTERVAL = 1.1  # minimum time between any API calls

# Track last API call time globally
_last_api_call = 0

def rate_limited_call(func):
    """Decorator to enforce rate limiting on all API calls"""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        global _last_api_call
        
        # Calculate time since last call
        current_time = time.time()
        time_since_last_call = current_time - _last_api_call
        
        # If not enough time has passed, wait
        if time_since_last_call < MIN_REQUEST_INTERVAL:
            sleep_time = MIN_REQUEST_INTERVAL - time_since_last_call
            print(f"  [Rate limit] Waiting {sleep_time:.2f}s before next API call...")
            time.sleep(sleep_time)
        
        # Update last call time
        _last_api_call = time.time()
        
        # Execute the function
        return func(*args, **kwargs)
    
    return wrapper

# Wrap all gspread methods that make API calls
def wrap_worksheet_methods(worksheet):
    """Wrap all API-calling methods of a worksheet with rate limiting"""
    api_methods = [
        'update', 'append_row', 'append_rows', 'clear', 'format', 
        'update_title', 'get_all_records', 'get_all_values', 
        'batch_update', 'freeze'
    ]
    
    for method_name in api_methods:
        if hasattr(worksheet, method_name):
            original_method = getattr(worksheet, method_name)
            setattr(worksheet, method_name, rate_limited_call(original_method))
    
    return worksheet

def wrap_spreadsheet_methods(spreadsheet):
    """Wrap spreadsheet batch_update with rate limiting"""
    if hasattr(spreadsheet, 'batch_update'):
        spreadsheet.batch_update = rate_limited_call(spreadsheet.batch_update)
    return spreadsheet

# ---------------- auth ----------------

print("Authenticating with Google Sheets...")
creds = Credentials.from_service_account_file(
    "scraper/credentials.json", scopes=SCOPES
)
gc = gspread.authorize(creds)
sh = gc.open_by_key(SPREADSHEET_ID)

# Wrap spreadsheet methods
sh = wrap_spreadsheet_methods(sh)

# Ensure sheet order and rename sheets
print("Setting up worksheets...")
try:
    changelog_ws = sh.worksheet("changelog")
    changelog_ws = wrap_worksheet_methods(changelog_ws)
    changelog_ws.update_title("Changelog")
except gspread.WorksheetNotFound:
    try:
        changelog_ws = sh.worksheet("Changelog")
        changelog_ws = wrap_worksheet_methods(changelog_ws)
    except gspread.WorksheetNotFound:
        changelog_ws = sh.add_worksheet("Changelog", rows=1000, cols=10)
        changelog_ws = wrap_worksheet_methods(changelog_ws)

try:
    current_ws = sh.worksheet("current_menu")
    current_ws = wrap_worksheet_methods(current_ws)
    current_ws.update_title("Current Menu")
except gspread.WorksheetNotFound:
    try:
        current_ws = sh.worksheet("Current Menu")
        current_ws = wrap_worksheet_methods(current_ws)
    except gspread.WorksheetNotFound:
        current_ws = sh.add_worksheet("Current Menu", rows=1000, cols=10)
        current_ws = wrap_worksheet_methods(current_ws)

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

def format_timestamp(dt=None):
    """Format timestamp without seconds/milliseconds"""
    if dt is None:
        dt = datetime.utcnow()
    return dt.strftime("%Y-%m-%d %H:%M")

def calculate_column_width(values, min_width=80, max_width=400, padding=20):
    """Calculate optimal column width based on content length"""
    if not values:
        return min_width
    
    # Find the longest string
    max_length = max(len(str(v)) for v in values)
    
    # Approximate 7-8 pixels per character, plus padding
    width = (max_length * 8) + padding
    
    # Clamp between min and max
    return max(min_width, min(width, max_width))

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
            "last_seen": format_timestamp()
        })

    return records

# ---------------- formatting ----------------

def format_sheet_dynamic(worksheet, headers, data_rows):
    """Apply formatting with dynamically calculated column widths"""
    
    print("Freezing header row...")
    worksheet.freeze(rows=1)
    
    print("Formatting header...")
    header_range = f'A1:{chr(64 + len(headers))}1'
    worksheet.format(header_range, {
        "backgroundColor": {"red": 0.2, "green": 0.2, "blue": 0.2},
        "textFormat": {
            "foregroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0},
            "fontSize": 11,
            "bold": True
        },
        "horizontalAlignment": "CENTER"
    })
    
    # Calculate column widths dynamically
    print("Calculating column widths...")
    column_widths = []
    for col_idx in range(len(headers)):
        # Collect all values in this column (header + data)
        column_values = [headers[col_idx]]
        if data_rows:
            column_values.extend([row[col_idx] for row in data_rows])
        
        width = calculate_column_width(column_values)
        column_widths.append(width)
    
    # Apply column widths
    print("Auto-resizing columns...")
    requests_body = {'requests': []}
    for col_idx, width in enumerate(column_widths):
        requests_body['requests'].append({
            'updateDimensionProperties': {
                'range': {
                    'sheetId': worksheet.id,
                    'dimension': 'COLUMNS',
                    'startIndex': col_idx,
                    'endIndex': col_idx + 1
                },
                'properties': {'pixelSize': width},
                'fields': 'pixelSize'
            }
        })
    
    worksheet.spreadsheet.batch_update(requests_body)
    
    if data_rows:
        num_rows = len(data_rows) + 1
        
        print("Centering stock column...")
        worksheet.format(f'B2:B{num_rows}', {
            "horizontalAlignment": "CENTER"
        })
        
        print("Centering tier column...")
        worksheet.format(f'C2:C{num_rows}', {
            "horizontalAlignment": "CENTER"
        })
        
        print("Formatting strain links...")
        worksheet.format(f'A2:A{num_rows}', {
            "textFormat": {
                "foregroundColor": {"red": 0.06, "green": 0.4, "blue": 0.8},
                "underline": True
            }
        })
        
        print("Formatting prices...")
        worksheet.format(f'D2:D{num_rows}', {
            "numberFormat": {
                "type": "CURRENCY",
                "pattern": "$#,##0.00"
            },
            "horizontalAlignment": "CENTER"
        })

# ---------------- gsheets sync ----------------

def update_sheets(records):
    timestamp = format_timestamp()

    # Get old data
    print("Reading existing data...")
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
    print("Clearing current menu...")
    current_ws.clear()
    
    # Write headers
    headers = ["Strain", "Stock", "Tier", "Price", "Last Seen"]
    print("Writing headers...")
    current_ws.append_row(headers)
    
    # Write data rows with hyperlinks embedded
    print(f"Writing {len(records)} rows with hyperlinks...")
    data_rows = []
    for record in records:
        # Create hyperlink formula for strain if link exists
        if record["link"]:
            strain_escaped = record["strain"].replace('"', '""')
            strain_value = f'=HYPERLINK("{record["link"]}","{strain_escaped}")'
        else:
            strain_value = record["strain"]
            
        row = [
            strain_value,
            record["stock"],
            record["tier"],
            record["price"],
            record["last_seen"]
        ]
        data_rows.append(row)
    
    if data_rows:
        # Batch insert all rows at once with formulas
        current_ws.append_rows(data_rows, value_input_option='USER_ENTERED')
        
    # Apply formatting with dynamic column widths
    print("Applying formatting...")
    format_sheet_dynamic(current_ws, headers, data_rows)

    # Append changelog
    if changelog_rows:
        print("Updating changelog...")
        existing_changelog = changelog_ws.get_all_values()
        
        changelog_headers = [
            "Timestamp", "Change Type", "Strain",
            "Link", "Field", "Old Value", "New Value"
        ]
        
        if not existing_changelog:
            # Add header row
            changelog_ws.append_row(changelog_headers)
            
            # Format changelog header to match current menu
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
        
        # Get the current row count to know where to add hyperlinks
        current_row_count = len(changelog_ws.get_all_values())
        
        # Add hyperlinks to strain names in changelog rows before appending
        print(f"Preparing {len(changelog_rows)} changelog entries with hyperlinks...")
        for row in changelog_rows:
            strain_name = row[2]  # Strain is column C (index 2)
            link_url = row[3]     # Link is column D (index 3)
            
            if link_url and strain_name:
                strain_escaped = strain_name.replace('"', '""')
                formula = f'=HYPERLINK("{link_url}","{strain_escaped}")'
                row[2] = formula  # Replace strain name with hyperlink formula
        
        # Append the changelog rows with formulas
        changelog_ws.append_rows(changelog_rows, value_input_option='USER_ENTERED')
        
        # Format strain column as blue hyperlinks
        new_row_count = current_row_count + len(changelog_rows)
        changelog_ws.format(f'C2:C{new_row_count}', {
            "textFormat": {
                "foregroundColor": {"red": 0.06, "green": 0.4, "blue": 0.8},
                "underline": True
            }
        })
        
        # Calculate and set changelog column widths dynamically
        print("Resizing changelog columns...")
        all_changelog_data = changelog_ws.get_all_values()
        
        changelog_column_widths = []
        for col_idx in range(len(changelog_headers)):
            column_values = [row[col_idx] for row in all_changelog_data if len(row) > col_idx]
            width = calculate_column_width(column_values)
            changelog_column_widths.append(width)
        
        requests_body = {'requests': []}
        for col_idx, width in enumerate(changelog_column_widths):
            requests_body['requests'].append({
                'updateDimensionProperties': {
                    'range': {
                        'sheetId': changelog_ws.id,
                        'dimension': 'COLUMNS',
                        'startIndex': col_idx,
                        'endIndex': col_idx + 1
                    },
                    'properties': {'pixelSize': width},
                    'fields': 'pixelSize'
                }
            })
        
        changelog_ws.spreadsheet.batch_update(requests_body)

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
