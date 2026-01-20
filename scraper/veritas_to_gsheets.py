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

REQUEST_DELAY = 1.1
MIN_REQUEST_INTERVAL = 1.1
_last_api_call = 0

def rate_limited_call(func):
    """Decorator to enforce rate limiting on all API calls"""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        global _last_api_call
        current_time = time.time()
        time_since_last_call = current_time - _last_api_call
        
        if time_since_last_call < MIN_REQUEST_INTERVAL:
            sleep_time = MIN_REQUEST_INTERVAL - time_since_last_call
            print(f"  [Rate limit] Waiting {sleep_time:.2f}s before next API call...")
            time.sleep(sleep_time)
        
        _last_api_call = time.time()
        return func(*args, **kwargs)
    
    return wrapper

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
sh = wrap_spreadsheet_methods(sh)

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
    m = re.search(r"(\d+)\s*g", text, re.IGNORECASE)
    if m:
        grams = int(m.group(1))
        return "SOLD OUT" if grams == 0 else f"{grams}g"
    return "SOLD OUT"

def parse_moq(text):
    """Parse minimum order quantity"""
    if not text or "SOLD OUT" in text.upper():
        return ""
    m = re.search(r"(\d+)\s*g", text, re.IGNORECASE)
    return f"{m.group(1)}g" if m else ""

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
    
    max_length = max(len(str(v)) for v in values)
    width = (max_length * 8) + padding
    
    return max(min_width, min(width, max_width))

# ---------------- scrape ----------------

def extract_lab_from_section(section_text):
    """Extract lab name from section header"""
    section_lower = section_text.lower()
    
    if "socal" in section_lower:
        return "SOCAL Lab"
    elif "vegas" in section_lower:
        return "Vegas Lab"
    elif "tier 3" in section_lower or ("oc" in section_lower and "lv" in section_lower):
        return "OC + Vegas Lab"
    else:
        return "OC Lab"

def is_section_header(cells):
    """Determine if this row is a section header"""
    if len(cells) == 0:
        return False
    
    first_cell_text = clean_text(cells[0]).lower()
    second_cell_text = clean_text(cells[1]).lower() if len(cells) > 1 else ""
    
    # Check for explicit tier markers in first cell
    if any(marker in first_cell_text for marker in ["tier 1", "tier 2", "tier 3", "tier 4"]):
        # Make sure second cell is empty or contains tier-related text (not actual data)
        if second_cell_text in ["", "tier", "tier level", "tier 1", "tier 2", "tier 3"]:
            return True
    
    # Additional check: if first cell has strong header indicators like "lab" 
    if any(indicator in first_cell_text for indicator in ["socal lab", "vegas lab", "lv + oc"]):
        return True
    
    return False

def normalize_strain_name(name_text):
    """Extract and normalize strain name, handling 'Exotic' suffix"""
    # Remove 'Exotic' if it's at the end
    name = re.sub(r'\s+Exotic\s*$', '', name_text, flags=re.IGNORECASE).strip()
    return name

def get_tier_sort_key(tier_text):
    """Generate sort key for tier ordering: T1 Exotic > T1 > T2 > T3"""
    tier_lower = tier_text.lower()
    
    if "tier 1" in tier_lower and "exotic" in tier_lower:
        return 0
    elif "tier 1" in tier_lower:
        return 1
    elif "tier 2" in tier_lower:
        return 2
    elif "tier 3" in tier_lower:
        return 3
    elif "tier 4" in tier_lower:
        return 4
    else:
        return 99  # Unknown tiers go last

def get_lab_sort_key(lab_text):
    """Generate sort key for lab ordering: OC > SOCAL > Vegas > OC+Vegas"""
    lab_lower = lab_text.lower()
    
    if "oc lab" in lab_lower and "vegas" not in lab_lower:
        return 0  # OC Lab first
    elif "socal" in lab_lower:
        return 1  # SOCAL Lab second
    elif "vegas" in lab_lower and "oc" not in lab_lower:
        return 2  # Vegas Lab third
    elif "oc + vegas" in lab_lower or "vegas + oc" in lab_lower:
        return 3  # OC + Vegas Lab last
    else:
        return 99  # Unknown labs go last

def sort_records(records):
    """Sort records by tier (T1 Exotic > T1 > T2 > T3), then lab (OC > SOCAL > Vegas), then alphabetically by strain"""
    return sorted(records, key=lambda r: (get_tier_sort_key(r["tier"]), get_lab_sort_key(r["lab"]), r["strain"].lower()))

def fetch_menu():
    soup = BeautifulSoup(requests.get(URL).text, "html.parser")
    table = soup.select_one("figure table")
    rows = table.select("tr")

    records = []
    current_section = "Unknown"
    current_lab = "Unknown"

    for row in rows:
        cells = row.find_all("td")
        if not cells:
            continue

        # Check if this is a section header
        if is_section_header(cells):
            current_section = clean_text(cells[0])
            current_lab = extract_lab_from_section(current_section)
            print(f"  Found section: {current_section} ({current_lab})")
            continue

        name_cell = cells[0]
        raw_name = clean_text(name_cell)
        link = name_cell.find("a")["href"] if name_cell.find("a") else ""

        # Skip header rows and empty rows
        if not raw_name or raw_name.lower() in ["name", "strain"]:
            continue
        
        # Skip rows that look like headers but weren't caught by is_section_header
        # (e.g., rows with tier markers but no actual strain data)
        if any(marker in raw_name.lower() for marker in ["tier 1", "tier 2", "tier 3", "tier 4"]) and not link:
            continue

        # Normalize strain name
        strain = normalize_strain_name(raw_name)
        
        tier = clean_text(cells[1]) if len(cells) > 1 else ""
        stock_text = clean_text(cells[2]) if len(cells) > 2 else ""
        moq_text = clean_text(cells[3]) if len(cells) > 3 else ""
        price_text = clean_text(cells[4]) if len(cells) > 4 else ""

        stock = parse_grams(stock_text)
        moq = parse_moq(moq_text)
        price = parse_price(price_text)

        # Create unique ID: lab + tier + strain name
        # This ensures same strain in different tiers/labs are tracked separately
        item_id = f"{current_lab}|{tier}|{strain}".lower().replace(" ", "_")

        records.append({
            "id": item_id,
            "section": current_section,
            "strain": strain,
            "tier": tier,
            "stock": stock,
            "moq": moq,
            "price": price,
            "lab": current_lab if current_lab != "Unknown" else "OC Lab",
            "link": link,
            "last_seen": format_timestamp()
        })

    # Sort records before returning
    return sort_records(records)

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
        
        # Center specific columns (adjust indices based on headers)
        print("Centering columns...")
        worksheet.format(f'B2:B{num_rows}', {"horizontalAlignment": "CENTER"})  # Stock
        worksheet.format(f'C2:C{num_rows}', {"horizontalAlignment": "CENTER"})  # Tier
        worksheet.format(f'D2:D{num_rows}', {"horizontalAlignment": "CENTER"})  # MOQ
        worksheet.format(f'E2:E{num_rows}', {"horizontalAlignment": "CENTER"})  # Lab
        
        print("Formatting strain links...")
        worksheet.format(f'A2:A{num_rows}', {
            "textFormat": {
                "foregroundColor": {"red": 0.06, "green": 0.4, "blue": 0.8},
                "underline": True
            }
        })
        
        print("Formatting prices...")
        worksheet.format(f'F2:F{num_rows}', {
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
                timestamp, "NEW_ITEM", item["strain"], item["link"], 
                item["tier"], item["lab"], "", "", item["stock"]
            ])

    # REMOVED items
    for item_id, item in old_dict.items():
        if item_id not in new_dict:
            changelog_rows.append([
                timestamp, "REMOVED", item.get("strain", ""), item.get("link", ""),
                item.get("tier", ""), item.get("lab", ""), "", "", ""
            ])

    # CHANGED items
    for item_id in set(old_dict.keys()) & set(new_dict.keys()):
        old_item = old_dict[item_id]
        new_item = new_dict[item_id]
        
        # Track all fields that can change
        for field in ["stock", "moq", "price"]:
            old_val = str(old_item.get(field, ""))
            new_val = str(new_item.get(field, ""))
            
            # Normalize for comparison
            if field == "price":
                try:
                    old_val_normalized = f"{float(old_val):.2f}" if old_val else "0.00"
                    new_val_normalized = f"{float(new_val):.2f}" if new_val else "0.00"
                except:
                    old_val_normalized = old_val
                    new_val_normalized = new_val
            else:
                old_val_normalized = old_val
                new_val_normalized = new_val
            
            if old_val_normalized != new_val_normalized:
                changelog_rows.append([
                    timestamp, "FIELD_CHANGE", new_item["strain"],
                    new_item["link"], new_item["tier"], new_item["lab"],
                    field, old_val, new_val
                ])

    # Overwrite current menu
    print("Clearing current menu...")
    current_ws.clear()
    
    # Write headers - now includes MOQ
    headers = ["Strain", "Stock", "Tier", "MOQ", "Lab", "Price", "Last Seen"]
    print("Writing headers...")
    current_ws.append_row(headers)
    
    # Write data rows with hyperlinks embedded
    print(f"Writing {len(records)} rows with hyperlinks...")
    data_rows = []
    sold_out_rows = []  # Track which rows are sold out for strikethrough formatting
    
    for idx, record in enumerate(records):
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
            record["moq"],
            record["lab"],
            record["price"],
            record["last_seen"]
        ]
        data_rows.append(row)
        
        # Track if this row is sold out (row number is idx + 2 because of header)
        if record["stock"] == "SOLD OUT":
            sold_out_rows.append(idx + 2)
    
    if data_rows:
        current_ws.append_rows(data_rows, value_input_option='USER_ENTERED')
        
    # Apply formatting with dynamic column widths
    print("Applying formatting...")
    format_sheet_dynamic(current_ws, headers, data_rows)
    
    # Apply strikethrough to sold out items
    if sold_out_rows:
        print(f"Applying strikethrough to {len(sold_out_rows)} sold out items...")
        for row_num in sold_out_rows:
            current_ws.format(f'A{row_num}:G{row_num}', {
                "textFormat": {
                    "strikethrough": True
                }
            })
            # Keep the hyperlink formatting on strain column
            current_ws.format(f'A{row_num}', {
                "textFormat": {
                    "foregroundColor": {"red": 0.06, "green": 0.4, "blue": 0.8},
                    "underline": True,
                    "strikethrough": True
                }
            })

    # Update changelog
    print("Updating changelog...")
    existing_changelog = changelog_ws.get_all_values()
    
    changelog_headers = ["Strain", "Tier", "Lab", "Status", "Timestamp"]
    
    # Always ensure headers are present and formatted
    if not existing_changelog:
        changelog_ws.clear()
        changelog_ws.append_row(changelog_headers)
        
        changelog_ws.format('A1:E1', {
            "backgroundColor": {"red": 0.2, "green": 0.2, "blue": 0.2},
            "textFormat": {
                "foregroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0},
                "fontSize": 11,
                "bold": True
            },
            "horizontalAlignment": "CENTER"
        })
        
        changelog_ws.freeze(rows=1)
    
    # Append changelog entries if there are any changes
    if changelog_rows:
        current_row_count = len(changelog_ws.get_all_values())
        
        print(f"Preparing {len(changelog_rows)} changelog entries with hyperlinks...")
        formatted_changelog = []
        for row in changelog_rows:
            timestamp = row[0]
            change_type = row[1]
            strain_name = row[2]
            link_url = row[3]
            tier = row[4]
            lab = row[5]
            field = row[6]
            old_val = row[7]
            new_val = row[8]
            
            # Create status message with more context
            if change_type == "NEW_ITEM":
                status = f"🆕 NEW ITEM - Stock: {new_val}"
            elif change_type == "REMOVED":
                status = "🗑️ REMOVED"
            elif change_type == "FIELD_CHANGE":
                if field == "stock":
                    if new_val == "SOLD OUT":
                        status = f"⛔ SOLD OUT (was {old_val})"
                    elif old_val == "SOLD OUT":
                        status = f"✅ BACK IN STOCK - {new_val}"
                    else:
                        status = f"📊 STOCK: {old_val} → {new_val}"
                elif field == "price":
                    status = f"💰 PRICE: ${old_val} → ${new_val}"
                elif field == "moq":
                    status = f"📦 MOQ: {old_val} → {new_val}"
                else:
                    status = f"{field.upper()}: {old_val} → {new_val}"
            else:
                status = change_type
            
            # Create hyperlink for strain
            if link_url and strain_name:
                strain_escaped = strain_name.replace('"', '""')
                strain_formula = f'=HYPERLINK("{link_url}","{strain_escaped}")'
            else:
                strain_formula = strain_name
            
            # Format: [Strain, Tier, Lab, Status, Timestamp]
            formatted_changelog.append([strain_formula, tier, lab, status, timestamp])
        
        # Sort changelog by timestamp (most recent first)
        # Extract timestamps and sort in reverse chronological order
        formatted_changelog.sort(key=lambda x: x[4], reverse=True)
        
        # Append the changelog rows with formulas
        changelog_ws.append_rows(formatted_changelog, value_input_option='USER_ENTERED')
        
        # Format strain column as blue hyperlinks
        new_row_count = current_row_count + len(formatted_changelog)
        changelog_ws.format(f'A2:A{new_row_count}', {
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
        
        print(f"✅ Logged {len(changelog_rows)} changes to changelog")

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
