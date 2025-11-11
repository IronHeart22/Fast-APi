from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
import logging
import os
import json

# -----------------------------
# Config / Environment
# -----------------------------
PORT = int(os.environ.get("PORT", 8000))
# Monthly interest rate (%). Default 1.5 if not provided via env.
MONTHLY_INTEREST_RATE = float(os.environ.get("MONTHLY_INTEREST_RATE", 1.5))
# Default number of columns for created worksheets (increased to allow new columns)
DEFAULT_SHEET_COLS = int(os.environ.get("DEFAULT_SHEET_COLS", 15))

# -----------------------------
# Logging Setup
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# -----------------------------
# FastAPI App
# -----------------------------
app = FastAPI(
    title="Statement of Accounts API",
    version="1.0.0",
    description="API for managing invoices and payments in Google Sheets"
)

# -----------------------------
# Pydantic Models
# -----------------------------
class Invoice(BaseModel):
    # Keep model keys as underscore style — but code accepts both underscore and space keys
    Date_Formatted: Optional[str] = None
    Reference_Number: Optional[str] = None
    Total_Formatted: Optional[str] = None
    Balance_Formatted: Optional[str] = None
    Status: Optional[str] = None
    Age: Optional[int] = None  # days overdue
    Invoice_ID: Optional[str] = None
    Balance_Due: Optional[float] = None


class Payment(BaseModel):
    Payment_ID: Optional[str] = None
    Paid_Amount: Optional[float] = None
    Unused_Amount: Optional[float] = None


class StatementData(BaseModel):
    invoices: List[Dict[str, Any]]
    payments: List[Dict[str, Any]]

# -----------------------------
# Google Sheets Configuration
# -----------------------------
SPREADSHEET_ID = "1-i1iJ_tPviu_KMtS06EtWVS1BBzbUuoEf0DtpkGtmrg"
CREDENTIALS_FILE = "cred.json"  # Primary credentials path

# -----------------------------
# Google Sheets Client
# -----------------------------
def get_google_sheets_client():
    """Initialize Google Sheets client with service account credentials.
    Tries multiple common file paths to find the credential file.
    Returns gspread client or None if not available.
    """
    try:
        SCOPES = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]

        possible_paths = [
            "cred.json",
            "./cred.json",
            "../cred.json",
            "service-account-credentials.json",
            "credentials.json",
            "./credentials/cred.json",
        ]

        logger.info("🔍 Searching for Google Sheets credentials...")
        for credentials_path in possible_paths:
            if os.path.exists(credentials_path):
                try:
                    logger.info(f"📄 Found credentials file at: {credentials_path}")
                    with open(credentials_path, 'r') as f:
                        cred_data = json.load(f)

                    required_fields = ['client_email', 'private_key', 'project_id']
                    missing_fields = [field for field in required_fields if not cred_data.get(field)]
                    if missing_fields:
                        logger.error(f"❌ Credentials file missing fields: {missing_fields}")
                        continue

                    creds = Credentials.from_service_account_file(credentials_path, scopes=SCOPES)
                    client = gspread.authorize(creds)
                    logger.info("✅ Successfully connected to Google Sheets!")
                    logger.info(f"📧 Service Account: {cred_data.get('client_email')}")
                    return client

                except json.JSONDecodeError:
                    logger.error(f"❌ Invalid JSON in credentials file: {credentials_path}")
                    continue
                except Exception as e:
                    logger.error(f"❌ Error loading credentials from {credentials_path}: {str(e)}")
                    continue

        logger.error("❌ No valid Google Sheets credentials found!")
        logger.error("📝 Ensure cred.json exists, is valid, and the sheet is shared with the service account email")
        return None

    except Exception as e:
        logger.error(f"❌ Unexpected error initializing Google Sheets client: {str(e)}")
        return None

# -----------------------------
# Helper Functions
# -----------------------------
def safe_float_conversion(value, default=0.0):
    """Safely convert a value to float (handles strings with currency and commas)."""
    if value is None:
        return default
    try:
        if isinstance(value, str):
            value = value.replace(',', '').replace('₹', '').replace('$', '').strip()
        return float(value)
    except (ValueError, TypeError):
        logger.warning(f"Could not convert '{value}' to float, using default {default}")
        return default


def safe_int_conversion(value, default=0):
    """Safely convert to int (handles strings)."""
    if value is None:
        return default
    try:
        if isinstance(value, str):
            value = value.strip()
        return int(value)
    except (ValueError, TypeError):
        logger.warning(f"Could not convert '{value}' to int, using default {default}")
        return default


def get_invoice_field(inv: Dict[str, Any], *keys, default=None):
    """Return the first existing key from keys in invoice dictionary.
    Helps accept both 'Balance Due' and 'Balance_Due' style keys.
    """
    for k in keys:
        if k in inv and inv[k] not in (None, ""):
            return inv[k]
    return default

# -----------------------------
# Core: prepare_sheet_data
# -----------------------------
def prepare_sheet_data(invoices_data, payments_data, monthly_rate=MONTHLY_INTEREST_RATE):
    """
    Prepare rows for Google Sheets and compute interest & total balance.
    - monthly_rate: percent per month (e.g., 1.5 for 1.5% per month)
    - Interest is computed using monthly compounding:
        interest = balance_due * ((1 + monthly_rate/100) ** (age_days/30) - 1)
    - Age is clamped to >= 0 (negative ages treated as 0)
    - Keeps raw float precision (no rounding / no currency formatting)
    - Accepts invoice keys with spaces or underscores (e.g., 'Balance Due' or 'Balance_Due')
    """
    try:
        if not isinstance(invoices_data, list):
            invoices_data = []
        if not isinstance(payments_data, list):
            payments_data = []

        logger.info(f"📊 Processing {len(invoices_data)} invoices and {len(payments_data)} payments")

        total_balance_due = 0.0
        total_interest = 0.0
        total_total_balance = 0.0

        # Payment totals (safe conversion)
        total_paid_amount = sum(safe_float_conversion(get_invoice_field(p, "Paid Amount", "Paid_Amount", default=0)) for p in payments_data)
        total_unused_amount = sum(safe_float_conversion(get_invoice_field(p, "Unused Amount", "Unused_Amount", default=0)) for p in payments_data)

        # Status counting
        status_counts: Dict[str, int] = {}
        for inv in invoices_data:
            st = str(get_invoice_field(inv, "Status", "Status", default="Unknown")).strip() or "Unknown"
            status_counts[st] = status_counts.get(st, 0) + 1

        rows: List[List[Any]] = []
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Header (human readable)
        rows.extend([
            ["STATEMENT OF ACCOUNTS"],
            [f"Generated on: {current_time}"],
            [f"Interest rate (per month): {monthly_rate}%"],  # show applied monthly rate
            [""],
            ["=" * 80],
            [""],
        ])

        # Invoices Section — we've placed Interest & Total Balance after Balance (as requested)
        rows.extend([
            ["INVOICES SECTION"],
            ["Date", "Reference", "Total", "Balance", "Interest", "Total Balance", "Status", "Age", "Invoice ID", "Balance Due"],
        ])

        for inv in invoices_data:
            # Normalize fields: accept both "Balance Due" and "Balance_Due", etc.
            balance_due = safe_float_conversion(get_invoice_field(inv, "Balance Due", "Balance_Due", default=0))
            age_days = safe_int_conversion(get_invoice_field(inv, "Age", "Age", default=0))
            # Clamp negative ages to 0 to avoid negative compounding
            age_days = max(age_days, 0)

            total_balance_due += balance_due

            # Monthly compound interest computation
            months = age_days / 30.0
            try:
                interest_val = balance_due * ((1 + (monthly_rate / 100.0)) ** months - 1.0)
            except Exception as e:
                logger.warning(f"Could not compute interest for invoice {get_invoice_field(inv, 'Invoice ID', 'Invoice_ID', default='?')}: {e}")
                interest_val = 0.0

            total_balance = balance_due + interest_val

            # Accumulate summary totals
            total_interest += interest_val
            total_total_balance += total_balance

            # Append invoice row: raw floats for interest and total_balance (no rounding/formatting)
            rows.append([
                str(get_invoice_field(inv, "Date Formatted", "Date_Formatted", default="")),
                str(get_invoice_field(inv, "Reference Number", "Reference_Number", default="")),
                str(get_invoice_field(inv, "Total Formatted", "Total_Formatted", default="")),
                str(get_invoice_field(inv, "Balance Formatted", "Balance_Formatted", default="")),
                interest_val,              # raw float
                total_balance,             # raw float
                str(get_invoice_field(inv, "Status", "Status", default="")),
                str(age_days),
                str(get_invoice_field(inv, "Invoice ID", "Invoice_ID", default="")),
                balance_due,
            ])

        # Separator rows
        rows.extend([[""], ["=" * 80], [""]])

        # Payments Section (unchanged format, safe numeric conversion)
        rows.extend([
            ["PAYMENTS SECTION"],
            ["Payment ID", "Paid Amount", "Unused Amount"],
        ])

        for pay in payments_data:
            rows.append([
                str(get_invoice_field(pay, "Payment ID", "Payment_ID", default="")),
                safe_float_conversion(get_invoice_field(pay, "Paid Amount", "Paid_Amount", default=0)),
                safe_float_conversion(get_invoice_field(pay, "Unused Amount", "Unused_Amount", default=0)),
            ])

        rows.extend([[""], ["=" * 80], [""]])

        # Financial Summary — raw floats for numeric values, includes interest totals and applied rate line above
        net_outstanding = total_balance_due - (total_paid_amount - total_unused_amount)
        rows.extend([
            ["FINANCIAL SUMMARY"],
            [""],
            ["Total Balance Due:", total_balance_due],
            ["Total Interest:", total_interest],
            ["Total (Balance + Interest):", total_total_balance],
            ["Total Paid Amount:", total_paid_amount],
            ["Total Unused Amount:", total_unused_amount],
            ["Net Outstanding (Balance - Paid + Unused):", net_outstanding],
            [""],
            ["INVOICE STATUS BREAKDOWN:"],
        ])

        for status, count in status_counts.items():
            rows.append([f"{status}:", count])

        rows.extend([
            [""],
            [f"Total Invoices: {len(invoices_data)}"],
            [f"Total Payments: {len(payments_data)}"],
            [f"Total Records: {len(invoices_data) + len(payments_data)}"],
            [""],
            ["=" * 80],
        ])

        summary = {
            "total_balance_due": total_balance_due,
            "total_interest": total_interest,
            "total_balance_plus_interest": total_total_balance,
            "total_paid_amount": total_paid_amount,
            "total_unused_amount": total_unused_amount,
            "net_outstanding": net_outstanding,
            "status_counts": status_counts,
            "invoices_count": len(invoices_data),
            "payments_count": len(payments_data),
            "rows_written": len(rows),
        }

        logger.info(f"✅ Data preparation complete with interest: {len(rows)} rows prepared")
        return rows, summary

    except Exception as e:
        logger.error(f"❌ Error preparing sheet data: {str(e)}")
        return [], {
            "error": str(e),
            "total_balance_due": 0,
            "total_interest": 0,
            "total_balance_plus_interest": 0,
            "total_paid_amount": 0,
            "total_unused_amount": 0,
            "net_outstanding": 0,
            "status_counts": {},
        }

# -----------------------------
# Write to Google Sheets
# -----------------------------
def write_to_google_sheets(client, rows):
    """Write data to Google Sheets using batch updates. Creates a new worksheet for each statement."""
    try:
        spreadsheet = client.open_by_key(SPREADSHEET_ID)

        # Worksheet name with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        worksheet_name = f"Statement_{timestamp}"
        logger.info(f"📝 Creating new worksheet: {worksheet_name}")

        # Create worksheet with default columns increased to DEFAULT_SHEET_COLS
        worksheet = spreadsheet.add_worksheet(
            title=worksheet_name,
            rows=max(len(rows) + 20, 100),
            cols=DEFAULT_SHEET_COLS
        )

        # Batch write in chunks for performance
        if rows:
            batch_size = 100
            total_batches = (len(rows) + batch_size - 1) // batch_size
            for i in range(0, len(rows), batch_size):
                batch = rows[i:i + batch_size]
                start_row = i + 1
                batch_num = (i // batch_size) + 1
                logger.info(f"📤 Writing batch {batch_num}/{total_batches} (rows {start_row}-{start_row + len(batch) - 1})")
                worksheet.update(f"A{start_row}", batch)
            logger.info(f"✅ Successfully wrote {len(rows)} rows to Google Sheets")

        return {
            "worksheet_name": worksheet_name,
            "worksheet_id": worksheet.id,
            "spreadsheet_url": f"https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/edit#gid={worksheet.id}"
        }

    except gspread.SpreadsheetNotFound:
        logger.error(f"❌ Spreadsheet not found: {SPREADSHEET_ID}")
        raise HTTPException(status_code=404, detail="Spreadsheet not found or not accessible")
    except gspread.APIError as e:
        logger.error(f"❌ Google Sheets API error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Google Sheets API error: {str(e)}")
    except Exception as e:
        logger.error(f"❌ Error writing to Google Sheets: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error writing to sheets: {str(e)}")

# -----------------------------
# API Endpoints
# -----------------------------
@app.get("/")
async def root():
    """API root endpoint"""
    return {
        "message": "Statement of Accounts API",
        "version": "1.0.0",
        "status": "running",
        "endpoints": {
            "POST /write_statement/": "Create new statement in Google Sheets",
            "POST /append_to_statement/": "Append data to existing statement",
            "GET /get_statement/": "Retrieve statement data",
            "GET /health": "Health check",
            "GET /check_credentials": "Check Google Sheets connection"
        }
    }


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "service": "Statement of Accounts API"
    }


@app.get("/check_credentials")
async def check_credentials():
    """Check if Google Sheets credentials are properly configured (and spreadsheet accessible)."""
    logger.info("🔍 Checking Google Sheets credentials...")
    client = get_google_sheets_client()
    if client is None:
        return {
            "status": "error",
            "message": "Google Sheets credentials not found or invalid",
            "instructions": [
                "1. Ensure 'cred.json' is in your project directory",
                "2. Verify the file contains valid service account credentials",
                "3. Share the Google Sheet with the service account email"
            ],
            "spreadsheet_id": SPREADSHEET_ID
        }

    try:
        spreadsheet = client.open_by_key(SPREADSHEET_ID)

        service_account_email = "Unknown"
        if os.path.exists(CREDENTIALS_FILE):
            with open(CREDENTIALS_FILE, 'r') as f:
                cred_data = json.load(f)
                service_account_email = cred_data.get('client_email', 'Unknown')

        return {
            "status": "success",
            "message": "✅ Credentials are valid and spreadsheet is accessible",
            "spreadsheet_title": spreadsheet.title,
            "spreadsheet_id": SPREADSHEET_ID,
            "spreadsheet_url": f"https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/edit",
            "service_account": service_account_email,
            "worksheet_count": len(spreadsheet.worksheets())
        }
    except gspread.SpreadsheetNotFound:
        return {
            "status": "error",
            "message": "Credentials are valid but spreadsheet not found or not shared",
            "instructions": [
                f"1. Open the spreadsheet: https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/edit",
                "2. Click 'Share'",
                "3. Add the service account email from your cred.json file",
                "4. Give it 'Editor' permissions",
                "5. Click 'Send'"
            ]
        }
    except Exception as e:
        return {
            "status": "error",
            "message": f"Error accessing spreadsheet: {str(e)}",
            "spreadsheet_id": SPREADSHEET_ID
        }


@app.post("/write_statement/")
async def create_statement(data: StatementData):
    """Create a new Statement of Accounts in Google Sheets."""
    try:
        logger.info("=" * 60)
        logger.info("📥 NEW STATEMENT REQUEST RECEIVED")
        logger.info(f"📄 Invoices: {len(data.invoices)}")
        logger.info(f"💳 Payments: {len(data.payments)}")
        logger.info("=" * 60)

        # Prepare the data (uses env MONTHLY_INTEREST_RATE by default)
        rows, summary = prepare_sheet_data(data.invoices, data.payments)

        if "error" in summary:
            logger.warning(f"⚠️ Data preparation warning: {summary.get('error')}")

        client = get_google_sheets_client()
        if client is None:
            # Simulated mode: return preview and summary without writing to Google Sheets
            logger.warning("⚠️ Running in SIMULATED MODE - No Google credentials")
            return {
                "status": "simulated_success",
                "message": "Statement created successfully (SIMULATED - No Google credentials)",
                "instructions": "To write to actual Google Sheets, configure your cred.json file",
                "spreadsheet_id": SPREADSHEET_ID,
                "worksheet_name": f"Statement_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                "summary": summary,
                "preview_rows": rows[:10] if rows else [],
                "spreadsheet_url": f"https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/edit",
                "rows_written": len(rows),
            }

        # Write to Google Sheets
        logger.info("📤 Writing to Google Sheets...")
        sheet_result = write_to_google_sheets(client, rows)

        logger.info("✅ STATEMENT CREATED SUCCESSFULLY!")
        logger.info(f"📊 Worksheet: {sheet_result['worksheet_name']}")
        logger.info(f"🔗 URL: {sheet_result['spreadsheet_url']}")

        return {
            "status": "success",
            "message": "Statement created successfully in Google Sheets",
            "spreadsheet_id": SPREADSHEET_ID,
            "worksheet_name": sheet_result['worksheet_name'],
            "summary": summary,
            "spreadsheet_url": sheet_result['spreadsheet_url'],
            "rows_written": len(rows),
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Unexpected error creating statement: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/append_to_statement/")
async def append_to_statement(data: StatementData):
    """Append data to existing statement (demo implementation).
    Current implementation prepares rows and returns the summary.
    To fully implement: accept worksheet_name and append there.
    """
    try:
        logger.info("📥 Append request received")
        rows, summary = prepare_sheet_data(data.invoices, data.payments)

        return {
            "status": "success",
            "message": "Data prepared for appending (feature not fully implemented)",
            "appended_invoices": len(data.invoices),
            "appended_payments": len(data.payments),
            "summary": summary,
            "note": "To implement: specify worksheet name to append to"
        }
    except Exception as e:
        logger.error(f"❌ Error in append operation: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/get_statement/")
async def get_statement(worksheet_name: Optional[str] = None):
    """Retrieve statement data from Google Sheets. If worksheet_name is omitted, returns available sheets."""
    try:
        client = get_google_sheets_client()
        if client is None:
            return {
                "status": "error",
                "message": "Google Sheets credentials not configured",
                "data": None
            }

        spreadsheet = client.open_by_key(SPREADSHEET_ID)
        worksheets = spreadsheet.worksheets()
        worksheet_list = [ws.title for ws in worksheets]

        if worksheet_name:
            try:
                worksheet = spreadsheet.worksheet(worksheet_name)
                data = worksheet.get_all_values()
                return {
                    "status": "success",
                    "worksheet": worksheet_name,
                    "rows": len(data),
                    "data": data[:50],
                    "message": "Data retrieved successfully"
                }
            except gspread.WorksheetNotFound:
                return {
                    "status": "error",
                    "message": f"Worksheet '{worksheet_name}' not found",
                    "available_worksheets": worksheet_list
                }
        else:
            return {
                "status": "success",
                "message": "Available worksheets",
                "worksheets": worksheet_list,
                "count": len(worksheet_list),
                "instruction": "Add ?worksheet_name=<name> to get specific worksheet data"
            }

    except Exception as e:
        logger.error(f"❌ Error retrieving statement: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# -----------------------------
# Run the application
# -----------------------------
if __name__ == "__main__":
    import uvicorn

    logger.info("🚀 Starting Statement of Accounts API...")
    logger.info(f"📊 Target Spreadsheet ID: {SPREADSHEET_ID}")
    logger.info(f"📄 Looking for credentials file: {CREDENTIALS_FILE}")
    logger.info(f"🔢 Default monthly interest rate: {MONTHLY_INTEREST_RATE}% per month")
    logger.info(f"🔢 Default sheet columns: {DEFAULT_SHEET_COLS}")

    client = get_google_sheets_client()
    if client:
        logger.info("✅ Google Sheets connection established!")
    else:
        logger.warning("⚠️ Running without Google Sheets connection (simulated mode)")

    uvicorn.run(
        app,
        host="127.0.0.1",
        port=PORT,
        log_level="info"
    )
