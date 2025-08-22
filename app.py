from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
import logging
import os
import json

PORT = int(os.environ.get("PORT", 8000))

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
    Date_Formatted: Optional[str] = None
    Reference_Number: Optional[str] = None
    Total_Formatted: Optional[str] = None
    Balance_Formatted: Optional[str] = None
    Status: Optional[str] = None
    Age: Optional[int] = None
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
CREDENTIALS_FILE = "cred.json"  # Your credentials file

# -----------------------------
# Google Sheets Client
# -----------------------------
def get_google_sheets_client():
    """Initialize Google Sheets client with service account credentials"""
    try:
        SCOPES = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]

        # List of possible credential file locations
        possible_paths = [
            "cred.json",  # Your specific file
            "./cred.json",
            "../cred.json",
            "service-account-credentials.json",
            "credentials.json",
            "./credentials/cred.json",
        ]

        logger.info("🔍 Searching for Google Sheets credentials...")
        
        # Try each possible path
        for credentials_path in possible_paths:
            if os.path.exists(credentials_path):
                try:
                    logger.info(f"📄 Found credentials file at: {credentials_path}")
                    
                    # Load and validate the credentials
                    with open(credentials_path, 'r') as f:
                        cred_data = json.load(f)
                        
                    # Check if essential fields are present
                    required_fields = ['client_email', 'private_key', 'project_id']
                    missing_fields = [field for field in required_fields if not cred_data.get(field)]
                    
                    if missing_fields:
                        logger.error(f"❌ Credentials file is missing required fields: {missing_fields}")
                        continue
                    
                    # Create credentials
                    creds = Credentials.from_service_account_file(
                        credentials_path, 
                        scopes=SCOPES
                    )
                    
                    # Authorize and return client
                    client = gspread.authorize(creds)
                    logger.info(f"✅ Successfully connected to Google Sheets!")
                    logger.info(f"📧 Service Account: {cred_data.get('client_email')}")
                    return client
                    
                except json.JSONDecodeError:
                    logger.error(f"❌ Invalid JSON in credentials file: {credentials_path}")
                    continue
                except Exception as e:
                    logger.error(f"❌ Error loading credentials from {credentials_path}: {str(e)}")
                    continue

        # No valid credentials found
        logger.error("❌ No valid Google Sheets credentials found!")
        logger.error("📝 Please ensure:")
        logger.error("   1. Your 'cred.json' file is in the project directory")
        logger.error("   2. The file contains valid service account credentials")
        logger.error("   3. The Google Sheet is shared with the service account email")
        return None

    except Exception as e:
        logger.error(f"❌ Unexpected error initializing Google Sheets client: {str(e)}")
        return None

# -----------------------------
# Helper Functions
# -----------------------------
def safe_float_conversion(value, default=0.0):
    """Safely convert a value to float"""
    if value is None:
        return default
    try:
        if isinstance(value, str):
            # Remove currency symbols and formatting
            value = value.replace(',', '').replace('₹', '').replace('$', '').strip()
        return float(value)
    except (ValueError, TypeError):
        logger.warning(f"Could not convert '{value}' to float, using default {default}")
        return default


def safe_int_conversion(value, default=0):
    """Safely convert a value to int"""
    if value is None:
        return default
    try:
        if isinstance(value, str):
            value = value.strip()
        return int(value)
    except (ValueError, TypeError):
        logger.warning(f"Could not convert '{value}' to int, using default {default}")
        return default


def format_currency(amount):
    """Format amount as currency"""
    if amount is None:
        return "₹0.00"
    try:
        amount = safe_float_conversion(amount, 0.0)
        return f"₹{amount:,.2f}"
    except Exception:
        return "₹0.00"


def prepare_sheet_data(invoices_data, payments_data):
    """Prepare data for Google Sheets"""
    
    try:
        # Ensure we have lists
        if not isinstance(invoices_data, list):
            invoices_data = []
        if not isinstance(payments_data, list):
            payments_data = []

        logger.info(f"📊 Processing {len(invoices_data)} invoices and {len(payments_data)} payments")

        # Calculate totals with safe conversions
        total_balance_due = 0.0
        for inv in invoices_data:
            balance = safe_float_conversion(inv.get("Balance Due", 0))
            total_balance_due += balance

        total_paid_amount = 0.0
        for pay in payments_data:
            paid = safe_float_conversion(pay.get("Paid Amount", 0))
            total_paid_amount += paid

        total_unused_amount = 0.0
        for pay in payments_data:
            unused = safe_float_conversion(pay.get("Unused Amount", 0))
            total_unused_amount += unused

        net_outstanding = total_balance_due - (total_paid_amount - total_unused_amount)

        # Count invoice statuses
        status_counts = {}
        for invoice in invoices_data:
            status = str(invoice.get("Status", "Unknown")).strip() or "Unknown"
            status_counts[status] = status_counts.get(status, 0) + 1

        # Build the sheet rows
        rows = []
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Header section
        rows.extend([
            ["STATEMENT OF ACCOUNTS"],
            [f"Generated on: {current_time}"],
            [""],
            ["=" * 80],
            [""],
        ])

        # Invoices section
        rows.extend([
            ["INVOICES SECTION"],
            ["Date", "Reference", "Total", "Balance", "Status", "Age", "Invoice ID", "Balance Due"],
        ])

        for invoice in invoices_data:
            rows.append([
                str(invoice.get("Date Formatted", "")),
                str(invoice.get("Reference Number", "")),
                str(invoice.get("Total Formatted", "")),
                str(invoice.get("Balance Formatted", "")),
                str(invoice.get("Status", "")),
                str(invoice.get("Age", "")),
                str(invoice.get("Invoice ID", "")),
                safe_float_conversion(invoice.get("Balance Due", 0)),
            ])

        rows.extend([[""], ["=" * 80], [""]])

        # Payments section
        rows.extend([
            ["PAYMENTS SECTION"],
            ["Payment ID", "Paid Amount", "Unused Amount"],
        ])

        for payment in payments_data:
            rows.append([
                str(payment.get("Payment ID", "")),
                safe_float_conversion(payment.get("Paid Amount", 0)),
                safe_float_conversion(payment.get("Unused Amount", 0)),
            ])

        rows.extend([[""], ["=" * 80], [""]])

        # Financial Summary
        rows.extend([
            ["FINANCIAL SUMMARY"],
            [""],
            ["Total Balance Due:", format_currency(total_balance_due)],
            ["Total Paid Amount:", format_currency(total_paid_amount)],
            ["Total Unused Amount:", format_currency(total_unused_amount)],
            ["Net Outstanding:", format_currency(net_outstanding)],
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
            "total_paid_amount": total_paid_amount,
            "total_unused_amount": total_unused_amount,
            "net_outstanding": net_outstanding,
            "status_counts": status_counts,
            "invoices_count": len(invoices_data),
            "payments_count": len(payments_data),
            "rows_written": len(rows),
        }

        logger.info(f"✅ Data preparation complete: {len(rows)} rows prepared")
        return rows, summary

    except Exception as e:
        logger.error(f"❌ Error preparing sheet data: {str(e)}")
        # Return minimal valid data on error
        return [], {
            "total_balance_due": 0,
            "total_paid_amount": 0,
            "total_unused_amount": 0,
            "net_outstanding": 0,
            "status_counts": {},
            "invoices_count": 0,
            "payments_count": 0,
            "rows_written": 0,
            "error": str(e)
        }


def write_to_google_sheets(client, rows):
    """Write data to Google Sheets"""
    try:
        # Open the spreadsheet
        spreadsheet = client.open_by_key(SPREADSHEET_ID)
        
        # Create a new worksheet with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        worksheet_name = f"Statement_{timestamp}"
        
        logger.info(f"📝 Creating new worksheet: {worksheet_name}")
        
        # Add new worksheet
        worksheet = spreadsheet.add_worksheet(
            title=worksheet_name,
            rows=max(len(rows) + 20, 100),
            cols=12
        )
        
        # Write data in batches for better performance
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
        logger.error("   Please check the spreadsheet ID and ensure it's shared with the service account")
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
    """Check if Google Sheets credentials are properly configured"""
    logger.info("🔍 Checking Google Sheets credentials...")
    
    client = get_google_sheets_client()
    
    if client is None:
        return {
            "status": "error",
            "message": "Google Sheets credentials not found or invalid",
            "instructions": [
                "1. Ensure 'cred.json' is in your project directory",
                "2. Verify the file contains valid service account credentials",
                "3. Check that all required fields are present and not empty",
                "4. Share your Google Sheet with the service account email"
            ],
            "spreadsheet_id": SPREADSHEET_ID
        }
    
    try:
        # Try to access the spreadsheet
        spreadsheet = client.open_by_key(SPREADSHEET_ID)
        
        # Get service account email from credentials
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
                "2. Click the 'Share' button",
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
    """Create a new Statement of Accounts in Google Sheets"""
    
    try:
        logger.info("=" * 60)
        logger.info("📥 NEW STATEMENT REQUEST RECEIVED")
        logger.info(f"📄 Invoices: {len(data.invoices)}")
        logger.info(f"💳 Payments: {len(data.payments)}")
        logger.info("=" * 60)

        # Prepare the data
        rows, summary = prepare_sheet_data(data.invoices, data.payments)
        
        if "error" in summary:
            logger.warning(f"⚠️ Data preparation warning: {summary.get('error')}")

        # Get Google Sheets client
        client = get_google_sheets_client()
        
        if client is None:
            # Simulated mode when no credentials
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
        raise  # Re-raise HTTP exceptions
    except Exception as e:
        logger.error(f"❌ Unexpected error creating statement: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/append_to_statement/")
async def append_to_statement(data: StatementData):
    """Append data to existing statement (demo implementation)"""
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
    """Retrieve statement data from Google Sheets"""
    try:
        client = get_google_sheets_client()
        
        if client is None:
            return {
                "status": "error",
                "message": "Google Sheets credentials not configured",
                "data": None
            }
        
        spreadsheet = client.open_by_key(SPREADSHEET_ID)
        
        # Get list of all worksheets
        worksheets = spreadsheet.worksheets()
        worksheet_list = [ws.title for ws in worksheets]
        
        if worksheet_name:
            # Get specific worksheet
            try:
                worksheet = spreadsheet.worksheet(worksheet_name)
                data = worksheet.get_all_values()
                return {
                    "status": "success",
                    "worksheet": worksheet_name,
                    "rows": len(data),
                    "data": data[:50],  # Return first 50 rows
                    "message": "Data retrieved successfully"
                }
            except gspread.WorksheetNotFound:
                return {
                    "status": "error",
                    "message": f"Worksheet '{worksheet_name}' not found",
                    "available_worksheets": worksheet_list
                }
        else:
            # Return list of available worksheets
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
    
    # Check credentials on startup
    client = get_google_sheets_client()
    if client:
        logger.info("✅ Google Sheets connection established!")
    else:
        logger.warning("⚠️ Running without Google Sheets connection (simulated mode)")
    
    # Run the server
    uvicorn.run(
        app, 
        host="127.0.0.1", 
        port=8000,
        log_level="info"
    )