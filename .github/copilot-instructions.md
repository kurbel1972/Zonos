# Zonos - Copilot Instructions

## Project Overview
Zonos is a postal data processing pipeline that enriches CSV files with SQL Server data and sends email notifications. It follows a simple batch processing pattern: read CSV → enrich with SQL → output formatted CSV → email results.

## Architecture & Data Flow

**Pipeline**: `main.py` → `processor.py` → `db.py` + `email_sender.py`

1. **processor.py**: Orchestrates the entire workflow - reads CSV files from `INPUT_DIR`, enriches each tracking number with SQL data, generates output files with timestamp-based names, and sends email notifications
2. **db.py**: Two-stage SQL query pattern - first SELECT extracts metadata (UAT, mail class/subclass), then calls stored procedure `CTT_SP_REL_CONTROLO_TRANSPORTE_INT` with those parameters
3. **email_sender.py**: Reusable email utilities that attach both output and log files to notification emails

## Critical Patterns

### SQL Data Retrieval Pattern
The `get_sql_data()` function uses a **multi-resultset pattern**:
- First query returns metadata variables (`@uat_number`, `@MAIL_CLASS_NM`, etc.)
- `fetch_last_resultset_row()` navigates to the final SELECT with variables
- Those variables feed into the stored procedure call
- Result parsing uses fallback logic: tries columns 26→23→20→18 for flight data

### CSV Column Name Flexibility
Input CSV handling supports multiple column name variations:
- `"CTT Account"` OR `"Shipper Account ID"` for account field
- `"Item Content Country of Origin Code"` with extra spacing handled
- Always clean country codes: `replace(" ", "")`

### File Naming Convention
- Output: `YYYYMMDDHHMMSS_PT_POST_POSTAL_DATA.csv`
- Logs: `YYYYMMDDHHMMSS_SKIPPED_ROWS.csv`
- Use `datetime.now().strftime("%Y%m%d%H%M%S")` for consistency

### Date Parsing Flexibility
Parse `arrival_date` with dual format support:
```python
try:
    arrival_date = datetime.strptime(arrival_date_str, "%Y-%m-%d %H:%M")
except ValueError:
    arrival_date = datetime.strptime(arrival_date_str, "%Y-%m-%d")
```

## Environment Configuration

`.env` file drives all configuration - no hardcoded paths:
- **Directories**: `INPUT_DIR`, `OUTPUT_DIR`, `PROCESSED_DIR`, `LOG_DIR`
- **SQL Server**: `SQL_SERVER`, `SQL_DATABASE` (uses Windows Authentication/Trusted_Connection)
- **Email**: `SMTP_SERVER`, `SMTP_PORT`, `SMTP_USER`, `SMTP_PASSWORD`, `EMAIL_FROM`, `EMAIL_TO` (comma-separated)

## Output File Mapping
Final CSV columns are **strictly ordered** and use specific string operations:
- `Carrier Code`: First 2 chars of `carrier`
- `Flight/Trip Number`: Chars 3+ of `flight` (skip first 2)
- `Arrival Port Code`: Chars 3-5 of `arrival_port` (positions 2-4 in zero-indexed)

## Dependencies
- `pyodbc` with `DRIVER={SQL Server}` - requires SQL Server ODBC driver installed
- `python-dotenv` for environment variables
- Standard library: `csv`, `smtplib`, `email.mime.*`

## Development Workflow
```bash
# Setup
pip install python-dotenv pyodbc

# Run
python main.py
```

No tests currently exist. SQL Server connection uses Trusted_Connection (Windows Authentication), so running on Windows with domain access is expected.
