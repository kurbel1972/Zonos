# Zonos
Create Files to Zonos 

## Description
This project processes postal tracking CSV files, enriches the data with SQL Server information, and generates formatted output files. At the end of each file processing, it sends an email notification with the generated files.

## Project Structure
- `main.py` - Application entry point
- `processor.py` - Main file processing logic
- `db.py` - SQL Server connection and queries
- `email_sender.py` - Email sending functionality

## Configuration

### Environment Variables

Create a `.env` file in the project root with the following variables:

```env
# Directories
INPUT_DIR=C:/path/to/input
OUTPUT_DIR=C:/path/to/output
PROCESSED_DIR=C:/path/to/processed
LOG_DIR=C:/path/to/output/logs

# SQL Server
SQL_SERVER=your_server
SQL_DATABASE=your_database

# Email Configuration
SMTP_SERVER=smtp.example.com
SMTP_PORT=587
SMTP_USER=your_email@example.com
SMTP_PASSWORD=your_email_password
EMAIL_FROM=your_email@example.com
EMAIL_TO=recipient1@example.com,recipient2@example.com,recipient3@example.com
```

### Installation

```bash
pip install python-dotenv pyodbc
```

## Usage

```bash
python main.py
```

## Features

1. **File Processing**: Reads CSV files from input folder
2. **Data Enrichment**: Queries SQL Server for additional data
3. **Output Generation**: Creates formatted file with consolidated data
4. **Error Logging**: Records rows that could not be processed
5. **Email Notification**: Sends email with generated files and processing report for each input file

## Output Files

- **PT_POST_POSTAL_DATA_[timestamp].csv**: Main file with processed data
- **SKIPPED_ROWS_[timestamp].csv**: Log of rows that were not processed
