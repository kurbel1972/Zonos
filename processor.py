import os
import csv
import re
from datetime import datetime
from db import get_sql_data
from email_sender import send_processing_notification


def _normalize_header(header):
    cleaned = (header or "").replace("\ufeff", "")
    return re.sub(r"[^a-z0-9]", "", cleaned.lower())


def _get_row_value(row, *possible_keys, default=""):
    normalized_row = {
        _normalize_header(key): value
        for key, value in row.items()
        if key is not None
    }

    for key in possible_keys:
        normalized_key = _normalize_header(key)
        if normalized_key in normalized_row:
            return normalized_row[normalized_key], True

    return default, False


def process_files():
    input_dir = os.getenv("INPUT_DIR")
    output_dir = os.getenv("OUTPUT_DIR")
    processed_dir = os.getenv("PROCESSED_DIR")
    log_dir = os.getenv("LOG_DIR", os.path.join(output_dir, "logs"))

    # Ensure the processed and log directories exist
    if not os.path.exists(processed_dir):
        os.makedirs(processed_dir)
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # List all files in the input directory
    p_value = 0
    
    for filename in os.listdir(input_dir):
        input_file = os.path.join(input_dir, filename)
        if not os.path.isfile(input_file):
            continue

        # Rename file with timestamp to avoid conflicts when moving to processed directory
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        name_without_ext, ext = os.path.splitext(filename)
        new_filename = f"{name_without_ext}_{timestamp}{ext}"
        new_input_file = os.path.join(input_dir, new_filename)
        os.rename(input_file, new_input_file)
        
        # Update variables to use the renamed file
        input_file = new_input_file
        filename = new_filename

        print(f"Processing: {input_file}")

        # Read the input CSV file
        with open(input_file, mode="r", encoding="utf-8") as f:
            reader = csv.DictReader(f, delimiter=",")
            rows = list(reader)

        final_results = []
        skipped_rows = []
        output_file_path = None
        log_file_path = None

        for row in rows:

            p_value += 1

            tracking_number, has_tracking_number = _get_row_value(row, "Tracking Number")
            if not has_tracking_number:
                print(f"Warning: 'Tracking Number' column not found in input file. Row: {row}")
                skipped_rows.append(row)
                continue

            print(f"{p_value} --> Processing tracking number: {tracking_number}")

            country_of_origin, has_country_of_origin = _get_row_value(
                row,
                "Item Content Country of Origin Code"
            )
            if not has_country_of_origin:
                print(f"Warning: 'Item Content  Country  of  Origin Code' column not found in input file. Row: {row}")
                country_of_origin = ""

            # Fetch additional data from SQL Server
            sql_data = get_sql_data(tracking_number)
            if not sql_data:
                print(f"No SQL data for tracking {tracking_number}")
                skipped_rows.append(row)
                continue

            # Create final row according to template
            # Parse arrival_date, supporting both date and datetime
            arrival_date_str = sql_data["arrival_date"]
            try:
                arrival_date = datetime.strptime(arrival_date_str, "%Y-%m-%d %H:%M")
            except ValueError:
                arrival_date = datetime.strptime(arrival_date_str, "%Y-%m-%d")

            cleaned_country_of_origin = country_of_origin.replace(" ", "") if country_of_origin else ""
            
            # Try both possible column names for account ID
            shipper_account, _ = _get_row_value(row, "CTT Account", "Shipper Account ID")
            nature_of_transaction, _ = _get_row_value(row, "Nature of Transaction")
            declared_value, _ = _get_row_value(row, "Item Content Declared Value")
            currency_code, _ = _get_row_value(row, "Item Content Currency Code")
            
            final_row = {
                "Carrier Code": sql_data["carrier"][:2] if sql_data["carrier"] else "",
                "Flight/ Trip Number": sql_data["flight"][2:] if sql_data["flight"] and len(sql_data["flight"])>=3 else "",
                "Tracking Number": tracking_number,
                "Nature of Transaction": nature_of_transaction,
                "Arrival Port Code": sql_data["arrival_port"][2:5] if sql_data["arrival_port"] and len(sql_data["arrival_port"])>=5 else "",
                "Arrival Date": arrival_date.strftime("%Y-%m-%d"),
                "Declared Value": declared_value,
                "Currency Code": currency_code,
                "Country of Origin": cleaned_country_of_origin,
                "Shipper Account ID": shipper_account
            }

            final_results.append(final_row)

        if final_results:
            # Create output file name with full timestamp
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            output_name = f"PT_POST_POSTAL_DATA_{timestamp}.csv".upper()
            output_path = os.path.join(output_dir, output_name)

            # Write final CSV
            with open(output_path, mode="w", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=final_results[0].keys(), delimiter=",")
                writer.writeheader()
                writer.writerows(final_results)

            print(f"File generated: {output_path}")
            output_file_path = output_path

        # Write log file for skipped rows
        if skipped_rows:
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            log_name = f"SKIPPED_ROWS_{timestamp}.csv"
            log_path = os.path.join(log_dir, log_name)

            # Filter out None keys from fieldnames (can happen with empty CSV headers or trailing commas)
            fieldnames = [key for key in rows[0].keys() if key is not None]
            
            # Clean skipped rows by removing None keys
            cleaned_skipped_rows = [{k: v for k, v in row.items() if k is not None} for row in skipped_rows]

            with open(log_path, mode="w", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=",")
                writer.writeheader()
                writer.writerows(cleaned_skipped_rows)

            print(f"Log file generated: {log_path} ({len(skipped_rows)} skipped rows)")
            log_file_path = log_path

        # Move processed file to processed directory
        destination = os.path.join(processed_dir, filename)
        os.rename(input_file, destination)
        print(f"File moved to: {destination}")
        
        # Send email notification for this file
        if output_file_path or log_file_path:
            print(f"\nSending email notification for {filename}...")
            
            email_sent = send_processing_notification(
                input_filename=filename,
                output_file=output_file_path,
                log_file=log_file_path,
                total_processed=len(final_results),
                total_skipped=len(skipped_rows)
            )
            
            if email_sent:
                print(f"Email notification sent successfully for {filename}")
            else:
                print(f"Failed to send email notification for {filename}")
