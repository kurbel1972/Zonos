import os
import csv
from datetime import datetime
from db import get_sql_data


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

        print(f"Processing: {input_file}")

        # Read the input CSV file
        with open(input_file, mode="r", encoding="utf-8") as f:
            reader = csv.DictReader(f, delimiter=",")
            rows = list(reader)

        final_results = []
        skipped_rows = []

        for row in rows:

            p_value += 1

            print(f"{p_value} --> Processing tracking number: {row['Tracking Number']}")

            tracking_number = row["Tracking Number"]

            country_of_origin = row.get("Item Content Country of Origin Code")
            if country_of_origin is None:
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
            final_row = {
                "Carrier Code": sql_data["carrier"][:2] if sql_data["carrier"] else "",
                "Flight/ Trip Number": sql_data["flight"][2:] if sql_data["flight"] and len(sql_data["flight"])>=3 else "",
                "Tracking Number": tracking_number,
                "Nature of Transaction": row["Nature of Transaction"],
                "Arrival Port Code": sql_data["arrival_port"][2:5] if sql_data["arrival_port"] and len(sql_data["arrival_port"])>=5 else "",
                "Arrival Date": arrival_date.strftime("%d/%m/%Y"),
                "Declared Value": row["Item Content Declared Value"],          
                "Currency Code": row["Item Content Currency Code"],
                "Country of Origin": cleaned_country_of_origin,
                "Shipper Account ID": row["Shipper Account ID"]
            }

            final_results.append(final_row)

        if final_results:
            # Create output file name with full timestamp
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            output_name = f"PT_POST_POSTAL_DATA_{timestamp}.csv".upper()
            output_path = os.path.join(output_dir, output_name)

            # Write final CSV
            with open(output_path, mode="w", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=final_results[0].keys(), delimiter=";")
                writer.writeheader()
                writer.writerows(final_results)

            print(f"File generated: {output_path}")

        # Write log file for skipped rows
        if skipped_rows:
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            log_name = f"SKIPPED_ROWS_{timestamp}.csv"
            log_path = os.path.join(log_dir, log_name)

            with open(log_path, mode="w", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=rows[0].keys(), delimiter=",")
                writer.writeheader()
                writer.writerows(skipped_rows)

            print(f"Log file generated: {log_path} ({len(skipped_rows)} skipped rows)")

        # Move processed file to processed directory
        destination = os.path.join(processed_dir, filename)
        os.rename(input_file, destination)
        print(f"File moved to: {destination}")
