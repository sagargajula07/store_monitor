from flask import Flask, request, jsonify, send_file
import os
import sys
import pandas as pd
from datetime import datetime, timedelta
import uuid


app = Flask(__name__)

# Load data from CSV files
df_status = pd.read_csv("status.csv")
df_business_hours = pd.read_csv("business_hours.csv")
df_timezone = pd.read_csv("timezone.csv")

# Merge data frames to create a comprehensive dataset
df_combined = pd.merge(df_status, df_business_hours, on="store_id", how="left")
df_combined = pd.merge(df_combined, df_timezone, on="store_id", how="left")

# Set the current timestamp to be the max timestamp among all observations
current_timestamp = df_status["timestamp_utc"].max()

def calculate_uptime_downtime(store_data):
    # Sort data by timestamp
    store_data = store_data.sort_values(by="timestamp_utc")

    # Initialize variables
    uptime_last_hour = downtime_last_hour = timedelta(0)
    uptime_last_day = downtime_last_day = timedelta(0)
    uptime_last_week = downtime_last_week = timedelta(0)

    # Iterate through observations
    for i in range(len(store_data) - 1):
        start_time = store_data["timestamp_utc"].iloc[i]
        end_time = store_data["timestamp_utc"].iloc[i + 1]
        status = store_data["status"].iloc[i]

        # Calculate time interval and adjust for time zone
        time_interval = end_time - start_time
        time_interval = time_interval.total_seconds() / 3600  # Convert to hours
        timezone_offset = store_data["timezone_str"].iloc[i]

        # Adjust time interval for time zone
        time_interval -= timezone_offset

        # Check if the status is active during business hours
        if status == "active" and store_data["start_time_local"].iloc[i] <= start_time.time() <= store_data["end_time_local"].iloc[i]:
            uptime_last_hour += timedelta(hours=time_interval)
            uptime_last_day += timedelta(hours=time_interval)
            uptime_last_week += timedelta(hours=time_interval)
        elif status == "inactive" and store_data["start_time_local"].iloc[i] <= start_time.time() <= store_data["end_time_local"].iloc[i]:
            downtime_last_hour += timedelta(hours=time_interval)
            downtime_last_day += timedelta(hours=time_interval)
            downtime_last_week += timedelta(hours=time_interval)

    return {
        "uptime_last_hour": uptime_last_hour.total_seconds() / 60,  # Convert to minutes
        "uptime_last_day": uptime_last_day.total_seconds() / 3600,  # Convert to hours
        "uptime_last_week": uptime_last_week.total_seconds() / 3600,  # Convert to hours
        "downtime_last_hour": downtime_last_hour.total_seconds() / 60,  # Convert to minutes
        "downtime_last_day": downtime_last_day.total_seconds() / 3600,  # Convert to hours
        "downtime_last_week": downtime_last_week.total_seconds() / 3600  # Convert to hours
    }

@app.route('/trigger_report', methods=['POST'])
def trigger_report():
    # Implement logic to trigger report generation
    report_id = str(uuid.uuid4())

    # Assuming you have a database to store the report generation status
    # Save the report_id and set the status as "Running"
    # ...

    # Run report generation in the background (asynchronously)
    # For simplicity, let's assume the report is generated immediately
    generate_report(report_id)

    return jsonify({"report_id": report_id})

def generate_report(report_id):
    # Implement logic to generate the report
    # Fetch data for the report from the database
    # ...

    # Use the calculate_uptime_downtime function to calculate metrics
    report_data = calculate_uptime_downtime(df_combined)

    # Create a DataFrame for the report
    df_report = pd.DataFrame([report_data])

    # Save the report to a CSV file
    report_filename = f"report_{report_id}.csv"
    df_report.to_csv(report_filename, index=False)

    # Assuming you have a database to update the report status
    # Update the status of the report to "Complete" and save the file path
    # ...

    return report_filename

@app.route('/get_report', methods=['GET'])
def get_report():
    # Get report_id from the request
    report_id = request.args.get('report_id')

if __name__ == '__main__':
    app.run(debug=True)
