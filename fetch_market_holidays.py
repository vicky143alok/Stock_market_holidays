import pandas as pd
from exchange_calendars import get_calendar
import requests
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

def fetch_stock_holidays_and_notify():
    # Get the NYSE calendar
    nyse = get_calendar("XNYS")
    start_date = "2024-01-01"
    end_date = "2024-12-31"
    
    valid_days = nyse.sessions_in_range(start_date, end_date)
    all_days = pd.date_range(start=start_date, end=end_date, freq="D")
    
    # Calculate holidays
    holidays = set(all_days) - set(valid_days)
    holidays = sorted(list(holidays))
    
    if holidays:
        # Format holidays for display
        holiday_list = "\n".join([holiday.strftime("%Y-%m-%d") for holiday in holidays])
        
        # Send notification to Microsoft Teams (simplified payload)
        webhook_url = "https://webhookbot.c-toss.com/api/bot/webhooks/7c701236-949d-4c2c-a8f7-fcf9c2a0e76f"  # Replace with your webhook URL
        payload = {
            "text": f"Hello Team,\n\nThe following dates are stock market holidays in 2024:\n\n{holiday_list}\n\nPlease plan accordingly."
        }
        headers = {"Content-Type": "application/json"}
        
        # Send the request
        response = requests.post(webhook_url, json=payload, headers=headers)
        
        if response.status_code == 200:
            print("Notification sent successfully to Microsoft Teams.")
        else:
            print(f"Failed to send notification. Status code: {response.status_code}")

default_args = {
    'owner': 'airflow',
    'retries': 1,
}

# Define the Airflow DAG
with DAG(
    dag_id='stock_market_holidays_notification',
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2023, 11, 27),
    catchup=False,
) as dag:
    notify_task = PythonOperator(
        task_id='fetch_holidays_and_notify',
        python_callable=fetch_stock_holidays_and_notify
    )
