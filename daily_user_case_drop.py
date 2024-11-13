import os
import pandas as pd
import requests
import pytz,traceback,sys
import time
import logging
import datetime as dt
from database_ops import get_data_cc, get_yaake_data
from urllib.parse import urlencode
from datetime import datetime, timedelta
# from slack_notification import slack_notification
from python_http_client.exceptions import HTTPError, BadRequestsError
from sqlalchemy import create_engine, text
import smtplib
from urllib import parse
import requests

# from prefect import task, Flow
from dotenv import load_dotenv

env_path = '.env'
load_dotenv(dotenv_path=env_path)

# def send_whatsapp_message(phone_number, client_name):
#     url = "https://api.gupshup.io/wa/api/v1/template/msg"
#     headers = {
#         "Content-Type": 'application/x-www-form-urlencoded',
#         "apikey": "YOUR_GUPSHUP_API_KEY"
#     }
#     payload = {
#         "channel": "whatsapp",
#         "source": '918951359309',  
#         "destination": phone_number,
#         "src.name": 'tHmYIgOKgztIGYG000ZhFHQp',  # Name configured in Gupshup
#         'template': {"id":"35238da8-424a-4b8a-8f61-66215f819bde","params":[]}
#     }
    
#     response = requests.post(url, headers=headers, data=payload)
#     print(response )
#     return response.status_code, response.json()    

# api_key = 'bhn6nqnfpieunjlerhevpfmktlb5gvb2'


def DAU_no_case(client_details):
    # Ensure 'case_receiving_date' is in datetime format
    client_details['case_receiving_date'] = pd.to_datetime(client_details['case_receiving_date'])
    print(client_details)

    # Define T-Period: T = today - 1 day (yesterday), T30 = T - 29 days (30-day window)
    today = datetime.now().date()
    T = datetime.combine(today - timedelta(days=1), datetime.min.time())
    T30 = datetime.combine(T - timedelta(days=29), datetime.min.time())

    print(f"T (D-1): {T}, T30: {T30}")

    # Filter data for the T-Period (last 30 days from T30 to T)
    t_period_data = client_details[client_details['case_receiving_date'].between(T30, T)]
    print("Filtered T-Period Data:")
    print(t_period_data)

    # Get unique list of clients who sent cases in the T-Period
    clients_in_t_period = t_period_data['client_fk'].unique()

    # Filter data for today
    today_data = client_details[client_details['case_receiving_date'] == pd.to_datetime(today)]

    # Get unique list of clients who sent cases today
    clients_today = today_data['client_fk'].unique()

    # Identify clients who sent cases in the last 30 days but not today
    clients_not_today = set(clients_in_t_period) - set(clients_today)

    # Get details of clients who sent cases in the T-Period but not today
    clients_not_today_data = t_period_data[t_period_data['client_fk'].isin(clients_not_today)]
    filtered_clients_not_today_data = clients_not_today_data[['client_fk', 'client_name']].drop_duplicates()
    print("Clients who sent cases in the last 30 days but didn't send any cases today:")
    print(filtered_clients_not_today_data )
    filtered_clients_not_today_data .to_csv("1clients_not_today_data.csv")
    
    # Now, merge filtered_clients_not_today_data with contact_details on client_fk
    merged_df = filtered_clients_not_today_data.merge(contact_details, on='client_fk', how='left')
    # Select only the specified columns
    merged_df = merged_df[['client_fk', 'client_name_x', 'contact_fk', 'unique_id', 'phone_number']]
    merged_df.rename(columns={'client_name_x': 'Client_Name'}, inplace= True)

    
    print("Merged DataFrame after combining with contact details:")
    merged_df.to_csv("DAU_contact.csv")
    
    # Clean phone numbers to ensure they are in the correct format
    def clean_phone_number(phone_number):
        if pd.isna(phone_number):
            return phone_number
        # Remove '+91-' or '91-' and ensure it's a 10 digit number
        phone_number = str(phone_number).replace('+91-', '').replace('91-', '')
        # Ensure the phone number has the correct 10-digit length and add '91' prefix if needed
        if len(phone_number) == 10 and phone_number.isdigit():
            return '91' + phone_number
        return phone_number  # Return the original if it's not a valid 10-digit number

    # Apply the clean_phone_number function to the 'phone_number' column
    merged_df['phone_number'] = merged_df['phone_number'].apply(clean_phone_number)
    # Drop rows where the phone number is null or None
    merged_df = merged_df.dropna(subset=['phone_number'])

    # Print the merged DataFrame after cleaning phone numbers
    print("Merged DataFrame after cleaning phone numbers:")
    print(merged_df)

    # Save the cleaned data to a CSV file
    merged_df.to_csv("DAU_contact_cleaned.csv", index=False)

    return merged_df
    
    # # Sending WhatsApp messages
    # for _, row in merged_df.iterrows():
    #     phone_number = row['phone_number']
    #     client_name = row['Client_Name']
    #     status_code, response = send_whatsapp_message(phone_number, client_name)
    #     print(f"Message sent to {client_name} (Phone: {phone_number}), Status Code: {status_code}")
    #     print("Response:", response)
    
client_details = get_data_cc('sql/clients_details.sql')
contact_details = get_data_cc('sql/contact_details.sql')
# Call the DAU_no_case function and capture its returned DataFrame
DAU_no_case_df = DAU_no_case(client_details)

# def daily_user_volume(client_details):
#     # Ensure 'case_receiving_date' is in datetime format
#     client_details['case_receiving_date'] = pd.to_datetime(client_details['case_receiving_date'])
#     print(client_details)
    
#     # Define T-Period: T = today - 3 days, T30 = T - 29 days (30-day window)
#     today = datetime.now().date()

#     print("Today: ", today)
#     # Convert T and T30 to datetime objects (set time to midnight)
#     T = datetime.combine(today - timedelta(days=1), datetime.min.time())
#     T30 = datetime.combine(T - timedelta(days=29), datetime.min.time())
    

#     print(f"T (D-1): {T}, T30: {T30}")
#     # Filter data for the T-Period (last 30 days from T30 to T)
#     t_period_data = client_details[client_details['case_receiving_date'].between(T30, T)]
#     print("Filtered T-Period Data:")
#     print(t_period_data)
    
#     # Filter data for the T-Period (last 30 days from T30 to T)
#     t_period_data = client_details[client_details['case_receiving_date'].between(T30, T)]
#     print("Filtered T-Period Data:")
#     print(t_period_data)

#     # Identify clients who have sent cases today
#     clients_sent_today = t_period_data[t_period_data['case_receiving_date'] == today]['client_fk'].unique()

#     # Get clients who didn't send cases today by excluding those who sent cases today
#     clients_not_sent_today = t_period_data[~t_period_data['client_fk'].isin(clients_sent_today)][['client_fk', 'client_name']].drop_duplicates()

#     print("Clients who didn't send cases today:")
#     print(clients_not_sent_today)

#     # Save to CSV if needed
#     clients_not_sent_today.to_csv("clients_not_sent_today.csv", index=False)
    
    
    
#     # # Get clients who didn't send cases today
#     # clients_not_sent_today = t_period_data[t_period_data['case_receiving_date'].isin[[today]]]
#     # clients_not_sent_today = clients_not_sent_today[['client_fk', 'client_name']].drop_duplicates()


#     # print("Clients who didn't send cases today:")
#     # print(clients_not_sent_today)
#     # # clients_not_sent_today.to_csv("clients_not_sent_today.csv")
    
    
#     # Group by client and calculate active days (A) and average case load (B)
#     client_activity = t_period_data.groupby('client_fk').agg(
#         active_days=('case_receiving_date', 'nunique'),  # Number of active days
#         avg_case_load=('total_case', 'mean')  # Average case load
#     ).reset_index()
    
#     print("Client Activity (Active Days and Avg Case Load):")
#     print(client_activity)
#     # Merge client_activity with client_details to include 'client_name'
#     client_activity = pd.merge(client_activity, client_details[['client_fk', 'client_name']], on='client_fk', how='left')
#     client_activity.to_csv("daily_client_activity.csv")
    
    
#     # not_sent_today= pd.merge(clients_not_sent_today, client_activity[['client_fk','active_days', 'avg_case_load']], on='client_fk', how='left')
#     # # Remove duplicate client_fk rows
#     # not_sent_today = not_sent_today.drop_duplicates(subset=['client_fk'])
#     # not_sent_today.to_csv("clients_not_sent_today.csv")
    
#     # Filter clients with average case load > 5
#     client_activity = client_activity[client_activity['avg_case_load'] > 2]
#     print(client_activity)
    

#     # Filter the client_details DataFrame for recent cases (D-1 to D-5)
#     recent_cases = client_details[client_details['case_receiving_date'] == today]
#     print("Recent Cases:")
#     print(recent_cases)
    
#     # Identify clients who have sent cases today (i.e., those who have 'client_fk' in recent_cases)
#     clients_sent_today = recent_cases['client_fk'].unique()
    
#     # Filter client_activity to find clients who didn't send cases today
#     clients_no_cases_today = client_activity[~client_activity['client_fk'].isin(clients_sent_today)]
    
#     clients_no_cases_today = clients_no_cases_today.drop_duplicates(subset=['client_fk'])
#     print("Clients Who Didn't Send Any Cases Today:")
#     print(clients_no_cases_today[['client_fk', 'client_name', 'active_days', 'avg_case_load']])
#     clients_no_cases_today.to_csv("DAU_zero_cases_today.csv")
    
    
#     # Categorize clients
#     cat1_clients = client_activity[(client_activity['active_days'] == 30)]
#     cat1_recent = recent_cases[recent_cases['case_receiving_date'] == today]
    
#     cat1_less_cases = cat1_clients[~cat1_clients['client_fk'].isin(cat1_recent['client_fk'])]
#     # Remove duplicate clients (based on 'client_fk' column, or use others if necessary)
#     cat1_less_cases = cat1_less_cases.drop_duplicates(subset=['client_fk'])
    
    
#     print("Clients (No Recent Cases):")
#     print(cat1_less_cases[['client_fk', 'client_name', 'active_days', 'avg_case_load']])
#     cat1_less_cases.to_csv("DAU_no_cases.csv")
    
#     # New: Filter clients in cat1_less_cases who have less than 50% of their average case load
#     cat1_less_cases_low_activity = cat1_less_cases[cat1_less_cases['avg_case_load'] * 0.5 > cat1_less_cases['avg_case_load']]
    
#     print("Clients with Less Than 50% of Their Average Case Load:")
#     print(cat1_less_cases_low_activity[['client_fk', 'client_name', 'active_days', 'avg_case_load']])
    
#     # Optionally, save this list to a CSV
#     cat1_less_cases_low_activity.to_csv("DAU_low_activity_cases.csv")

#     # return cat1_no_cases[['client_fk', 'active_days', 'avg_case_load', 'client_active_type', 'churn_category']]
    
# client_details = get_data_cc('sql/clients_details.sql')
# daily_user_volume(client_details)
