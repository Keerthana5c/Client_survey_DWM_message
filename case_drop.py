import os
import pandas as pd
import pytz,traceback,sys
import time
import logging
import datetime as dt
from database_ops import get_data_cc
from datetime import datetime, timedelta
# from slack_notification import slack_notification

# from prefect import task, Flow
from dotenv import load_dotenv

env_path = '.env'
load_dotenv(dotenv_path=env_path)

client_details = get_data_cc('sql/case_detail.sql')
print( client_details)

def calculate_last_4_weekdays(client_details, today):
    # Get the day name for today (e.g., 'Tuesday')
    today_day_name = today.day_name()

    # Filter data for the last 4 occurrences of the same day of the week
    past_4_same_weekdays = client_details[
        (client_details['case_receiving_date'] < today) & 
        (client_details['case_receiving_date'].dt.day_name() == today_day_name)
    ].groupby('client_id').tail(4)

    # Calculate the average case count for these days
    avg_case_count_4_weeks = past_4_same_weekdays.groupby('client_id')['total_case'].mean().reset_index()
    avg_case_count_4_weeks.rename(columns={'total_case': 'avg_case_count_last_4_weeks'}, inplace=True)

    return avg_case_count_4_weeks

# Function to get yesterday's case count
def calculate_yesterday_cases(client_details, yesterday):
    # Filter data for yesterday
    yesterday_cases = client_details[client_details['case_receiving_date'] == yesterday].copy()

    # Rename the column for clarity
    yesterday_cases.rename(columns={'total_case': 'yesterday_case_count'}, inplace=True)

    return yesterday_cases[['client_id', 'yesterday_case_count']]


# Function to get today's case count
def calculate_today_cases(client_details, today):
    # Filter data for today
    today_cases = client_details[client_details['case_receiving_date'] == today].copy()

    # Rename the column for clarity
    today_cases.rename(columns={'total_case': 'today_case'}, inplace=True)

    return today_cases[['client_id', 'client_name', 'today_case']]

# Function to calculate weekly case count (current week)
def calculate_weekly_case_count(client_details, today):
    # Determine the start of the week (Monday) for the current week
    start_of_week = today - pd.DateOffset(days=today.weekday())
    
    # Filter data for the current week
    weekly_cases = client_details[(client_details['case_receiving_date'] >= start_of_week) & (client_details['case_receiving_date'] <= today)]
    weekly_cases = weekly_cases.groupby('client_id')['total_case'].sum().reset_index()
    weekly_cases.rename(columns={'total_case': 'week_case_count'}, inplace=True)

    return weekly_cases

# Function to calculate monthly case count (current month)
def calculate_monthly_case_count(client_details, today):
    # Filter data for the current month
    monthly_cases = client_details[client_details['case_receiving_date'].dt.month == today.month]
    monthly_cases = monthly_cases.groupby('client_id')['total_case'].sum().reset_index()
    monthly_cases.rename(columns={'total_case': 'month_case_count'}, inplace=True)

    return monthly_cases

# Function to calculate case counts for the last 3 months
def calculate_last_3_months(client_details, today):
    last_3_months = client_details[
        (client_details['case_receiving_date'] < today) &
        (client_details['case_receiving_date'].dt.month >= (today.month - 3))
    ]
    monthly_cases = last_3_months.groupby(['client_id', client_details['case_receiving_date'].dt.month])['total_case'].sum().reset_index()
    avg_case_count_3_months = monthly_cases.groupby('client_id')['total_case'].mean().reset_index()
    avg_case_count_3_months.rename(columns={'total_case': 'avg_case_count_last_3_months'}, inplace=True)
    
    return avg_case_count_3_months

# Function to calculate case counts for the last 3 weeks
def calculate_last_3_weeks(client_details, today):
    # Determine the start of the week for the last 3 weeks
    start_of_last_3_weeks = today - pd.DateOffset(weeks=4)
    
    # Filter data for the last 3 weeks
    past_3_weeks = client_details[(client_details['case_receiving_date'] >= start_of_last_3_weeks) & (client_details['case_receiving_date'] < today)]
    weekly_cases = past_3_weeks.groupby(['client_id', client_details['case_receiving_date'].dt.isocalendar().week])['total_case'].sum().reset_index()
    
    # Calculate the average case count for the last 3 weeks
    avg_case_count_3_weeks = weekly_cases.groupby('client_id')['total_case'].mean().reset_index()
    avg_case_count_3_weeks.rename(columns={'total_case': 'avg_case_count_last_3_weeks'}, inplace=True)
    
    return avg_case_count_3_weeks

# Function to calculate drop percentages for week and month
def calculate_drop_percentages_with_trend(client_details):
    client_details['drop_percentage_today'] = ((client_details['today_case'] - client_details['yesterday_case_count']) / client_details['yesterday_case_count']) * 100
    client_details['drop_percentage_4_weeks'] = ((client_details['today_case'] - client_details['avg_case_count_last_4_weeks']) / client_details['avg_case_count_last_4_weeks']) * 100
    client_details['drop_percentage_week'] = ((client_details['week_case_count'] - client_details['avg_case_count_last_3_weeks']) / client_details['avg_case_count_last_3_weeks']) * 100
    client_details['drop_percentage_month'] = ((client_details['month_case_count'] - client_details['avg_case_count_last_3_months']) / client_details['avg_case_count_last_3_months']) * 100
    return client_details

# Main function to generate the report
def generate_case_comparison_report_with_trends(client_details):
    # Get today's and yesterday's date
    today = pd.Timestamp.today().normalize()
    yesterday = today - pd.DateOffset(days=1)

    # Get case counts for today, yesterday, and the last 4 weeks (same weekday)
    today_cases = calculate_today_cases(client_details, today)
    yesterday_cases = calculate_yesterday_cases(client_details, yesterday)
    avg_case_count_4_weeks = calculate_last_4_weekdays(client_details, today)

    # Get weekly and monthly case counts
    weekly_case_count = calculate_weekly_case_count(client_details, today)
    monthly_case_count = calculate_monthly_case_count(client_details, today)

    # Get average case counts for last 3 months and last 3 weeks
    avg_case_count_3_months = calculate_last_3_months(client_details, today)
    avg_case_count_3_weeks = calculate_last_3_weeks(client_details, today)

    # Merge data into a single report
    report = today_cases.merge(yesterday_cases, on='client_id', how='left')
    report = report.merge(avg_case_count_4_weeks, on='client_id', how='left')
    report = report.merge(weekly_case_count, on='client_id', how='left')
    report = report.merge(monthly_case_count, on='client_id', how='left')
    report = report.merge(avg_case_count_3_weeks, on='client_id', how='left')
    report = report.merge(avg_case_count_3_months, on='client_id', how='left')

    # Calculate percentage drops
    report = calculate_drop_percentages_with_trend(report)

    # Select relevant columns for the final report
    final_report = report[['client_id', 'client_name', 'today_case', 'yesterday_case_count',
                           'week_case_count', 'month_case_count', 'drop_percentage_today', 
                           'avg_case_count_last_4_weeks', 'drop_percentage_4_weeks', 
                           'avg_case_count_last_3_weeks', 'drop_percentage_week',
                           'avg_case_count_last_3_months', 'drop_percentage_month']]
    
    return final_report

# Example usage:
# Assuming client_details is your DataFrame with columns: client_id, client_name, onboarded_date, case_Receiving_Date, total_Case_count
client_details['case_receiving_date'] = pd.to_datetime(client_details['case_receiving_date'])  # Ensure date column is in datetime format
case_comparison_report = generate_case_comparison_report_with_trends(client_details)

# Display the report
print("case_comparison_report: ")
print(case_comparison_report)
case_comparison_report.to_csv("case_comparison_report.csv")
