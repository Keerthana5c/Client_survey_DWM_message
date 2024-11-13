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

client_details['case_receiving_date'] = pd.to_datetime(client_details['case_receiving_date'])

def analyse_day_wise_cases(client_details, current_date=None):
    if current_date is None:
        current_date = datetime.now().date()
    else:
        current_date = pd.to_datetime(current_date).date()

    # Calculate date ranges
    five_weeks_ago = current_date - timedelta(weeks=5)
    
    # Extract only the date part from 'case_receiving_date'
    client_details['case_receiving_date'] = pd.to_datetime(client_details['case_receiving_date']).dt.date

    # Filter data for last 5 weeks (to get 4 weeks of history + current day)
    filtered_df = client_details[(client_details['case_receiving_date'] > five_weeks_ago) & 
                                 (client_details['case_receiving_date'] <= current_date)]

    # Get the day of the week for the current date
    current_day_of_week = current_date.weekday()

    # Filter for the same day of the week over the last 5 weeks
    same_day_df = filtered_df[filtered_df['case_receiving_date'].apply(lambda x: x.weekday()) == current_day_of_week]

    # Separate current day and historical data
    current_day_data = same_day_df[same_day_df['case_receiving_date'] == current_date]
    historical_data = same_day_df[same_day_df['case_receiving_date'] < current_date]

    # Group historical data by client and date, then sum the total cases per date
    historical_data_sum = historical_data.groupby(['client_id', 'client_name', 'case_receiving_date']).agg({
        'total_case': 'sum'
    }).reset_index()
    
    # Calculate the average total cases for the same day of the week over the last 4 weeks
    avg_4_weeks = historical_data_sum.groupby('client_id').agg({
        'client_name': 'first',
        'total_case': 'mean'  # Mean of total cases per date
    }).rename(columns={'total_case': 'avg_cases_4w'})

    # Group current day data by client and sum cases for the current date
    current_day_data_sum = current_day_data.groupby(['client_id', 'client_name']).agg({
        'total_case': 'sum'
    }).reset_index()

    # Merge current day data with the historical averages
    result = pd.merge(current_day_data_sum, avg_4_weeks, on=['client_id', 'client_name'], how='right')

    # Calculate percentage differences
    result['case_diff_pct'] = (result['total_case'] - result['avg_cases_4w']) / result['avg_cases_4w'] * 100

    # Filter clients with dropped case count
    dropped_clients = result[result['case_diff_pct'] < 0].copy()

    # Round percentage differences for better readability
    dropped_clients['case_diff_pct'] = dropped_clients['case_diff_pct'].round(2)

    return dropped_clients[['client_id', 'client_name', 'total_case', 'avg_cases_4w', 'case_diff_pct']]

    # # Calculate averages for the same day of the week over the last 4 weeks
    # avg_4_weeks = historical_data.groupby('client_id').agg({
    #     'client_name': 'first',
    #     'total_case': 'mean'
    # }).rename(columns={'total_case': 'avg_cases_4w'})

    # # Merge with current day data
    # result = pd.merge(current_day_data, avg_4_weeks, on=['client_id', 'client_name'], how='right')
    
    # # Calculate percentage differences
    # result['case_diff_pct'] = (result['total_case'] - result['avg_cases_4w']) / result['avg_cases_4w'] * 100
    
    # # Filter clients with dropped case count
    # dropped_clients = result[result['case_diff_pct'] < 0].copy()
    # dropped_clients['case_diff_pct'] = dropped_clients['case_diff_pct'].round(2) 

    # return dropped_clients[['client_id', 'client_name', 'total_case', 'avg_cases_4w', 'case_diff_pct']]

def analyze_hour_wise_cases(client_details, current_date=None):
    # Ensure 'case_receiving_date' is in datetime format inside the function
    client_details['case_receiving_date'] = pd.to_datetime(client_details['case_receiving_date'], errors='coerce')

    if current_date is None:
        current_date = datetime.now().date()
    else:
        current_date = pd.to_datetime(current_date).date()

    # Calculate date ranges (to get 4 weeks of history + current day)
    five_weeks_ago = current_date - timedelta(weeks=5)

    # Filter data for the last 5 weeks
    filtered_df = client_details[
        (client_details['case_receiving_date'].dt.date > five_weeks_ago) &
        (client_details['case_receiving_date'].dt.date <= current_date)
    ]

    # Get the day of the week for the current date
    current_day_of_week = current_date.weekday()

    # Filter for the same day of the week (historical)
    same_day_df = filtered_df[filtered_df['case_receiving_date'].dt.dayofweek == current_day_of_week]

    # Separate current day and historical data
    current_day_data = same_day_df[same_day_df['case_receiving_date'].dt.date == current_date]
    historical_data = same_day_df[same_day_df['case_receiving_date'].dt.date < current_date]

    # Group by client and hour for the current day
    current_day_hourly = current_day_data.groupby(
        ['client_id', 'client_name', current_day_data['case_receiving_date'].dt.hour]
    )['total_case'].sum().reset_index(name='case_count')
    
    current_day_hourly.rename(columns={'case_receiving_date': 'hour'}, inplace=True)

    # Calculate average hourly case count for historical data
    historical_hourly = historical_data.groupby(
        ['client_id', 'client_name', historical_data['case_receiving_date'].dt.hour]
    )['total_case'].sum().reset_index(name='avg_case_count')
    
    historical_hourly.rename(columns={'case_receiving_date': 'hour'}, inplace=True)
    historical_hourly['avg_case_count'] = historical_hourly['avg_case_count'] / 4  # Average over 4 weeks

    # Merge current day and historical averages
    merged_df = pd.merge(current_day_hourly, historical_hourly, on=['client_id', 'client_name', 'hour'], how='outer')
    merged_df = merged_df.fillna(0)

    # Calculate percentage difference
    merged_df['case_diff_pct'] = ((merged_df['case_count'] - merged_df['avg_case_count']) / merged_df['avg_case_count']) * 100
    merged_df['case_diff_pct'] = merged_df['case_diff_pct'].replace([float('inf'), -float('inf')], 0).round(2)

    # Sort by client_id and hour
    merged_df = merged_df.sort_values(['client_id', 'hour'])

    return merged_df


def main():
    try:
        current_date = '2024-09-23'  # You can change this to any date you want to analyze
        
        # Day-wise analysis
        day_analysis = analyse_day_wise_cases(client_details, current_date)
        print("Day-wise case count analysis:")
        print(day_analysis)
        day_analysis.to_csv("day_wise_analysis.csv", index=False)

        # Hour-wise analysis
        hour_analysis = analyze_hour_wise_cases(client_details, current_date)
        print("\nHour-wise case count analysis:")
        print(hour_analysis)
        hour_analysis.to_csv("hour_wise_analysis.csv", index=False)

    except Exception as e:
        import traceback
        import sys
        print(traceback.format_exc())
        print(sys.exc_info()[2])

if __name__ == "__main__":
    main()



# def analyze_hourly_case_count(client_details, current_date=None):
#     if current_date is None:
#         current_date = datetime.now().date()
#     else:
#         current_date = pd.to_datetime(current_date).date()

#     # Calculate date ranges
#     five_weeks_ago = current_date - timedelta(weeks=5)

#     # Filter data for last 5 weeks (to get 4 weeks of history + current day)
#     filtered_df = client_details[(client_details['date'].dt.date > five_weeks_ago) & (client_details['date'].dt.date <= current_date)]

#     # Get the day of the week for the current date
#     current_day_of_week = current_date.weekday()

#     # Filter for the same day of the week
#     same_day_df = filtered_df[filtered_df['date'].dt.dayofweek == current_day_of_week]

#     # Separate current day and historical data
#     current_day_data = same_day_df[same_day_df['date'].dt.date == current_date]
#     historical_data = same_day_df[same_day_df['date'].dt.date < current_date]

#     # Group by client and hour for current day
#     current_day_hourly = current_day_data.groupby(['client_id', 'client_name', current_day_data['date'].dt.hour]).size().reset_index(name='case_count')
#     current_day_hourly.rename(columns={'date': 'hour'}, inplace=True)

#     # Calculate average hourly case count for historical data
#     historical_hourly = historical_data.groupby(['client_id', 'client_name', historical_data['date'].dt.hour]).size().reset_index(name='avg_case_count')
#     historical_hourly.rename(columns={'date': 'hour'}, inplace=True)
#     historical_hourly['avg_case_count'] = historical_hourly['avg_case_count'] / 4  # Average over 4 weeks

#     # Merge current day and historical averages
#     merged_df = pd.merge(current_day_hourly, historical_hourly, on=['client_id', 'client_name', 'hour'], how='outer')
#     merged_df = merged_df.fillna(0)

#     # Calculate percentage difference
#     merged_df['case_diff_pct'] = ((merged_df['case_count'] - merged_df['avg_case_count']) / merged_df['avg_case_count']) * 100
#     merged_df['case_diff_pct'] = merged_df['case_diff_pct'].replace([float('inf'), -float('inf')], 0)

#     # Sort by client_id and hour
#     merged_df = merged_df.sort_values(['client_id', 'hour'])

#     return merged_df

# # Analyze the data for a specific date
# analysis_result = analyze_hourly_case_count(client_details, current_date='2024-04-11')

# # Print the results
# print("Hour-wise case count analysis:")
# print(analysis_result)

# def main():
#     try:
#         analyse_client_cases(client_details, current_date=None)
#         analyse_hourly_case_count(client_details, current_date=None)

#     except Exception as e:
#         print(traceback.format_exc())
#         print(sys.exc_info()[2])

# if __name__ == "__main__":
#     main()
 