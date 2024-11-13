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

def compare_cases(client_details):
    client_details['case_receiving_date'] = pd.to_datetime(client_details['case_receiving_date'])

    # Get today's date and define the last 30 days range
    today = pd.Timestamp.today().normalize()
    yesterday = today - timedelta(days=1)
    day_before_yesterday = today - timedelta(days=2)
    
    last_30_days = today - timedelta(days=30)
    # Filter data for the last 30 days
    df_last_30_days = client_details[client_details['case_receiving_date'].between(last_30_days, today)]
    print(df_last_30_days)

    # Group data by client and date to get the daily case count per client
    daily_cases = df_last_30_days.groupby(['client_id', 'client_name', 'case_receiving_date']).agg(
        total_cases=pd.NamedAgg(column='total_case', aggfunc='sum')
    ).reset_index()
    print(daily_cases)
    daily_cases.to_csv("daily_case.csv")

    df_30=daily_cases.groupby('client_name')['case_receiving_date'].nunique().reset_index()
    client_list=df_30[df_30['case_receiving_date'] > 25]['client_name'].to_list()
    filtered_daily_cases = daily_cases[daily_cases['client_name'].isin(client_list)]
    min_cases_per_client = filtered_daily_cases.groupby('client_name')['total_cases'].min().reset_index()
    print("min_cases_per_client: ")
    print(min_cases_per_client)

    clients_over_10_cases_every_day = min_cases_per_client[min_cases_per_client['total_cases'] > 5]['client_name'].to_list()
    required_df=daily_cases[daily_cases['client_name'].isin(clients_over_10_cases_every_day)]
    print("clients_with_more_than_5_cases: ")
    print(required_df)

    # Get cases for today, yesterday, and day before yesterday
    today_cases = required_df[required_df['case_receiving_date'] == today]
    yesterday_cases = required_df[required_df['case_receiving_date'] == yesterday]
    day_before_yesterday_cases = required_df[required_df['case_receiving_date'] == day_before_yesterday]

    # Merge cases for comparison
    case_comparison = pd.merge(yesterday_cases, day_before_yesterday_cases, on='client_id', suffixes=('_yesterday', '_day_before'))

    # Add today's cases to the comparison
    case_comparison = pd.merge(case_comparison, today_cases[['client_id', 'total_cases']], on='client_id', how='left')
    case_comparison.rename(columns={'total_cases': 'total_cases_today'}, inplace=True)

    # Calculate percentage drop between today and yesterday
    case_comparison['drop_today_vs_yesterday'] = ((case_comparison['total_cases_today'] - case_comparison['total_cases_yesterday']) /
                                                  case_comparison['total_cases_yesterday']) * 100

    # Calculate percentage drop between yesterday and day before yesterday
    case_comparison['drop_yesterday_vs_day_before'] = ((case_comparison['total_cases_yesterday'] - case_comparison['total_cases_day_before']) /
                                                       case_comparison['total_cases_day_before']) * 100

    # Calculate percentage drop between today and day before yesterday
    case_comparison['drop_today_vs_day_before'] = ((case_comparison['total_cases_today'] - case_comparison['total_cases_day_before']) /
                                                   case_comparison['total_cases_day_before']) * 100

    # Return the final DataFrame with all relevant information
    result = case_comparison[['client_id', 'client_name_yesterday', 'total_cases_today', 'total_cases_yesterday', 'total_cases_day_before',
                              'drop_today_vs_yesterday', 'drop_yesterday_vs_day_before', 'drop_today_vs_day_before']]
    
    return result

# Fetch client details
client_details = get_data_cc('sql/case_detail.sql')

# Call the compare_cases function and print the result
comparison_result = compare_cases(client_details)
print(comparison_result)
comparison_result.to_csv("comparison_result.csv")



# import os
# import pandas as pd
# import pytz,traceback,sys
# import time
# import logging
# import datetime as dt
# from database_ops import get_data_cc
# from datetime import datetime, timedelta
# # from slack_notification import slack_notification

# # from prefect import task, Flow
# from dotenv import load_dotenv

# env_path = '.env'
# load_dotenv(dotenv_path=env_path)

# client_details = get_data_cc('sql/case_detail.sql')
# print( client_details)

# client_details['case_receiving_date'] = pd.to_datetime(client_details['case_receiving_date'])

# # Get today's date and define the last 30 days range
# today = pd.Timestamp.today().normalize()
# last_30_days = today - timedelta(days=30)

# # Filter data for the last 30 days
# df_last_30_days = client_details[client_details['case_receiving_date'].between(last_30_days, today)]
# print(df_last_30_days)

# # Group data by client and date to get the daily case count per client
# daily_cases = df_last_30_days.groupby(['client_id', 'client_name', 'case_receiving_date']).agg(
#     total_cases=pd.NamedAgg(column='total_case', aggfunc='sum')
# ).reset_index()
# print(daily_cases)
# daily_cases.to_csv("daily_case.csv")

# df_30=daily_cases.groupby('client_name')['case_receiving_date'].nunique().reset_index()
# client_list=df_30[df_30['case_receiving_date'] > 25]['client_name'].to_list()
# filtered_daily_cases = daily_cases[daily_cases['client_name'].isin(client_list)]
# min_cases_per_client = filtered_daily_cases.groupby('client_name')['total_cases'].min().reset_index()

# clients_over_10_cases_every_day = min_cases_per_client[min_cases_per_client['total_cases'] > 10]['client_name'].to_list()
# required_df=daily_cases[daily_cases['client_name'].isin(clients_over_10_cases_every_day)]
# print("clients_with_more_than_10_cases: ")
# print(required_df)


# yesterday = today - timedelta(days=1)
# day_before_yesterday = today - timedelta(days=2)

# # Get case counts for yesterday and day before yesterday
# yesterday_cases = required_df[required_df['case_receiving_date'] == yesterday]
# day_before_yesterday_cases = required_df[required_df['case_receiving_date'] == day_before_yesterday]

# # Merge yesterday and day before yesterday cases for comparison
# case_comparison = pd.merge(yesterday_cases, day_before_yesterday_cases, on='client_id', suffixes=('_yesterday', '_day_before'))
# print(case_comparison)

# # Calculate percentage drop between yesterday and day before yesterday
# case_comparison['drop_percentage'] = ((case_comparison['total_cases_yesterday'] - case_comparison['total_cases_day_before']) / 
#                                        case_comparison['total_cases_day_before']) * 100

# # Display the clients with their case drop percentage
# print(case_comparison[['client_id', 'client_name_yesterday', 'total_cases_yesterday', 'total_cases_day_before', 'drop_percentage']])