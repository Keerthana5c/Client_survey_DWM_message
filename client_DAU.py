import os
import pandas as pd
import json
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
import mailchimp_transactional as MailchimpTransactional
from mailchimp_transactional.api_client import ApiClientError
from jinja2 import FileSystemLoader, Environment
from sqlalchemy import create_engine, text
import smtplib
from urllib import parse
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# from prefect import task, Flow
from dotenv import load_dotenv

env_path = '.env'
load_dotenv(dotenv_path=env_path)
MAILCHIMP_API_KEY = os.getenv("MAILCHIMP_API_KEY")

def send_notification_mail_mailChimp(to_email, cc_email, subject, message, from_email='insights@5cnetwork.com'):
    combined_emails = to_email + cc_email

    message = {
        "from_email": from_email,
        "from_name": 'insights',
        "subject": subject,
        "html": message,
        "to": [
            {
                "email": email,
                "type": "to" if email in to_email else "cc"
            } for email in combined_emails
        ],
    }
    # try:
    mailchimp = MailchimpTransactional.Client(MAILCHIMP_API_KEY)
    response = mailchimp.messages.send({"message": message})
    status = response[0]['status']
    print('API called successfully: {}'.format(response))
    if status in ['queued', 'sent']:
        return 202
    else:
        return 500

def detect_dau_churn(client_details):
    # Ensure 'case_receiving_date' is in datetime format
    client_details['case_receiving_date'] = pd.to_datetime(client_details['case_receiving_date'])
    print(client_details)
    
    # Define T-Period: T = today - 3 days, T30 = T - 29 days (30-day window)
    today = datetime.now().date()

    print("Today: ", today)
    T = today - timedelta(days=3)
    T30 = T - timedelta(days=29)
    
    T = pd.to_datetime(T)
    T30 = pd.to_datetime(T30)

    print(f"T (D-3): {T}, T30: {T30}")
    # Filter data for the T-Period (last 30 days from T30 to T)
    t_period_data = client_details[client_details['case_receiving_date'].between(T30, T)]

    # Filter data for the T-Period (last 30 days from T30 to T)
    t_period_data = client_details[client_details['case_receiving_date'].between(T30, T)]
    print("Filtered T-Period Data:")
    print(t_period_data)
    
    # Group by client and calculate active days (A) and average case load (B)
    client_activity = t_period_data.groupby('client_fk').agg(
        active_days=('case_receiving_date', 'nunique'),  # Number of active days
        avg_case_load=('total_case', 'mean')  # Average case load
    ).reset_index()
    
    print("Client Activity (Active Days and Avg Case Load):")
    print(client_activity)
    client_activity.to_csv("client_activity.csv")
    
    # Filter clients with average case load > 5
    client_activity = client_activity[client_activity['avg_case_load'] > 5]
    print(client_activity)
    
    # Get D-1, D-2, D-3, D-4, D-5
    d_1 = pd.to_datetime(today - timedelta(days=1))
    d_2 = pd.to_datetime(today - timedelta(days=2))
    d_3 = pd.to_datetime(today - timedelta(days=3))
    d_4 = pd.to_datetime(today - timedelta(days=4))
    d_5 = pd.to_datetime(today - timedelta(days=5))
    
    print("d_1:", d_1, "d_2:", d_2, "d_3:", d_3, "d_4:", d_4, "d_5:", d_5)

    # Filter the client_details DataFrame for recent cases (D-1 to D-5)
    recent_cases = client_details[client_details['case_receiving_date'].isin([d_1, d_2, d_3, d_4, d_5])]
    print("recent_cases:")
    print(recent_cases)
    
    # Categorization Logic

    # For Cat-1 Churn: A = 30, B > 5, no cases on D-1
    cat1_clients = client_activity[(client_activity['active_days'] == 30)]
    cat1_recent = recent_cases[recent_cases['case_receiving_date'] == d_1]
    
    cat1_no_cases = cat1_clients[~cat1_clients['client_fk'].isin(cat1_recent['client_fk'])]
    cat1_no_cases['client_active_type'] = 'DAU'
    cat1_no_cases['churn_category'] = 'Critical DAU30 Cat-1 Churn'
    
    print("Cat-1 Churn:")
    print(cat1_no_cases)

    # For Cat-2 Churn: 28 <= A < 30, B > 5, no cases on D-1, D-2, D-3
    cat2_clients = client_activity[(client_activity['active_days'] >= 28) & (client_activity['active_days'] < 30)]
    cat2_recent = recent_cases[recent_cases['case_receiving_date'].isin([d_1, d_2, d_3])]
    
    cat2_no_cases = cat2_clients[~cat2_clients['client_fk'].isin(cat2_recent['client_fk'])]
    cat2_no_cases['client_active_type'] = 'DAU'
    cat2_no_cases['churn_category'] = 'Critical DAU>28 Cat-2 Churn'
    
    print("Cat-2 Churn:")
    print(cat2_no_cases)

    # For Cat-3 Churn: 25 <= A < 28, B > 5, no cases on D-1, D-2, D-3, D-4, D-5
    cat3_clients = client_activity[(client_activity['active_days'] >= 25) & (client_activity['active_days'] < 28)]
    cat3_recent = recent_cases[recent_cases['case_receiving_date'].isin([d_1, d_2, d_3, d_4, d_5])]
    # print(cat1_recent,,cat2_clients,cat2_recent,"Hi",cat3_recent)
    cat3_no_cases = cat3_clients[~cat3_clients['client_fk'].isin(cat3_recent['client_fk'])]
    cat3_no_cases['client_active_type'] = 'DAU'
    cat3_no_cases['churn_category'] = 'Critical DAU>25 Cat-3 Churn'
    
    print("Cat-3 Churn:")
    print(cat3_no_cases)
    
    # Combine all the churn categories into a single DataFrame
    churn_clients = pd.concat([cat1_no_cases, cat2_no_cases, cat3_no_cases], ignore_index=True)

    # Return the final result with client categorization
    return churn_clients[['client_fk', 'active_days', 'avg_case_load', 'client_active_type', 'churn_category']]

# Assuming client_details is your DataFrame containing case details
client_details = get_data_cc('sql/clients_details.sql')
case_details = get_data_cc('sql/case_details.sql')
print("case_details: ")
print(case_details)
contact_details = get_data_cc('sql/contact_details.sql')
client_survey_df = get_yaake_data('postgres', 'sql/client_survey.sql')
print("client_survey: ", client_survey_df)


# Detect DAU churn
dau_churn = detect_dau_churn(client_details)
# print(dau_churn)
dau_churn.to_csv("dau_churn.csv")

# Merging churn clients with contact and case details
merged_df = dau_churn.merge(contact_details, on='client_fk', how='left').merge(case_details, on='client_fk', how='left')
print("merged_df after reading case)details and contact_Details: ")
print(merged_df)
merged_df.drop(columns=['persona_type', 'client_name_y', 'designation', 'XRAY_Count',
       'Within_TAT_XRAY', 'XRAY_Rework', 'CT_Count', 'Within_TAT_CT',
       'CT_Rework', 'MRI_Count', 'Within_TAT_MRI', 'MRI_Rework', 'NM_Count',
       'Within_TAT_NM', 'NM_Rework'], inplace=True)
merged_df = merged_df.loc[:, ~merged_df.columns.str.contains('^Unnamed')]
print(merged_df.columns)
merged_df = merged_df.iloc[:, [0, 4, 1, 2, 3, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18,19, 20, 21]]
merged_df.rename(columns={'client_name_x': 'Client_Name', 'contact_id': 'contact_fk', 'churn_category': 'churn_type'}, inplace= True)
merged_df.to_csv("merged_df.csv")
merged_df['phone_number'].replace('NA', pd.NA, inplace=True)

#removing rows where both number and mail are null
merged_df = merged_df[
    (merged_df['phone_number'].notnull() & (merged_df['phone_number'] != '') ) | 
    (merged_df['email'].notnull() & (merged_df['email'] != ''))
]
merged_df.to_csv("cleaned_merged_df.csv", index=False)

#columns to be saved in the client_survey table in DB
columns_to_keep = ['contact_fk', 'client_fk' , 'client_active_type', 'churn_type']
filtered_client_survey =  merged_df[columns_to_keep].drop_duplicates(subset=['contact_fk'])
filtered_client_survey ['created_at'] = datetime.now()
filtered_client_survey['updated_at'] = datetime.now()
print("filtered_client_survey: ", filtered_client_survey )

def upload_to_collection(filtered_client_survey, client_survey, append_type = 'append'):
    host = os.getenv("YAAKE_HOST")
    username = os.getenv("YAAKE_USER")
    password = parse.quote(os.getenv("YAAKE_PASSWORD"))
    port = os.getenv("YAAKE_PORT")
    mydb = create_engine(f'postgresql://'+ username + ':' + password + '@' + host + ':' + str(port) + '/postgres', connect_args={'options':'-csearch_path={}'.format('public')} )
    print(mydb)
    
    #list to store the created_id in client survey table
    created_ids = []
  
    with mydb.connect() as connection:
        for index, row in filtered_client_survey.iterrows():
            # Check if there is a previous entry for the client
            existing_entry_query = text(f"""
                SELECT created_at FROM client_survey
                WHERE contact_fk = :contact_fk
                ORDER BY created_at DESC
                LIMIT 1
            """)
            result = connection.execute(existing_entry_query, {'contact_fk': row['contact_fk'], 'client_fk': row['client_fk']}).fetchone()

            # Determine the value for 'trigger' based on the result
            if result is None:
                # No previous entry for this client, so set trigger to True
                trigger = True
            else:
                # Check if the previous entry is more than 7 days old
                last_created_at = result[0].replace(tzinfo = None)
                trigger = (datetime.now() - last_created_at) > timedelta(days=7)

            insert_query = text(f""" INSERT INTO client_survey (contact_fk, client_fk, client_active_type, churn_type, trigger, created_at, updated_at) 
                VALUES (:contact_fk, :client_fk, :client_active_type, :churn_type, :trigger, :created_at, :updated_at) RETURNING id  -- Return the generated ID""")
            # print("insert_query: ", insert_query)
            result = connection.execute(insert_query, {
                'contact_fk': row['contact_fk'],
                'client_fk': row['client_fk'],
                'client_active_type': row['client_active_type'],
                'churn_type': row['churn_type'],
                'trigger': trigger,
                'created_at': row['created_at'],
                'updated_at': row['updated_at'] 
            })
            created_id = result.fetchone()[0]
            created_ids.append({
                'created_id': created_id,
                'contact_fk': row['contact_fk'],
                'client_fk': row['client_fk'],
                'trigger': trigger,
                'created_at': row['created_at']
            })
            
            connection.commit()
            
    print("Data upload complete")
    # Convert created_ids to DataFrame for merging
    created_ids_df = pd.DataFrame(created_ids)
    created_ids_df.to_csv("created_id_df.csv")
    
    # survey_merged = pd.merge(created_ids_df, contact_details, on='client_fk', how='left')
    # survey_merged.to_csv("contact_merge.csv")
    survey_merged_df = pd.merge(created_ids_df, merged_df, on=['client_fk', 'contact_fk'], how='left' )
    print("survey_merged_df", survey_merged_df)
    survey_merged_df.to_csv("contact_merge_with_survey.csv")
    # Filter rows with trigger=True
    to_notify = survey_merged_df[survey_merged_df['trigger'] == True]
    print("to_notify", to_notify)
    to_notify.to_csv("to_notify.csv")
    # to_notify.drop(columns=['client_fk_y', 'unique_id_y', 'phone_number_y', 'email_y'], inplace=True)
    # to_notify.rename(columns={'client_fk_x': 'client_fk', 'unique_id_x': 'unique_id', 'phone_number_x': 'phone_number', 'email_x': 'email'}, inplace= True)
    print(to_notify.columns)
    
    
    # Create feedback link and trigger message
    for _, row in to_notify.iterrows():
        feedback_link = f"https://yaake.cubebase.ai/Y2FtcGFpZ24K_!43w111Y2FtcGFpZ24K/dXJsZW5jb2Rl!sdsdfdXJsZW5jb2Rl/Y2xpZW50X3N1cnZleQ==?id={row['created_id']}"
        send_message(row['phone_number'], row['email'], feedback_link, to_notify, row['client_fk'], row['contact_fk'])

    print("Messages sent for triggered contacts.")
    return created_ids,survey_merged_df
# upload_to_collection(filtered_client_survey, 'client_survey')

# def send_message(phone_number, email, feedback_link, to_notify, client_id, contact_id):
#     # Set up Jinja2 environment and load the template
#     env = Environment(loader=FileSystemLoader('template'))
#     template = env.get_template('mail_template.html')

#     if email is not None and len(email) > 0:
#         # Prepare modality data, example given for XRAY
#         modality_data = {}
        
#         client_data = to_notify[to_notify['client_fk'] == client_id]
#         print("client_data", client_data)
#         if client_data['Last_30_XRAY_Count'].values[0] > 0:
#                modality_data['XRAY'] = {
#                    'total_XRAY': client_data['Last_30_XRAY_Count'].values[0],
#                    'avg_tat': '60 Mins',
#                    'delivered_within_tat': client_data['Last_30_Within_TAT_XRAY'].values[0],
#                    'percentage': (client_data['Last_30_Within_TAT_XRAY'].values[0] / client_data['Last_30_XRAY_Count'].values[0] * 100),
#                    'rework': client_data['Last_30_XRAY_Rework'].values[0]
#                }

#         if client_data['Last_30_CT_Count'].values[0] > 0:
#             modality_data['CT'] = {
#                 'total_CT': client_data['Last_30_CT_Count'].values[0],
#                 'avg_tat': '120 Mins',
#                 'delivered_within_tat': client_data['Last_30_Within_TAT_CT'].values[0],
#                 'percentage': (client_data['Last_30_Within_TAT_CT'].values[0] / client_data['Last_30_CT_Count'].values[0] * 100),
#                 'rework': client_data['Last_30_CT_Rework'].values[0]
#             }


#         if client_data['Last_30_MRI_Count'].values[0] > 0:
#             modality_data['MRI'] = {
#                 'total_MRI': client_data['Last_30_MRI_Count'].values[0],
#                 'avg_tat': '180 Mins',
#                 'delivered_within_tat': client_data['Last_30_Within_TAT_MRI'].values[0],
#                 'percentage': (client_data['Last_30_Within_TAT_MRI'].values[0] / client_data['Last_30_MRI_Count'].values[0] * 100),
#                 'rework': client_data['Last_30_MRI_Rework'].values[0]
#             }


#         if 'Last_30_NM_Count' in client_data.columns and client_data['Last_30_NM_Count'].values[0] > 0:
#             modality_data['NM'] = {
#                 'total_NM': client_data['Last_30_NM_Count'].values[0],
#                 'avg_tat': '240 Mins',
#                 'delivered_within_tat': client_data['Last_30_Within_TAT_NM'].values[0] if 'Last_30_Within_TAT_NM' in client_data.columns else 0,
#                 'percentage': (client_data['Last_30_Within_TAT_NM'].values[0] / client_data['Last_30_NM_Count'].values[0] * 100) if 'Last_30_Within_TAT_NM' in client_data.columns else 0,
#                 'rework': client_data['Last_30_NM_Rework'].values[0] if 'Last_30_NM_Rework' in client_data.columns else 0
#             }
        
#         print("modlaity_data: ",modality_data)

#         # If modality data exists, render the email and prepare to send
#         if modality_data:
#             # Render the email content with Jinja2
#             html_message = template.render(
#                 Modality_Data=modality_data,
#                 Client_Name=client_data['Client_Name'].values[0],
#                 subject=" 📈 5CNetwork - Quick Check-In: Ensuring Seamless Case Activations with 5CNetwork",
#                 feedback_link=feedback_link
#             )
            
#             # Prepare email recipients
#             to_email =[email]
#             cc_email = ['keerthana.r@5cnetwork.com', 'sunil.kumar@5cnetwork.com', 'mahesh@5cnetwork.com']
            
#             # Send the email (assumes send_notification_mail_mailChimp function is defined elsewhere)
#             status = send_notification_mail_mailChimp(
#                 to_email=to_email,
#                 cc_email=cc_email,
#                 subject="📈 5CNetwork - Quick Check-In: Ensuring Seamless Case Activations with 5CNetwork",
#                 message=html_message
#             )
            
#             if status == 202:
#                 print(f"Email sent successfully to {email} for client {client_id} under contact {contact_id}!")
#             else:
#                 print(f"Failed to send email to {email} for client {client_id} under contact {contact_id}.")
#     else:
#         print(f"No valid modality data found for client {client_id} under contact {contact_id}.")
    
#     return "Mail process completed"
create_id,survey_merged_df=upload_to_collection(filtered_client_survey, 'client_survey')


# Step 2: Function to send WhatsApp messages
def send_whatsapp_message(payload, api_key):
    url = "https://api.gupshup.io/wa/api/v1/template/msg"
    headers = {
               'Content-Type': 'application/x-www-form-urlencoded',
               'Apikey':api_key}

    response = requests.post(url, headers=headers, data=payload)
    
    # Check if the response is JSON and handle errors
    try:
        response_data = response.json()
        status = response_data.get('status', 'No status in response')
    except json.JSONDecodeError:
        print("Error: Received non-JSON response:", response.text)
        status = "Error: Unable to parse response as JSON"
    
    return status

# Step 3: Iterate through each client in merged DataFrame
api_key = 'bhn6nqnfpieunjlerhevpfmktlb5gvb2'

for idx, row in survey_merged_df.iterrows():
    client_id = row['client_fk']
    contact_number = row['phone_number']  
    created_id = row['created_id']
    url = "https://yaake.cubebase.ai/Y2FtcGFpZ24K_!43w111Y2FtcGFpZ24K/dXJsZW5jb2Rl!sdsdfdXJsZW5jb2Rl/Y2xpZW50X3N1cnZleQ==?id={created_id}"
    
    if pd.isna(contact_number):
        # If contact_number is NaN, handle accordingly (e.g., print or store as NaN)
        print(f"Contact number is missing for client ID {client_id}")
        continue
    
    # Clean the contact number
    contact_number = str(contact_number).replace('+91-', '').replace('91-', '')
    if len(contact_number) == 10 and contact_number.isdigit():
        contact_number = '91' + contact_number
    else:
        print(f"Invalid contact number format for client ID {client_id}: {contact_number}")
    
    print("contact_number:", contact_number)
    
    # XRAY data
    xr_total_cases = row['Last_30_XRAY_Count']
    xr_delivered_1hr = row['Last_30_Within_TAT_XRAY']
    xr_rework = row['Last_30_XRAY_Rework']
    
    # CT data
    ct_total_cases = row['Last_30_CT_Count']
    ct_delivered_2hr = row['Last_30_Within_TAT_CT']
    ct_rework = row['Last_30_CT_Rework']
    
    # MRI data
    mri_total_cases = row['Last_30_MRI_Count']
    mri_delivered_3hr = row['Last_30_Within_TAT_MRI']
    mri_rework = row['Last_30_MRI_Rework']
    
    # NM data
    nm_total_cases = row['Last_30_NM_Count']
    nm_delivered = row['Last_30_Within_TAT_NM']
    nm_rework = row['Last_30_NM_Rework']
    
    # Check if the column exists before accessing it
    nm_rework = row['Last_30_NM_Rework'] if 'Last_30_NM_Rework' in survey_merged_df.columns else 0
    nm_total_cases = row['Last_30_NM_Count'] if 'Last_30_NM_Count' in survey_merged_df.columns else 0
    nm_delivered = row['Last_30_Within_TAT_NM'] if 'Last_30_Within_TAT_NM' in survey_merged_df.columns else 0


    # Calculate percentages for XRAY
    xr_delivered_percent = (xr_delivered_1hr / xr_total_cases * 100) if xr_total_cases > 0 else 0
    xr_rework_percent = (xr_rework / xr_total_cases * 100) if xr_total_cases > 0 else 0
    
    # Calculate percentages for CT
    ct_delivered_percent = (ct_delivered_2hr / ct_total_cases * 100) if ct_total_cases > 0 else 0
    ct_rework_percent = (ct_rework / ct_total_cases * 100) if ct_total_cases > 0 else 0
    
    # Calculate percentages for MRI
    mri_delivered_percent = (mri_delivered_3hr / mri_total_cases * 100) if mri_total_cases > 0 else 0
    mri_rework_percent = (mri_rework / mri_total_cases * 100) if mri_total_cases > 0 else 0
    
    # Calculate percentages for NM
    nm_delivered_percent = (nm_delivered / nm_total_cases * 100) if nm_total_cases > 0 else 0
    nm_rework_percent = (nm_rework / nm_total_cases * 100) if nm_total_cases > 0 else 0
    
    # Format percentages to two decimal places with a percentage symbol
    xr_delivered_percent = f"{xr_delivered_percent:.2f}%"
    xr_rework_percent = f"{xr_rework_percent:.2f}%"
    ct_delivered_percent = f"{ct_delivered_percent:.2f}%"
    ct_rework_percent = f"{ct_rework_percent:.2f}%"
    mri_delivered_percent = f"{mri_delivered_percent:.2f}%"
    mri_rework_percent = f"{mri_rework_percent:.2f}%"
    nm_delivered_percent = f"{nm_delivered_percent:.2f}%"
    nm_rework_percent = f"{nm_rework_percent:.2f}%"
    
    # avg TAT
    avg_XRAY_TAT = '60'
    avg_CT_TAT = '120'
    avg_MRI_TAT = '180'
    avg_NM_TAT = '2400'
    
    # Prepare the params for the template
    params = [
        avg_XRAY_TAT, xr_total_cases, xr_delivered_1hr, xr_delivered_percent,
        xr_rework, xr_rework_percent,
        avg_CT_TAT, ct_total_cases, ct_delivered_2hr, ct_delivered_percent,
        ct_rework, ct_rework_percent,
        avg_MRI_TAT, mri_total_cases, mri_delivered_3hr, mri_delivered_percent,
        mri_rework, mri_rework_percent,
        avg_NM_TAT, nm_total_cases, nm_delivered, nm_delivered_percent,
        nm_rework, nm_rework_percent,
        f"{created_id}"
    ]
    
    # Prepare the payload with params
    payload = {
        'channel': 'whatsapp',
        'source': '918951359309',
        # 'destination': '916381524434',
        'destination': contact_number,
        'src.name': 'tHmYIgOKgztIGYG000ZhFHQp',
        'template': json.dumps({
            'id': 'edf7cccf-cc07-4afc-a322-667956898dec',
            'params': params
        })
    }

    # URL-encode the payload data
    encoded_payload = urlencode(payload)
    
    print("Payload:", payload)
    print("Encoded Payload:", encoded_payload)
    
    # Send WhatsApp message
    response_status = send_whatsapp_message(encoded_payload, api_key)
    print(f"Message sent to {contact_number}, Status: {response_status}")
    
    
   