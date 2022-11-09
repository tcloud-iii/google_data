import requests
import json
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build

from datetime import date

today = str(date.today())

# Locate the json key file generated in the previous step
KEY_FILE_LOCATION = '/home/tcloud_iii_gcp/tcloud-ga.json'

def connect(KEY_FILE_LOCATION):
    """Create a connection to the Google Search Console API and return service object.
    
    Args:
        key (string): Google Search Console JSON client secrets path.
    
    Returns:
        service (object): Google Search Console service object.
    """
    
    scope = ['https://www.googleapis.com/auth/webmasters.readonly']
    credentials = service_account.Credentials.from_service_account_file(KEY_FILE_LOCATION, scopes=scope)
    service = build('searchconsole', 'v1',
        credentials=credentials
    )
    
    return service

def query(service, site_url, payload):
    """Run a query on the Google Search Console API and return a dataframe of results.
    
    Args:
        service (object): Service object from connect()
        site_url (string): URL of Google Search Console property
        payload (dict): API query payload dictionary
    
    Return:
        df (dataframe): Pandas dataframe containing requested data. 
    
    """
    
    response = service.searchanalytics().query(siteUrl=site_url, body=payload).execute()
    
    results = []
    
    for row in response['rows']:    
        data = {}
        
        for i in range(len(payload['dimensions'])):
            data[payload['dimensions'][i]] = row['keys'][i]

        data['clicks'] = row['clicks']
        data['impressions'] = row['impressions']
        data['ctr'] = round(row['ctr'] * 100, 2)
        data['position'] = round(row['position'], 2)        
        results.append(data)
    
    return pd.DataFrame.from_dict(results)

service = connect(KEY_FILE_LOCATION)

payload = {
    'startDate': "2021-07-01",
    'endDate': today,
    'dimensions': ["page","device","query"],
    'rowLimit': 20000,
    'startRow': 0
}

site_url = "https://www.tcloud.gov.tw/"

df = query(service, site_url, payload)

#to bigquery
from google.cloud import bigquery
import os
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = KEY_FILE_LOCATION
client = bigquery.Client()

dataset_ref = client.dataset('search_console')
table_ref = dataset_ref.table('search_console')

job_config = bigquery.job.LoadJobConfig()
job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
client.load_table_from_dataframe(df, table_ref, job_config=job_config)


print('GSC files have sent to Bigquery!')
