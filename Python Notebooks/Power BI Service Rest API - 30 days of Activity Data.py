# Install required libraries
##!pip install msal pyarrow azure-storage-blob

# Import libraries
import requests
import msal
import json
import pandas as pd
from azure.storage.blob import BlobServiceClient
from datetime import datetime, timedelta, timezone

# Azure AD app registration details
tenant_id = 'my tenant_id'         
client_id = 'my client_id'         
client_secret = 'my client_secret' 

# Authority and scope
authority_url = f'https://login.microsoftonline.com/{tenant_id}'
scope = ['https://analysis.windows.net/powerbi/api/.default']

# Create a confidential client application
app = msal.ConfidentialClientApplication(
    client_id=client_id,
    client_credential=client_secret,
    authority=authority_url
)

# Acquire a token
result = app.acquire_token_for_client(scopes=scope)

if 'access_token' in result:
    access_token = result['access_token']
    print("Authentication successful.")
else:
    print("Authentication failed.")
    print(result.get("error"))
    print(result.get("error_description"))
    print(result.get("correlation_id"))
    raise SystemExit

# Define the batch size in days (e.g., 2 days will run 2 separate one-day pulls)
batch_size = 1  # Change this to run batches for 2, 3, 4, etc. days

# Loop over the range of days you want to process
# For example, this will pull data for 10 days in total, split into batches of `batch_size` days
total_days_to_pull = 30  # Total number of days to pull data for
current_datetime = datetime.now(timezone.utc)

for batch_start in range(0, total_days_to_pull, batch_size):
    for day_offset in range(batch_size):
        # Calculate the start and end datetime for this single day
        start_datetime = (current_datetime - timedelta(days=batch_start + day_offset + 1)).replace(hour=0, minute=0, second=0, microsecond=0)
        end_datetime = (current_datetime - timedelta(days=batch_start + day_offset + 1)).replace(hour=23, minute=59, second=59, microsecond=0)
        
        # Format the datetime strings and wrap in single quotes
        startDateTime = f"'{start_datetime.strftime('%Y-%m-%dT%H:%M:%SZ')}'"
        endDateTime = f"'{end_datetime.strftime('%Y-%m-%dT%H:%M:%SZ')}'"

        # Format the date for the container name
        formatted_date = end_datetime.strftime("%Y%m%d")
        container_name = f'powerbi/Activity/{formatted_date}'
    
    print(f"Start DateTime: {startDateTime}")
    print(f"End DateTime: {endDateTime}")
    print(f"Container Name: {container_name}")

    # API endpoint
    base_url = 'https://api.powerbi.com/v1.0/myorg/admin/activityevents'

    # Parameters with single quotes around the date strings
    params = {
        'startDateTime': startDateTime,
        'endDateTime': endDateTime
    }

    # Headers with the access token
    headers = {
        'Authorization': f'Bearer {access_token}'
    }

    # Initialize variables
    all_events = []
    next_url = base_url

    while next_url:
        print(f'Requesting URL: continuationUri')  # For debugging
        response = requests.get(next_url, headers=headers, params=params)
        
        if response.status_code == 200:
            data = response.json()
            events = data.get('activityEventEntities', [])
            all_events.extend(events)
            continuation_uri = data.get('continuationUri')
            
            if continuation_uri:
                next_url = continuation_uri
                params = None  # No need for params after the first call
                print("Fetching next batch of events...")
            else:
                next_url = None  # Exit the loop
                print("All events fetched.")
        else:
            print(f'Error: {response.status_code} - {response.text}')
            break  # Exit the loop on error

    # Check if any events were fetched
    if all_events:
        df = pd.DataFrame(all_events)[['Id', 'RecordType', 'CreationTime', 'Operation',
                                        'OrganizationId', 'UserType', 'UserKey', 'Workload', 'UserId', 'ClientIP', 'UserAgent', 'Activity'
                                        ,'ItemName', 'WorkSpaceName', 'DatasetName', 'ReportName', 'WorkspaceId', 'ObjectId', 'DatasetId'
                                        ,'ReportId', 'IsSuccess', 'ReportType', 'RequestId', 'ActivityId', 'DistributionMethod'
                                        #,'ImportId', 'ImportSource', 'ImportType', 'ImportDisplayName' 
                                        ]]

        # Add columns for startDateTime and endDateTime to the DataFrame
        df = df.assign(
            startDateTime=startDateTime,
            endDateTime=endDateTime
        )
        
        print(f"Total events fetched (DataFrame row count): {len(df)}")
        
        # Save the DataFrame to a Parquet file
        output_file = 'powerbi_activity_events.parquet' ##f'powerbi_activity_events_{formatted_date}.parquet'
        df.to_parquet(output_file, index=False)
        print(f"Data saved to {output_file}")

        # Upload the Parquet file to Azure Blob Storage (optional)
        storage_connection_string = 'my storage_connection_string' 

        # Create the BlobServiceClient object
        blob_service_client = BlobServiceClient.from_connection_string(storage_connection_string)

        # Get the container client
        container_client = blob_service_client.get_container_client(container_name)

        # Upload the Parquet file
        with open(output_file, "rb") as data:
            container_client.upload_blob(name=output_file, data=data, overwrite=True)
            print(f"File uploaded to Azure Blob Storage container '{container_name}' as '{output_file}'.")
    else:
        print("No events were fetched.")
