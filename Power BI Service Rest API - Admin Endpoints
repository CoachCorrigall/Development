# Import necessary libraries
import requests
import json
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from azure.storage.blob import BlobServiceClient
from datetime import datetime
import msal

# Step 1: Azure AD app registration details
tenant_id = 'my tenant_id'         
client_id = 'my client_id'         
client_secret = 'my client_secret' 

# Step 2: Authority and scope for Power BI API
authority_url = f'https://login.microsoftonline.com/{tenant_id}'
scope = ['https://analysis.windows.net/powerbi/api/.default']

# Step 3: Create a confidential client application for Azure AD authentication
app = msal.ConfidentialClientApplication(
    client_id=client_id,
    client_credential=client_secret,
    authority=authority_url
)

# Step 4: Acquire an access token
result = app.acquire_token_for_client(scopes=scope)

# Check if authentication is successful
if 'access_token' in result:
    access_token = result['access_token']
    print("Authentication successful.")
else:
    print("Authentication failed.")
    print(result.get("error"))
    print(result.get("error_description"))
    print(result.get("correlation_id"))
    raise SystemExit

# Step 5: Define the list of Power BI API endpoints
endpoints = ["reports", "datasets", "groups", "dataflows", "dashboards"]

# Step 6: Loop through each endpoint
for endpoint in endpoints:
    # Define the dynamic URL for the current endpoint
    if endpoint == "groups":
        url = f"https://api.powerbi.com/v1.0/myorg/admin/{endpoint}?$top=5000"
    else:
        url = f"https://api.powerbi.com/v1.0/myorg/admin/{endpoint}"

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    # Make the API call to get data
    response = requests.get(url, headers=headers)
    
    # Check if the API call was successful
    if response.status_code == 200:
        data = response.json()
        print(f"Data retrieved successfully from {endpoint}.")
    else:
        print(f"API call failed with status code: {response.status_code} for {endpoint}")
        continue  # Skip to the next endpoint in case of failure
    
    # Convert the data to a pandas DataFrame
    df = pd.json_normalize(data['value'])

    # Clean column names by replacing invalid characters (e.g., dots) with underscores
    df.columns = df.columns.str.replace(r'[^\w]', '_', regex=True)

    # Add a new column 'ETLImportDate' with today's date
    df['ETLImportDate'] = datetime.today().strftime('%Y-%m-%d')
    
    # Save the DataFrame to a local Parquet file, with the file name reflecting the endpoint
    output_file = f'Get{endpoint.capitalize()}.parquet'
    df.to_parquet(output_file, index=False)
    print(f"Data saved locally as {output_file}")
    
    # Step 9: Upload the Parquet file to Azure Blob Storage
    # Replace with your actual storage connection string and container details
    storage_connection_string = 'my storage_connection_string'
    container_name = 'powerbi-restapi/admin/'
    blob_name = f'{output_file}'   # Use the dynamic output_file name for the blob

    # Create the BlobServiceClient object
    blob_service_client = BlobServiceClient.from_connection_string(storage_connection_string)

    # Get the container client
    container_client = blob_service_client.get_container_client(container_name)

    # Upload the Parquet file to Azure Blob Storage
    with open(output_file, "rb") as data:
        container_client.upload_blob(name=blob_name, data=data, overwrite=True)
        print(f"File uploaded to Azure Blob Storage container '{container_name}' as '{blob_name}'.")
