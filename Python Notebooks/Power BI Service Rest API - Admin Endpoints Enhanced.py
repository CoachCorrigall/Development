# Import necessary libraries
import requests
import json
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from azure.storage.blob import BlobServiceClient
from datetime import datetime
import msal

# Step 1: Azure AD Authentication
def get_access_token(tenant_id, client_id, client_secret, scope):
    authority_url = f'https://login.microsoftonline.com/{tenant_id}'
    app = msal.ConfidentialClientApplication(
        client_id=client_id,
        client_credential=client_secret,
        authority=authority_url
    )
    result = app.acquire_token_for_client(scopes=scope)
    if 'access_token' in result:
        print("Authentication successful.")
        return result['access_token']
    else:
        print(f"Authentication failed: {result.get('error_description')}")
        raise SystemExit("Authentication Error")

# Step 2: Reusable Functions
def fetch_data_from_powerbi(access_token, endpoint, top_param=False):
    url = f"https://api.powerbi.com/v1.0/myorg/admin/{endpoint}"
    if top_param:
        url += "?$top=5000"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()['value']
    else:
        print(f"API call failed for {endpoint} with status: {response.status_code}")
        return None

def clean_and_add_columns(df):
    df.columns = df.columns.str.replace(r'[^\w]', '_', regex=True)
    df['ETLImportDate'] = datetime.today().strftime('%Y-%m-%d')
    return df

def upload_to_blob(file_name, container_name, storage_connection_string):
    blob_service_client = BlobServiceClient.from_connection_string(storage_connection_string)
    container_client = blob_service_client.get_container_client(container_name)
    with open(file_name, "rb") as data:
        container_client.upload_blob(name=file_name, data=data, overwrite=True)
    print(f"File '{file_name}' uploaded to Azure Blob Storage")

def filter_active_workspaces(groups_df, datasets_df):
    active_workspaces = groups_df[(groups_df['state'] == 'Active') & (groups_df['type'] == 'Workspace')]
    return datasets_df[datasets_df['workspaceId'].isin(active_workspaces['id'])]

def fetch_and_save_datasources(dataset_ids, access_token, container_name, storage_connection_string):
    datasources_all = []
    for dataset_id in dataset_ids:
        url = f"https://api.powerbi.com/v1.0/myorg/admin/datasets/{dataset_id}/datasources"
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            datasources = response.json()['value']
            for datasource in datasources:
                datasource['dataset_id'] = dataset_id
                datasources_all.append(datasource)
        else:
            print(f"Failed to fetch datasources for dataset {dataset_id}")
    
    df_datasources = pd.DataFrame(datasources_all)
    df_datasources = clean_and_add_columns(df_datasources)
    df_datasources.to_parquet('GetDatasources.parquet', index=False)
    upload_to_blob('GetDatasources.parquet', container_name, storage_connection_string)

# Step 3: Main Workflow
def main():
    tenant_id = 'tenant_id'         
    client_id = 'client_id'         
    client_secret = 'client_secret' 
    scope = ['https://analysis.windows.net/powerbi/api/.default']
    storage_connection_string = 'storage_connection_string'
    container_name = 'powerbi-restapi/admin/'

    access_token = get_access_token(tenant_id, client_id, client_secret, scope)

    endpoints = {
        "reports": {"top_param": False},
        "datasets": {"top_param": False},
        "groups": {"top_param": True},
        "dataflows": {"top_param": False},
        "dashboards": {"top_param": False}
    }

    groups_df = pd.DataFrame()
    datasets_df = pd.DataFrame()

    for endpoint, params in endpoints.items():
        data = fetch_data_from_powerbi(access_token, endpoint, top_param=params['top_param'])
        if data:
            df = pd.json_normalize(data)
            df = clean_and_add_columns(df)
            output_file = f'Get{endpoint.capitalize()}.parquet'
            df.to_parquet(output_file, index=False)
            upload_to_blob(output_file, container_name, storage_connection_string)

            if endpoint == "groups":
                groups_df = df
            elif endpoint == "datasets":
                datasets_df = df

    active_datasets = filter_active_workspaces(groups_df, datasets_df)
    fetch_and_save_datasources(active_datasets['id'].tolist(), access_token, container_name, storage_connection_string)

if __name__ == "__main__":
    main()
