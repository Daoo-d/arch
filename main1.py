import pandas as pd
import requests
import json
from sqlalchemy import create_engine
import time
from playwright.sync_api import sync_playwright
import os,logging


# Database credentials
hostname = "localhost"
usernamedb = "root"
passworddb = "198924Aa."
dbname ="metadata"
dsn_name="mydb"

# API configuration
username = 'dawaood.ahmed@bitsglobalconsulting.com'
password = '198924Aa.'

dataset_id = 'b1e77ad7-adf3-484e-bc7b-839d5d4e1662'
report_id = 'c0cbdb2f-ab0c-497c-a188-9d18784c750a'
group_id = '7c5d1f1b-134c-4a74-a092-1116fb22b5fe'
table_name = 'Customer'

TOKEN_FILE = 'access_token.json'
# DESKTOP-EUE71HP\SQLEXPRESS

def connect_database(dsn_name):
    try:
        engine = create_engine(f"mssql+pyodbc://{dsn_name}")
        return engine
    except Exception as e:
        print("Failed to connect to database")
        print("Error:", e)
        engine = None
        return engine



def acquire_bearer_token(username,password):
    print("GETTING ACCESS TOKEN")
    with sync_playwright() as p:
        
        browser = p.chromium.launch(headless = False)
        page = browser.new_page()
        page.goto('https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/get-dataset?tryIt=true#code-try-0')
        page.click('a[id="try-it-sign-in"]')
        
        page.fill('input[id="i0116"]',username)
        page.click('input[id="idSIButton9"]')
        
        page.click('div[id="aadTile"]')
        
        page.fill('input[id="i0118"]',password)
        
        page.click('input[id="idSIButton9"]')
        
        page.click('input[id="KmsiCheckboxField"]')
        
        page.click('input[id="idSIButton9"]')
        
        # page.goto("https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/get-dataset")
        # time.sleep(2)
        # page.click('button[data-bi-name="code-header-try-it-http"]')
        
        page.click('button[id="continue-with-account"]')
        
        pre_element = page.query_selector('pre[name="http-request"]')
        span_text = pre_element.query_selector('span').inner_text()
        token = span_text.split('Bearer ')[1].strip()
        
        browser.close()
        token_data = {
            'access_token': token,
            'expires_at': time.time() + 3600  # Assuming the token expires in 1 hour
        }
    with open(TOKEN_FILE, 'w') as f:      
        json.dump(token_data, f)
        print("Access token stored")
    return token

def read_access_token(username,password):
    try:
        with open(TOKEN_FILE, 'r') as f:
            token_data = json.load(f)
            if time.time() < token_data['expires_at']:
                return token_data['access_token']
            else:
                print("Access token expired. Fetching a new token...")
                return acquire_bearer_token(username, password)
    except FileNotFoundError:
        print("Token file not found. Fetching a new token...")
        return acquire_bearer_token(username, password) 

def get_reports_in_groups(access_token,group_id):
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{group_id}/reports"
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        return response.json()['value']
    else:
        print(f"Failed to get REPORT. Status code: {response.status_code}")
        return response.json()

def create_dataset(access_token, dataset_name, table_name, column_list):
    data = {
    "name": dataset_name,
    "defaultMode": "Push",
    "tables": [
        {
            "name": table_name,
            "columns": column_list
            }
        ]
    }
    url = "https://api.powerbi.com/v1.0/myorg/datasets"
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }
    
    response = requests.post(url, headers=headers, data=json.dumps(data))
    
    if response.status_code == 200:
        print("Push API dataset created successfully!")
    else:
        print(f"Error {response.status_code}: {response.text}")

def add_table_to_dataset(access_token, dataset_id, table_name, column_list):
    data = {
        "name": table_name,
        "columns": column_list
    }
    url = f"https://api.powerbi.com/v1.0/myorg/datasets/{dataset_id}/tables"
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }
    
    response = requests.post(url, headers=headers, data=json.dumps(data))
    
    if response.status_code == 201:
        print("Table added successfully!")
    else:
        print(f"Error {response.status_code}: {response.text}")


def push_data_to_powerbi(access_token,dataset_id, table_name, values):
    
    data_to_push = {
    "rows": values
    }
    
    url = f"https://api.powerbi.com/v1.0/myorg/datasets/{dataset_id}/tables/{table_name}/rows"
    headers = {
        'Authorization': f'Bearer {access_token}'
    }
    response = requests.post(url, headers=headers, data=json.dumps(data_to_push))
    
    if response.status_code == 200:
        print("Push API dataset created successfully!")
    else:
        print(f"Error {response.status_code}: {response.text}")

def get_report(access_token,report_id):
    url = f"https://api.powerbi.com/v1.0/myorg/reports/{report_id}"
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        json_data = json.loads(response.text)
        return json_data
    else:
        print(f"Failed to get REPORT. Status code: {response.status_code}")
        return response.json()

def report_pages(access_token,report_id):
    url = f"https://api.powerbi.com/v1.0/myorg/reports/{report_id}/pages"
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        pages = response.json()['value']
        # print(f"Number of pages: {len(pages)}")
        return pages
    else:
        print(f"Failed to get tables. Status code: {response.status_code}")
        return response.json()

def get_dataset(access_token, dataset_id):
    url = f"https://api.powerbi.com/v1.0/myorg/datasets/{dataset_id}"
    headers = {
        'Authorization': f'Bearer {access_token}',
         'Content-Type': 'application/json'
    }
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        tables = response.json()
        # print(f"Number of tables: {len(tables)}")
        # for table in tables:
        #     print(f"Table Name: {table['name']}")
        #     print()
        return tables
    else:
        print(f"Failed to get tables. Status code: {response.status_code}")
        return response.json()

def dataset_tables(access_token,dataset_id):
    url = f"https://api.powerbi.com/v1.0/myorg/datasets/{dataset_id}/tables"
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        tables = response.json()['value']
        
        return tables
    else:
        print(f"Failed to get tables. Status code: {response.status_code}")
        return response.json()
    
def get_datasources(access_token,dataset_id):
    url = f"https://api.powerbi.com/v1.0/myorg/datasets/{dataset_id}/datasources"
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        tables = response.json()['value']
        
        return tables
    else:
        print(f"Failed to get tables. Status code: {response.status_code}")
        return response.json()

def get_datasources_in_groups(access_token,group_id):
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{group_id}/datasources"
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        return response.json()['value']
    else:
        print(f"Failed to get REPORT. Status code: {response.status_code}")
        return response.json()

def fetch_data_from_powerbi(access_token,dataset_id,table_name):

    url = f"https://api.powerbi.com/v1.0/myorg/datasets/{dataset_id}/executeQueries"
    headers = {
        'Authorization': f'Bearer {access_token}',
         'Content-Type': 'application/json'
    }
    escaped_table_name = f"'{table_name}'"
    dax_query = {
    "queries": [
        {
            "query": f"EVALUATE {escaped_table_name}"
        }
    ]
    }
    response = requests.post(url, headers=headers, data=json.dumps(dax_query))

    if response.status_code == 200:
        query_results = response.json()
        rows = query_results["results"][0]["tables"][0]["rows"]
        df = pd.DataFrame(rows)
        
        # Remove the table prefix
        df.columns = [col.split('[')[-1].rstrip(']') for col in df.columns]
        return df
    else:
        print(f"Error {response.status_code}: {response.text}")
        return None

def listdatatype(df):
    column_info = []
    for column in df.columns:
        col_data_type = str(df[column].dtype)
        # Map pandas dtypes to Power BI data types
        if col_data_type.startswith('int'):
            data_type = "Int64"
        elif col_data_type.startswith('float'):
            data_type = "Double"
        elif col_data_type == 'bool':
            data_type = "bool"
        elif col_data_type == 'datetime64[ns]':
            data_type = "DateTime"
        else:
            data_type = "string"
        
        column_info.append({"name": column, "dataType": data_type})    
    return column_info    
def get_tables_from_dataset(access_token, dataset_id,query):
    query = {
        "queries": [
            {
                "query": query
            }
        ]
    }
    url = f"https://api.powerbi.com/v1.0/myorg/datasets/{dataset_id}/executeQueries"
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }
    
    response = requests.post(url, headers=headers, data=json.dumps(query))
    
    if response.status_code == 200:
        tables = response.json()
        return tables
    else:
        print(f"Error {response.status_code}: {response.text}")
        return None

def get_metadata(access_token,dataset_id):
    query_list = {
    "tables": "EVALUATE INFO.TABLES()",
    "columns": "EVALUATE INFO.COLUMNS()",
    "relationship": "EVALUATE INFO.RELATIONSHIPS()",
    "measure": "EVALUATE INFO.MEASURES()"
    }
    data = get_dataset(access_token,dataset_id)
    dataset_name = data['name']
    dataframes = []
    # with pd.ExcelWriter(f'{dataset_name}.xlsx', engine='openpyxl') as writer:
    i=0
    for tname, query in query_list.items():
        data2 = get_tables_from_dataset(access_token,dataset_id,query)
        db = data2['results'][0]['tables'][0]['rows']
        # table_name = generate_table_name(access_token,dataset_id,tname)
        df = pd.DataFrame(db) 
        if df is not None and not df.empty:
            df['Source'] = f'{dataset_name}_{i}'
            dataframes.append(df)
            
            # df.to_excel(writer, sheet_name=f"{tname}", index=False)
            i+=1
        else:
            print(f"Failed to fetch data for {f"{tname}"} or dataframe is empty")
        # if df is not None:
            # engine = connect_database(dsn_name)
            # df.to_sql(table_name,con=engine,if_exists='replace',index=False)
        # else:
        #     print("Failed to fetch data for the table.")  
    combined_df = pd.concat(dataframes, ignore_index=True)   
    return combined_df    
    
def get_groups(access_token):
    url = f"https://api.powerbi.com/v1.0/myorg/groups"
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        return response.json()['value']
    else:
        print(f"Failed to get REPORT. Status code: {response.status_code}")
        return response.json()
logging.basicConfig(level=logging.INFO)
def getdict_expr(bearer_token,report_id,dataset_id):
    try:
        report = get_report(bearer_token, report_id)
        pages = report_pages(bearer_token, report_id)
        dataset = get_dataset(bearer_token, dataset_id)
        datasource = get_datasources(bearer_token, dataset_id)
        dataS = pd.json_normalize(datasource)
        dataS['Dataset ID'] = report['datasetId']

        table_queries = [
            ("EVALUATE INFO.TABLES()", "table_data"),
            ("EVALUATE INFO.COLUMNS()", "column_data"),
            ("EVALUATE INFO.RELATIONSHIPS()", "relation_data")
        ]
        data_frames = {}
        for query, name in table_queries:
            data = get_tables_from_dataset(bearer_token, dataset_id, query)
            rows = data['results'][0]['tables'][0]['rows']
            data_frames[name] = pd.DataFrame(rows)
            data_frames[name]['Dataset ID'] = report['datasetId']

        datadf = {
            "Dataset Name": dataset['name'],
            "Dataset ID": report['datasetId'],
            "Table Count": len(data_frames['table_data']),
            "Report Name": report['name'],
            "Report ID": report['id'],
            "Report Type": report['reportType'],
            "Page Count": len(pages)
            }
        combined_data = pd.DataFrame([datadf]).merge(dataS, on='Dataset ID')

        return combined_data, data_frames['table_data'], data_frames['column_data'], data_frames['relation_data']
    except Exception as e:
        logging.error(f"Error in getdict_expr: {e}")
        return None, None, None, None



def generate_table_name(access_token,dataset_id, table_name):
    data = get_dataset(access_token,dataset_id)
    dataset_name = data['name']

    # Split the dataset name into words
    words = dataset_name.split()
    
    # Take the first letter of each word to form the abbreviation
    abbreviation = ''.join(word[0].upper() for word in words)
    
    # Combine the abbreviation with the table name
    full_table_name = f"{abbreviation}_{table_name}"
    
    return full_table_name

def fetch_all_tables(access_token,dataset_id):
    table_query="EVALUATE INFO.TABLES()"
    data = get_tables_from_dataset(access_token,dataset_id,table_query)
    rows = data['results'][0]['tables'][0]['rows']
    # Loop through rows and print names without brackets
    for row in rows:
        name = row['[Name]']
        tname = generate_table_name(access_token,dataset_id,name)
        print(tname)
        data2 = fetch_data_from_powerbi(access_token,dataset_id,name)
        if data2 is not None:
            engine = connect_database(dsn_name)
            data2.to_sql(tname,con=engine,if_exists='replace',index=False)
        else:
            print("Failed to fetch data for the table.") 


# def main():
#     bearer_token = read_access_token(username,password)
#     table_query="EVALUATE INFO.TABLES()" 
#     data = get_tables_from_dataset(bearer_token,"b6e02f95-5cd6-42c5-85d1-cf216678dc5b",table_query)
#     rows = data['results'][0]['tables'][0]['rows']
#     table_data = pd.DataFrame(rows)
#     print(data)
#     print(table_data)
# #     # data = get_tables_from_dataset(bearer_token,dataset_id,"EVALUATE INFO.TABLES()")
#     groups = get_groups(bearer_token)
#     dataframes = []
#     dfs = []
#     dfs2 = []
#     dfs3 = []

# #     # with pd.ExcelWriter('Workspaces.xlsx', engine='openpyxl') as writer:
# #     df1 = pd.DataFrame(groups)
# #     if df1 is not None and not df1.empty:
# #         dataframes.append(df1)
# #         # df1.to_excel(writer, sheet_name="workspace_record", index=False)
# #     else:
# #         print(f"Failed to fetch data for {"workspace_record"} or dataframe is empty") 
# #     i=1
#     for group in groups:
#         tname = group['name']
#         print(f"getting :{tname}")
#         data = get_reports_in_groups(bearer_token,group['id'])
#         # df = pd.DataFrame(data)
# #         if df is not None and not df.empty:
# #             df['Source'] = f'workspace_{i}'
# #             dataframes.append(df)
# #             # df.to_excel(writer, sheet_name=f"{tname}", index=False)
# #             i+=1
# #         else:
# #             print(f"Failed to fetch data for {f"{tname}"} or dataframe is empty") 

#         for d in data:
#             reportid = d['id']
#             datasetid = d['datasetId']
#             dataframe1,dataframe2,dataframe3,dataframe4 = getdict_expr(bearer_token,reportid,datasetid)
#             dfs.append(dataframe2)
#             dataframes.append(dataframe1)
#             dfs2.append(dataframe3) 
#             dfs3.append(dataframe4)   
#         combined_df = pd.concat(dfs, ignore_index=True)
#         combined_df1 = pd.concat(dfs2,ignore_index=True)
#         combined_df3 = pd.concat(dfs3,ignore_index=True)

#         da = combined_df.merge(combined_df1,on="Dataset ID").merge(combined_df3,on="Dataset ID")
#         da.to_excel('merge.xlsx',index=False)
#         combined_df2 = pd.concat(dataframes,ignore_index=True)
#         dataset = combined_df2.merge(da, on='Dataset ID')
#         dataset.to_excel("output4.xlsx",index=False)
        
#     dataframes = dataframes+dfs        
#     combined_df = pd.concat(dataframes, ignore_index=True)   
#     combined_df.to_excel("powerbi.xlsx",index=False)        


    # table_query="EVALUATE INFO.TABLES()" 
    # data = get_tables_from_dataset(bearer_token,"90320467-3194-4df0-ae3e-a60129418862",table_query)
    # rows = data['results'][0]['tables'][0]['rows'] 
    # dfv = pd.DataFrame(rows)
    # dfv.to_excel('map.xlsx',index=False)           


    # fetch_all_tables(bearer_token,"90320467-3194-4df0-ae3e-a60129418862")

#                     # pages = report_pages(bearer_token,reportid)
#                     # print(len(pages))
#                     # df = pd.DataFrame(pages)
#                     # if df is not None and not df.empty:
#                     #     df.to_excel(writer, sheet_name=f"sheet_name{i}", index=False)
#                     #     i+=1
#                     # else:
#                     #     print(f"Failed to fetch data for {f"sheet_name{i}"} or dataframe is empty")

#                 # print("Data saved to multiple sheets in Excel.")        
#                     # df.to_excel(writer,sheet_name=f"report{i}", index=False)
                

    
#     # if combined_df is not None:
#     #     engine = connect_database(dsn_name)
#     #     combined_df.to_sql("paeges",con=engine,if_exists='replace',index=False)
#     # else:
#     #     print("Failed to fetch data for the table.") 

#     # # Display the combined DataFrame
#     # print(combined_df)        
    
        

    
    
# if __name__=="__main__":
#     main()