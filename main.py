import pandas as pd
import requests
import json
from sqlalchemy import create_engine
import time
from playwright.sync_api import sync_playwright
import os


# Database credentials
hostname = "DESKTOP-EUE71HP"
usernamedb = "sa"
passworddb = "198924"
dbname ="shop"
dsn_name="mydb"

# API configuration
username = 'dawaood.ahmed@bitsglobalconsulting.com'
password = '198924Aa.'

dataset_id = 'b1e77ad7-adf3-484e-bc7b-839d5d4e1662'
report_id = 'c0cbdb2f-ab0c-497c-a188-9d18784c750a'
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
        
        page.goto("https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/get-dataset")
        time.sleep(2)
        page.click('button[data-bi-name="code-header-try-it-http"]')
        
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
def get_tables_from_dataset(access_token, dataset_id):
    query = {
        "queries": [
            {
                "query": "EVALUATE INFO.TABLES()"
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

def getdict_expr(bearer_token,report_id,dataset_id):
    report = get_report(bearer_token,report_id)
    pages = report_pages(bearer_token,report_id)
    dataset = get_dataset(bearer_token,dataset_id)
    data = dataset_tables(bearer_token,dataset_id)
    
    # print(len(table.columns))
    print(report['id'])
    # print(len(pages))
    column_count = 0
    for item in data:
        table = fetch_data_from_powerbi(bearer_token,dataset_id,item['name'])
        column_count += len(table.columns)
    # print(column_count)    
    datadf={
        "Dataset Name":dataset['name'],
        "Dataset ID":report['datasetId'],
        "Table Count":len(data),
        "Column Count":column_count,
        "Report Name":report['name'],
        "Report ID":report['id'],
        "Report Type":report['reportType'],
        "Page Count":len(pages)
    }
    data = pd.DataFrame([datadf])
    data.to_excel('output.xlsx', index=False)
    return data

def main():
    bearer_token = read_access_token(username,password)
    # report = get_report(bearer_token,report_id)

    # data2 = get_tables_from_dataset(bearer_token,dataset_id)
    # db = data2['results'][0]['tables'][0]['rows']
    
    # df = pd.DataFrame(db) 
    # df.to_excel('measures.xlsx', index=False)
    # print(df)  



    data = get_tables_from_dataset(bearer_token,dataset_id)
    rows = data['results'][0]['tables'][0]['rows']

# Loop through rows and print names without brackets
    for row in rows:
        name = row['[Name]']
        print(name)
        data2 = fetch_data_from_powerbi(bearer_token,dataset_id,name)
        if data2 is not None:
            engine = connect_database(dsn_name)
            data2.to_sql(name,con=engine,if_exists='replace',index=False)
        else:
            print("Failed to fetch data for the table.") 



    # data = getdict_expr(bearer_token,report_id,dataset_id)
    # print(data)

    # if df is not None:
    #     engine = connect_database(dsn_name)
    #     df.to_sql("metadata",con=engine,if_exists='replace',index=False)
    # else:
    #     print("connection failed")    


    # GET Dataset
    # data = get_dataset(bearer_token,dataset_id)

    # Get Report
    
    # print(dataset['name'])

    # GET Report Pages
    # data = report_pages(bearer_token,report_id)
    # print(data)

    # Get table from the dataset 
    # data = fetch_data_from_powerbi(access_token,dataset_id,table_name)

    # Storing in the database
    # if data is not None:
    #     engine = connect_database(hostname,username,password,dbname)
    #     data.to_sql(table_name,con=engine,if_exists='replace',index=False)
    # else:
    #     print("connection failed")    
    # push_data_to_powerbi(access_token)

    # CREATE DATASET
    # data = pd.read_parquet('output.parquet')
    # df = pd.DataFrame(data)
    # dictl_datatypes = listdatatype(df)
    # add_table_to_dataset(bearer_token,dataset_id,"cleaned_2",dictl_datatypes)

    # PUSH DATA IN A DATASET 
    # dictl = df.head().to_dict(orient='records')
    # print(df)
    # push_data_to_powerbi(access_token,dataset_id,"cleaned",dictl)    
    
    # DAX QUERY
    # "EVALUATE INFO.RELATIONSHIPS()"
    # "EVALUATE INFO.MEASURES()"
    #"EVALUATE INFO.COLUMNS()"
    # "EVALUATE INFO.TABLES()"
    
if __name__=="__main__":
    main()    


    # ssis
    # def get_connections(file_path):
#     with open(file_path, 'r', encoding='utf-8') as file:
#         content = file.read()
#         soup = BeautifulSoup(content, 'lxml-xml')
#         connection_managers = soup.find_all('DTS:ConnectionManager')

#     # Extract data from each 'ConnectionManager' element
#         data = []
#         for cm in connection_managers:
#             cm_data = {attr: cm.get(attr) for attr in cm.attrs}
#             object_data = cm.find('DTS:ConnectionManager')
#             if object_data:
#                 cm_data.update({attr: object_data.get(attr) for attr in object_data.attrs})
#             data.append(cm_data)
#         df = pd.DataFrame(data)
#         df.dropna(how='all', inplace=True)
#         df.to_excel("df.xlsx",index=False)
#         print(df)    

# def  get_components(file_path):
#     with open(file_path, 'r', encoding='utf-8') as file:
#         content = file.read()   
#         soup = BeautifulSoup(content, 'lxml-xml')
#         components = soup.find_all('component')

#         # Extract data from each 'ConnectionManager' element
#         data = []
#         for component in components:
#             component_data = {attr: component.get(attr) for attr in component.attrs}
#             properties = component.find_all('property')
#             for prop in properties:
#                 component_data[prop.get('name')] = prop.text
#             data.append(component_data)

# # Convert to DataFrame
#         df = pd.DataFrame(data)
#         df.to_excel("df1.xlsx",index=False)

# # Display the DataFrame
#         print(df)

# def extract_attributes(element):
#     """Recursively extract attributes from an element and its children."""
#     element_data = {attr: element.get(attr) for attr in element.attrs}
#     for child in element.find_all(recursive=False):
#         child_data = extract_attributes(child)
#         for key, value in child_data.items():
#             if key in element_data:
#                 count = 1
#                 new_key = f"{key}_{count}"
#                 while new_key in element_data:
#                     count += 1
#                     new_key = f"{key}_{count}"
#                 element_data[new_key] = value
#             else:
#                 element_data[key] = value
#     return element_data

# def get_connect(file_path):
#     with open(file_path, 'r', encoding='utf-8') as file:
#         content = file.read()
#         soup = BeautifulSoup(content, 'lxml-xml')
#         connection_managers = soup.find_all('DTS:Executable')

#         data = []
#         for cm in connection_managers:
#             cm_data = extract_attributes(cm)
#             data.append(cm_data)
    
#     # Create a DataFrame for the main elements
#     df_main = pd.DataFrame(data)
#     nested_data = []
#     for cm in connection_managers:
#         for child in cm.find_all(recursive=False):
#             child_data = extract_attributes(child)
#             child_data['parent_id'] = cm.get('DTS:ID')  # Assuming 'DTS:ID' is a unique identifier for the parent
#             nested_data.append(child_data)
    
#     df_nested = pd.DataFrame(nested_data)
    
#     # Save DataFrames to Excel
#     with pd.ExcelWriter("dff2244.xlsx") as writer:
#         df_main.to_excel(writer, sheet_name='Main', index=False)
#         df_nested.to_excel(writer, sheet_name='Nested', index=False)
#     print(df_main)
#     print(df_nested) 