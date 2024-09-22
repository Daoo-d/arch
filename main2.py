from playwright.sync_api import sync_playwright
import requests
import pandas as pd
import pypyodbc as odbc
import pyodbc
import asyncio
from sqlalchemy import create_engine

username=r'DESKTOP-EUE71HP\hp'
password='198924Aa.'
# hostname = "DESKTOP-EUE71HP\SQLEXPRESS"
hostname = r'DESKTOP-EUE71HP\SQLEXPRESS'
dbname = 'shop'
DRIVER_NAME = 'MS Access Database'
SERVER_NAME = r'DESKTOP-EUE71HP\SQLEXPRESS'
dsn_name="my_db"
dsn_name2 = "mydb"

def connect_database(dsn_name):
    try:
        engine = create_engine(f"mssql+pyodbc://{dsn_name}")
        return engine
    except Exception as e:
        print("Failed to connect to database")
        print("Error:", e)
        engine = None
        return engine
    
def get_projects(dsn_name,database_name):
    conn = connect_database(dsn_name)
    query = f"""
    SELECT project_id,folder_id,name,project_format_version,
    deployed_by_name,last_deployed_time,created_time,
    object_version_lsn 
    FROM {database_name}.catalog.projects
    """
    try:
        with conn.connect() as connection:
            metadata_df = pd.read_sql(query, connection)
            # metadata_df.to_excel(f'{database_name}.xlsx',index=False)
            connection.close()
            conn.dispose()
            return metadata_df
    except Exception as e:
        print("Failed to fetch metadata")
        print("Error:", e)
        return None

def get_packages(dsn_name,database_name):
    conn = connect_database(dsn_name)
    query = f"""
    SELECT 
        *
    FROM {database_name}.catalog.packages
    """
    try:
        with conn.connect() as connection:
            metadata_df = pd.read_sql(query, connection)
            # metadata_df.to_excel(f'{database_name}.xlsx',index=False)
            connection.close()
            conn.dispose()
            return metadata_df
    except Exception as e:
        print("Failed to fetch metadata")
        print("Error:", e)
        return None

def get_elements(dsn_name,query):
    conn = connect_database(dsn_name)
    try:
        with conn.connect() as connection:
            metadata_df = pd.read_sql(query, connection)
            # metadata_df.to_excel(f'{database_name}.xlsx',index=False)
            connection.close()
            conn.dispose()
            return metadata_df
    except Exception as e:
        print("Failed to fetch metadata")
        print("Error:", e)
        return None    

def main():
    query = f"""
    SELECT parameter_id,parameter_name,project_id,CAST(design_default_value AS VARCHAR) AS design_default_value,
    object_name,data_type,description,value_type,value_set,referenced_variable_name,validation_status,last_validation_time
    
    FROM SSISDB.catalog.object_parameters
    """
    query1 = f"""
    SELECT * FROM SSISDB.catalog.environments
    """
    df = get_projects(dsn_name,"SSISDB")
    df1 = get_packages(dsn_name,"SSISDB")
    df2 = get_elements(dsn_name,query1)
    df3 = get_elements(dsn_name,query)
    print(df2)
    if df2.empty:
        combined = df3.merge(df,on="project_id").merge(df1,on="project_id")
    else:
        combined = df3.merge(df,on="project_id").merge(df1,on="project_id").merge(df2,on="folder_id")
    for column in combined.select_dtypes(include=['datetime64[ns, UTC]']).columns:
        combined[column] = combined[column].dt.tz_localize(None)
    combined.to_excel("SSISDB.xlsx",index=False)
    # print(combined)

if __name__=="__main__":
    main()



# def get_database_columns(dsn_name,database_name):
#     conn = connect_database(dsn_name)
#     query = f"""
#     SELECT 
#         *
#     FROM {database_name}.INFORMATION_SCHEMA.COLUMNS
#     """
#     try:
#         with conn.connect() as connection:
#             metadata_df = pd.read_sql(query, connection)
#             # metadata_df.to_excel(f'{database_name}.xlsx',index=False)
#             connection.close()
#             conn.dispose()
#             return metadata_df
#     except Exception as e:
#         print("Failed to fetch metadata")
#         print("Error:", e)
#         return None
# def get_tables(dsn_name,database_name):
#     conn = connect_database(dsn_name)
#     query = f"""
#         SELECT * FROM {database_name}.INFORMATION_SCHEMA.TABLES
#         """
#     try:
#         with conn.connect() as connection:
#             metadata_df = pd.read_sql(query, connection)
#             # metadata_df.to_excel(f'{database_name}.xlsx',index=False)
#             return metadata_df
#     except Exception as e:
#         print("Failed to fetch metadata")
#         print("Error:", e)
#         return None
# def get_columns_usage(dsn_name,database_name):
#     conn = connect_database(dsn_name)
#     query = f"""
#         SELECT * FROM {database_name}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE
#         """
#     try:
#         with conn.connect() as connection:
#             metadata_df = pd.read_sql(query, connection)
#             # metadata_df.to_excel(f'{database_name}.xlsx',index=False)
#             connection.close()
#             return metadata_df
#     except Exception as e:
#         print("Failed to fetch metadata")
#         print("Error:", e)
#         return None

# def get_database_relations(dsn_name,database_name):
#     conn = connect_database(dsn_name)
#     query = f"""
#         SELECT 
#             fk.name AS foreign_key_name,
#             tp.name AS parent_table,
#             cp.name AS parent_column,
#             tr.name AS referenced_table,
#             cr.name AS referenced_column
#         FROM {database_name}.sys.foreign_keys AS fk
#         INNER JOIN {database_name}.sys.foreign_key_columns AS fkc ON fk.object_id = fkc.constraint_object_id
#         INNER JOIN {database_name}.sys.tables AS tp ON fkc.parent_object_id = tp.object_id
#         INNER JOIN {database_name}.sys.columns AS cp ON fkc.parent_object_id = cp.object_id AND fkc.parent_column_id = cp.column_id
#         INNER JOIN {database_name}.sys.tables AS tr ON fkc.referenced_object_id = tr.object_id
#         INNER JOIN {database_name}.sys.columns AS cr ON fkc.referenced_object_id = cr.object_id AND fkc.referenced_column_id = cr.column_id 
#     """
#     try:
#         with conn.connect() as connection:
#             relationships_df = pd.read_sql(query, connection)
#             relationships_df.to_excel(f'{database_name}_relationships.xlsx', index=False)
#             return relationships_df
#     except Exception as e:
#         print("Failed to fetch relationships")
#         print("Error:", e)
#         return None

# def get_databases(dsn_name):
#     conn = connect_database(dsn_name)
#     query = f"""
#     SELECT 
#         *
#     FROM sys.databases
#     WHERE name NOT IN ('master', 'tempdb', 'model', 'msdb')
#     """
#     try:
#         with conn.connect() as connection:
#             metadata_df = pd.read_sql(query, connection)
#             # metadata_df.to_excel('ssmetadata.xlsx',index=False)
#             return metadata_df
#     except Exception as e:
#         print("Failed to fetch metadata")
#         print("Error:", e)
#         return None

# databases = get_databases(dsn_name)
# for name in databases['name']:
#     # get_database_columns(dsn_name,name)    
#     metadata_df =get_database_columns(dsn_name,name)      
#     print(metadata_df)
#     if metadata_df is not None:
#         engine = connect_database(dsn_name2)
#         metadata_df.to_sql("pges",con=engine,if_exists='replace',index=False)
#     else:
#         print("Failed to fetch data for the table.") 

# try:
#     # Switch to the specified database
#     cursor = conn.cursor()
#     # cursor.execute(f"USE {database_name}")
    
#     # Fetch metadata
#     query = "SELECT * FROM sys.databases"
#     cursor.execute(query)        
#     # Fetch all results
#     rows = cursor.fetchall()
  
#     print(f"Metadata for database '{database_name}':")
#     df = pd.DataFrame(rows)
#     print(rows)
# except Exception as e:
#     print(f"Failed to fetch metadata from database '{database_name}'")
#     print("Error:", e)

    # databases.to_excel('sqlttt.xlsx',index=False)

# def connect_database(DRIVER_NAME, SERVER_NAME, dbname):
#     try:
#         connectionString = f"""
#             DRIVER = {{{DRIVER_NAME}}};
#             SERVER = {SERVER_NAME};
#             DATABASE={dbname};
#             Trust_Connection=yes;
#             uid=sa;
#             pwd=198924;
#             """
#         engine = odbc.connect(connectionString)
#         print(engine)
#         print("Database connected successfully")
#         return engine
#     except Exception as e:
#         print("Failed to connect to database")
#         print("Error:", e)
#         engine = None
#         return engine

# directory = r'D:\arch\samples'
#     file_paths = glob.glob(os.path.join(directory, '**', '*.dtsx'), recursive=True)
#     unique_files = set()

# # List to store unique file paths
#     unique_file_paths = []

#     for filep in file_paths:
#         file_name = os.path.basename(filep)
#         if file_name not in unique_files:
#             unique_files.add(file_name)
#             unique_file_paths.append(filep)
    


# engine = connect_database('mydb')
# data = pd.read_parquet('output.parquet')
# df = pd.DataFrame(data)
# df.to_sql('table_name',con=engine,if_exists='replace',index=False)

# async def get_access_token():
#     start = time.time()
#     async with async_playwright() as p:
#         browser = await p.chromium.launch()
#         page = await browser.new_page()
#         await page.goto('https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/get-dataset?tryIt=true#code-try-0')
#         await page.click('a[id="try-it-sign-in"]')
        
#         await page.fill('input[id="i0116"]', 'dawaood.ahmed@bitsglobalconsulting.com')
#         await page.click('input[id="idSIButton9"]')
        
#         await page.click('div[id="aadTile"]')
        
#         await page.fill('input[id="i0118"]', '198924Aa.')
        
#         await page.click('input[id="idSIButton9"]')
        
#         await page.click('input[id="KmsiCheckboxField"]')
        
#         await page.click('input[id="idSIButton9"]')
        
#         await page.goto("https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/get-dataset")
#         await asyncio.sleep(2)
#         await page.click('button[data-bi-name="code-header-try-it-http"]')
        
#         await page.click('button[id="continue-with-account"]')
        
#         pre_element = await page.query_selector('pre[name="http-request"]')
#         span_element = await pre_element.query_selector('span')
#         span_text = await span_element.inner_text()
#         token = span_text.split('Bearer ')[1].strip()
#         print(token)
        
#         await browser.close()
#         end = time.time()
#         t = end - start
#         print(t)
# asyncio.run(get_access_token())


# with sync_playwright() as p:
#     start = time.time()
#     browser = p.chromium.launch(headless = False)
#     page = browser.new_page()
#     page.goto('https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/get-dataset?tryIt=true#code-try-0')
#     page.click('a[id="try-it-sign-in"]')
    
#     page.fill('input[id="i0116"]','dawaood.ahmed@bitsglobalconsulting.com')
#     page.click('input[id="idSIButton9"]')
    
#     page.click('div[id="aadTile"]')
    
#     page.fill('input[id="i0118"]','198924Aa.')
    
#     page.click('input[id="idSIButton9"]')
    
#     page.click('input[id="KmsiCheckboxField"]')
    
#     page.click('input[id="idSIButton9"]')
    
#     page.goto("https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/get-dataset")
#     time.sleep(2)
#     page.click('button[data-bi-name="code-header-try-it-http"]')
    
#     page.click('button[id="continue-with-account"]')
    

#     pre_element = page.query_selector('pre[name="http-request"]')
#     span_text = pre_element.query_selector('span').inner_text()
#     token = span_text.split('Bearer ')[1].strip()
    
#     browser.close()


# def acquire_bearer_token(authority, client_id, scope,redirect_uri):
#     app = msal.PublicClientApplication(client_id, authority=authority)

# # Initiate the interactive authentication flow
#     result = app.acquire_token_interactive(scopes=scope)

#     if 'access_token' in result:
#         print("Access token:", result['access_token'])
#     else:
#         print("Error acquiring token:", result.get('error_description'))

# def acquire_bearer_token(client,tenant,client_secret,api):
#     auth = ClientSecretCredential(authority = 'https://login.microsoftonline.com/',
#                                                         tenant_id = tenant,
#                                                         client_id = client,
#                                                         client_secret = client_secret)
#     access_token = auth.get_token(api)
#     access_token = access_token.token
#     return access_token
