from main2 import *
import aiohttp
from playwright.async_api import async_playwright
import pandas as pd
from fastapi import FastAPI, HTTPException,Header
import logging,json,time
from sqlalchemy import text
from pydantic import BaseModel
import tracemalloc
from concurrent.futures import ThreadPoolExecutor

app = FastAPI()

TOKEN_FILE_NEW = 'bearer_token.json'
hostname = "localhost"
usernamedb = "root"
passworddb = "198924Aa."
dbname ="metadata"
dsn_name="mydb"

# API configuration
username = 'dawaood.ahmed@bitsglobalconsulting.com'
password = '198924Aa.'

class Credentials(BaseModel):
    username: str
    password: str

async def write_to_excel(df, filename):
    loop = asyncio.get_event_loop()
    with ThreadPoolExecutor() as pool:
        await loop.run_in_executor(pool, df.to_excel, filename, False, 'xlsxwriter')

async def fetch(session, url):
    async with session.get(url) as response:
        return await response.json()

# Asynchronous data retrieval
async def get_data_async(urls):
    async with aiohttp.ClientSession() as session:
        tasks = [fetch(session, url) for url in urls]
        return await asyncio.gather(*tasks)

def connect_mysql_database(hostname,username,password,dbname):
    try:    
        engine = create_engine(f"mysql+pymysql://{username}:{password}@{hostname}/{dbname}")
        print("database connected successfully")
        return engine
    except Exception as e:
        print("Failed to connect to database")
        print("Error:", e)
        engine = None 

async def get_groups_update(access_token):
    url = "https://api.powerbi.com/v1.0/myorg/groups"
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as response:
            if response.status == 200:
                data =await response.json()
                return data['value']
            else:
                print(f"Failed to get REPORT. Status code: {response.status}")
                return await response.json()

async def get_report_update(access_token, report_id):
    url = f"https://api.powerbi.com/v1.0/myorg/reports/{report_id}"
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as response:
            if response.status == 200:
                json_data = await response.json()
                return json_data
            else:
                print(f"Failed to get REPORT. Status code: {response.status}")
                return await response.json()

async def report_pages_update(access_token, report_id):
    url = f"https://api.powerbi.com/v1.0/myorg/reports/{report_id}/pages"
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as response:
            if response.status == 200:
                pages = await response.json()
                return pages['value']
            else:
                print(f"Failed to get pages. Status code: {response.status}")
                return await response.json()

async def get_dataset_update(access_token, dataset_id):
    url = f"https://api.powerbi.com/v1.0/myorg/datasets/{dataset_id}"
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as response:
            if response.status == 200:
                tables = await response.json()
                return tables
            else:
                print(f"Failed to get dataset. Status code: {response.status}")
                return await response.json()

async def get_datasources_update(access_token, dataset_id):
    url = f"https://api.powerbi.com/v1.0/myorg/datasets/{dataset_id}/datasources"
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as response:
            if response.status == 200:
                tables = await response.json()
                return tables['value']
            else:
                print(f"Failed to get datasources. Status code: {response.status}")
                return await response.json()

async def get_tables_from_dataset_update(access_token, dataset_id, query):
    query_payload = {
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
    async with aiohttp.ClientSession() as session:
        async with session.post(url, headers=headers, json=query_payload) as response:
            if response.status == 200:
                tables = await response.json()
                return tables['results'][0]['tables'][0]['rows']
            else:
                print(f"Error {response.status}: {response.text}")
                return None

async def get_reports_in_groups_update(access_token, group_id):
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{group_id}/reports"
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as response:
            if response.status == 200:
                data = await response.json()
                return data['value']
            else:
                print(f"Failed to get REPORT. Status code: {response.status}")
                return await response.json()

async def getdict_expr_update(bearer_token, report_id, dataset_id):
    tracemalloc.start()
    try:
        print("getting reports")
        report = await get_report_update(bearer_token, report_id)
        print("getting reports pages")
        pages = await report_pages_update(bearer_token, report_id)
        print("getting datasets")
        dataset = await get_dataset_update(bearer_token, dataset_id)
        print("getting datasources")
        datasource = await get_datasources_update(bearer_token, dataset_id)
        dataS = pd.json_normalize(datasource)
        dataS['Dataset ID'] = report['datasetId']

        table_queries = [
            ("EVALUATE INFO.TABLES()", "table_data"),
            ("EVALUATE INFO.COLUMNS()", "column_data"),
            ("EVALUATE INFO.RELATIONSHIPS()", "relation_data")
        ]
        data_frames = {}
        for query, name in table_queries:
            print("getting tables info")
            rows = await get_tables_from_dataset_update(bearer_token, dataset_id, query)    
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

        return [combined_data, data_frames['table_data'], data_frames['column_data'], data_frames['relation_data']]
    except Exception as e:
        logging.error(f"Error in getdict_expr: {e}")
        return None, None, None, None

async def aquire_access_tokken_update(username: str, password: str):
    print("GETTING ACCESS TOKEN")
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        page = await browser.new_page()
        await page.goto('https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/get-dataset?tryIt=true#code-try-0')
        await page.click('a[id="try-it-sign-in"]')
        
        await page.fill('input[id="i0116"]', username)
        await page.click('input[id="idSIButton9"]')
        
        await page.click('div[id="aadTile"]')
        
        await page.fill('input[id="i0118"]', password)
        await page.click('input[id="idSIButton9"]')
        
        await page.click('input[id="KmsiCheckboxField"]')
        await page.click('input[id="idSIButton9"]')
        
        await page.click('button[id="continue-with-account"]')
        
        
        pre_element = await page.query_selector('pre[name="http-request"]')
        span_element = await pre_element.query_selector('span')
        span_text = await span_element.inner_text()
        token = span_text.split('Bearer ')[1].strip()
        
        await browser.close()
        token_data = {
            'access_token': token,
            'expires_at': time.time() + 3600  # Assuming the token expires in 1 hour
        }
        
    with open(TOKEN_FILE_NEW, 'w') as f:
        json.dump(token_data, f)
        print("Access token stored")
    return token

async def read_access_token_update(username: str, password: str):
    try:
        with open(TOKEN_FILE_NEW, 'r') as f:
            token_data = json.load(f)
            if time.time() < token_data['expires_at']:
                return token_data['access_token']
            else:
                print("Access token expired. Fetching a new token...")
                return await aquire_access_tokken_update(username, password)
    except FileNotFoundError:
        print("Token file not found. Fetching a new token...")
        return await aquire_access_tokken_update(username, password)


def get_SSIS():
    
    query = f"""
    SELECT parameter_id,parameter_name,project_id,CAST(design_default_value AS VARCHAR) AS design_default_value,CAST(default_value AS VARCHAR)default_value,
    object_type,object_name,data_type AS parameter_data_type,required,sensitive,description,value_type,value_set,referenced_variable_name,validation_status,last_validation_time
    
    FROM SSISDB.catalog.object_parameters
    """
    # query1 = f"""
    # SELECT * FROM SSISDB.catalog.environments
    # """
    df = get_projects(dsn_name,"SSISDB")
    df.drop('deployed_by_sid', axis=1, inplace=True)

    df1 = get_packages(dsn_name,"SSISDB")
    # df1 = get_packages(dsn_name,"SSISDB")
    # df2 = get_elements(dsn_name,query1)
    df2 = get_elements(dsn_name,query)
    # print(df3)
    # if df2.empty:
    #     combined = df.merge(df1,on="project_id")
    # else:
    #     combined = df3.merge(df,on="project_id").merge(df1,on="project_id").merge(df2,on="folder_id")
    # for column in combined.select_dtypes(include=['datetime64[ns, UTC]']).columns:
    #     combined[column] = combined[column].dt.tz_localize(None)
    
    df['ToolID'] = '2'
    # df.to_excel("SSISDB4.xlsx",index=False)

    return df,df1,df2

async def get_powerbi(bearer_token):
    tracemalloc.start()
    try:
        groups = await get_groups_update(bearer_token)
        dataframes, dfs, dfs2, dfs3 = [], [], [], []

        async def process_group(group):
            print("reports in groups")
            data = await get_reports_in_groups_update(bearer_token, group['id'])
            for d in data:
                reportid = d['id']
                datasetid = d['datasetId']
                print("getting dict")
                dictlist = await getdict_expr_update(bearer_token, reportid, datasetid)
                dataframe1, dataframe2, dataframe3, dataframe4 = dictlist
                
                if dataframe1 is not None:
                    dfs.append(dataframe2)
                    dataframes.append(dataframe1)
                    dfs2.append(dataframe3)
                    dfs3.append(dataframe4)

        await asyncio.gather(*[process_group(group) for group in groups])
       
        combined_df = pd.concat(dfs, ignore_index=True)
        combined_df1 = pd.concat(dfs2, ignore_index=True)
        combined_df3 = pd.concat(dfs3, ignore_index=True)
        da = combined_df.merge(combined_df1, on="Dataset ID").merge(combined_df3, on="Dataset ID")
  
        da.head(100).to_excel("merge4.xlsx",index=False)
        combined_df2 = pd.concat(dataframes, ignore_index=True)
        dataset = combined_df2.merge(da, on='Dataset ID')

        dataset['Tool'] = "Power BI"
        
        return dataset
    except Exception as e:
        logging.error(f"Error in get_powerbi: {e}")
        return None

@app.get("/powerbi_data")
async def powerbi_data(username: str = Header(...), password: str = Header(...)):
    try:
        access_token = await read_access_token_update(username, password)
        dataframe = await get_powerbi(access_token)

        if dataframe is not None:
            engine = connect_mysql_database(hostname,usernamedb,passworddb,dbname)
            dataframe.to_sql("powerbi",con=engine,if_exists='replace',index=False)
            
        else:
            print("Failed to fetch data for the table.") 
        return {
            "message": "Data populated and saved successfully",
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.get("/SSIS_data")
async def SSIS_data():
    try:
        project_df,package_df,parameter_df = get_SSIS()
        if project_df is not None:
            engine = connect_mysql_database(hostname,usernamedb,passworddb,"dlineage")
            if engine:
                try:
                    connection = engine.connect()
                    transaction = connection.begin()
                    try:
                        project_df.to_sql("ssis_projects", con=connection, if_exists='replace', index=False)
                        print("parameter inserted successfully")
                    except Exception as e:
                        print(f"Failed to insert data: {e}")
                    try:
                        package_df.to_sql("ssis_packages", con=connection, if_exists='replace', index=False)
                        print("parameter inserted successfully")
                    except Exception as e:
                        print(f"Failed to insert data: {e}")
                    try:
                        parameter_df.to_sql("package_parameters", con=connection, if_exists='replace', index=False)
                        print("parameter inserted successfully")
                    except Exception as e:
                        print(f"Failed to insert data: {e}")        
                    transaction.commit()          
                except Exception as e:
                    transaction.rollback()
                    print("Failed to insert data")
                    print("Error:", e)
                finally:
                    connection.close()    
        # if dataframe is not None:
        #     engine = connect_mysql_database(hostname,usernamedb,passworddb,dbname)
        #     dataframe.to_sql("SSIS_table",con=engine,if_exists='replace',index=False)
            
        # else:
        #     print("Failed to fetch data for the table.") 
        return {
            "message": "Data populated and saved successfully",
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))    

@app.get("/get_combined_data")
async def get_combined_data(username: str = Header(...), password: str = Header(...)):
    try:
        access_token = await read_access_token_update(username, password)
        df2 = await get_powerbi(access_token)
        df1 = get_SSIS()
        combined_df = pd.concat([df2, df1], ignore_index=True)

        # df2.to_excel('result4.xlsx', index=False)
        if combined_df is not None:
            engine = connect_mysql_database(hostname,usernamedb,passworddb,dbname)
            combined_df.to_sql("combined_table",con=engine,if_exists='replace',index=False)
            
        else:
            print("Failed to fetch data for the table.") 
        return {
            "message": "Data combined and saved successfully",
            "row_count": df2.shape[0],
            "column_count": df2.shape[1],
            
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/populate_data")
async def populate_database(username: str = Header(...), password: str = Header(...)):
    try:
        access_token = await read_access_token_update(username, password)
        groups = await get_groups_update(access_token)

        async def process_group(group):
            print("reports in groups")
            data = await get_reports_in_groups_update(access_token, group['id'])
            for d in data:     
                dataset = await get_dataset_update(access_token,d['datasetId'])
                datasource = await get_datasources_update(access_token,d['datasetId'])
                report = await get_report_update(access_token,d['id'])
                pages = await report_pages_update(access_token,d['id'])
                tables = await get_tables_from_dataset_update(access_token,d['datasetId'],"EVALUATE INFO.TABLES()")
                columns = await get_tables_from_dataset_update(access_token,d['datasetId'],"EVALUATE INFO.COLUMNS()")  
                relationships = await get_tables_from_dataset_update(access_token,d['datasetId'],"EVALUATE INFO.RELATIONSHIPS()")   
                first_element = datasource[0]

        # Access the nested keys
                connectiondet = first_element.get('connectionDetails', {})
                server = connectiondet.get('server', None)
                database = connectiondet.get('database', None)
                path = connectiondet.get('path', None)
                url = connectiondet.get('url', None)
                connection_details_df = pd.DataFrame([{
                    'Server': server,
                    'DatabaseName': database,
                    'Path': path,
                    'URL': url
                }])
                if connection_details_df is not None:
                    engine = connect_mysql_database(hostname,usernamedb,passworddb,"dlineage")
                    if engine:
                        connection = engine.connect()
                        transaction = connection.begin()
                        try:
                            # Insert data into ConnectionDetails table
                            connection_details_df.to_sql("connectiondetails", con=connection, if_exists='append', index=False)

                            # Retrieve the last inserted ID
                            result = connection.execute(text("SELECT LAST_INSERT_ID()"))
                            last_id = result.scalar()
                            
                            print("Last inserted ID:", last_id)
                            dataset_df = pd.DataFrame([{
                            "DatasetID":dataset['id'],
                            "DatasetName":dataset['name'],
                            "TableCount":len(pd.DataFrame(tables)),
                            "DataSourceType":first_element['datasourceType'],
                            "DataSourceID":first_element['datasourceId'],
                            "GatewayID":first_element['gatewayId'],
                            "isRefreshable":dataset['isRefreshable'],
                            "ReportEmbbedURL":dataset['createReportEmbedURL'],
                            "EmbedURL":dataset['qnaEmbedURL'],
                            "ConnectionID":last_id,
                            "ToolID":1        
                        }])
                            try:
                                dataset_df.to_sql("dataset", con=connection, if_exists='append', index=False)
                                print("Data inserted successfully")
                            except Exception as e:
                                print(f"Failed to insert data: {e}")
                            report_df = pd.DataFrame([{
                            "ReportID":report['id'],
                            "ReportName":report.get('name', None),
                            "ReportType":report['reportType'],
                            "PageCount":len(pages),
                            "DatasetID":report['datasetId']
                        }])
                            try:
                                report_df.to_sql("report", con=connection, if_exists='append', index=False)
                                print("report inserted successfully")
                            except Exception as e:
                                print(f"Failed to insert data: {e}")    
                            
                            for table in tables:
                                table_df = pd.DataFrame([{
                                "TableID":table["[ID]"],
                                "TableName":table["[Name]"],
                                "IsHidden":table["[IsHidden]"],
                                "TableStorageID":table["[TableStorageID]"],
                                "ModifiedTime":table["[ModifiedTime]"],
                                "StructureModifiedTime":table["[StructureModifiedTime]"],
                                "SystemFlags":table.get("[SystemFlags]",None),
                                "LineageTag":table.get("[LineageTag]",None),
                                "DataCategory":table.get("[DataCategory]",None),
                                "DatasetID":dataset['id']
                                }])
                                try:
                                    table_df.to_sql("tableentity", con=connection, if_exists='append', index=False)
                                    print("tables inserted successfully")
                                except Exception as e:
                                    print(f"Failed to insert data: {e}")
                            for column in columns:
                                column_df = pd.DataFrame([{
                                "ColumnID":column.get("[ID]",None),
                                "TableID":column.get("[TableID]",None),
                                "ExplicitName":column.get("[ExplicitName]",None),
                                "ExplicitDataType":column.get("[ExplicitDataType]",None),
                                "InferredDataType":column.get("[InferredDataType]",None),
                                "IsHidden":column.get("[IsHidden]",None),
                                "IsKey":column.get("[IsKey]",None),
                                "IsNullable":column.get("[IsNullable]",None),
                                "Alignment":column.get("[Alignment]",None),
                                "ColumnStorageID":column.get("[ColumnStorageID]",None),
                                "SourceColumn":column.get("[SourceColumn]",None),
                                "LineageTag":column.get("[LineageTag]",None),
                                "SummarizeBy":column.get("[SummarizeBy]",None),
                                "ModifiedTime":column.get("[ModifiedTime]",None),
                                "DatasetID":dataset['id']
                                }])
                                try:
                                    column_df.to_sql("columnentity", con=connection, if_exists='append', index=False)
                                    print("column inserted successfully")
                                except Exception as e:
                                    print(f"Failed to insert data: {e}")
                            for relation in relationships:
                                relation_df = pd.DataFrame([{
                                "RelationshipID":relation.get("[ID]",None),
                                "FromTableID":relation.get("[FromTableID]",None),
                                "FromColumnID":relation.get("[FromColumnID]",None),
                                "ToTableID":relation.get("[ToTableID]",None),
                                "ToColumnID":relation.get("[ToColumnID]",None),
                                "FromCardinality":relation.get("[FromCardinality]",None),
                                "ToCardinality":relation.get("[ToCardinality]",None),
                                "CrossFilteringBehavior":relation.get("[CrossFilteringBehavior]",None),
                                "RelyOnReferentialIntegrity":relation.get("[RelyOnReferentialIntegrity]",None),
                                "ModifiedTime":relation.get("[ModifiedTime]",None),
                                "DatasetID":dataset['id']
                                }])
                                try:
                                    relation_df.to_sql("relationships", con=connection, if_exists='append', index=False)
                                    print("relation inserted successfully")
                                except Exception as e:
                                    print(f"Failed to insert data: {e}")                  
                            transaction.commit()          
                        except Exception as e:
                            transaction.rollback()
                            print("Failed to insert data")
                            print("Error:", e)
                        finally:
                            connection.close()    
                
        await asyncio.gather(*[process_group(group) for group in groups])
        return {
                    "message": "Data populated and saved successfully",
                }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# def main():

#     df2 = get_poowerbi()
#     df1 = get_SSIS()
#     combined_df = pd.concat([df2,df1],ignore_index=True)

#     combined_df.to_excel('result.xlsx',index=False)
    
#     if combined_df is not None:
#         engine = connect_mysql_database(hostname,usernamedb,passworddb,dbname)
#         combined_df.to_sql("combined_data",con=engine,if_exists='replace',index=False)
#     else:
#         print("Failed to fetch data for the table.") 
     

# if __name__=='__main__':
    
    # main()