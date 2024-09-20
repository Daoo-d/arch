from main2 import *

import pandas as pd
from main1 import *
import os
import glob

def connect_mysql_database(hostname,username,password,dbname):
    try:    
        engine = create_engine(f"mysql+pymysql://{username}:{password}@{hostname}/{dbname}")
        print("database connected successfully")
        return engine
    except Exception as e:
        print("Failed to connect to database")
        print("Error:", e)
        engine = None 

def get_SSIS():
    
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
    combined['Tool'] = 'SSIS'
    combined.to_excel("SSISDB.xlsx",index=False)

    return combined

def get_poowerbi():
    bearer_token = read_access_token(username,password)

#     # data = get_tables_from_dataset(bearer_token,dataset_id,"EVALUATE INFO.TABLES()")
    groups = get_groups(bearer_token)
    dataframes = []
    dfs = []
    dfs2 = []
    dfs3 = []
    for group in groups:
        tname = group['name']
        print(f"get :{tname}")
        data = get_reports_in_groups(bearer_token,group['id'])
        for d in data:
            reportid = d['id']
            datasetid = d['datasetId']
            dataframe1,dataframe2,dataframe3,dataframe4 = getdict_expr(bearer_token,reportid,datasetid)
            dfs.append(dataframe2)
            dataframes.append(dataframe1)
            dfs2.append(dataframe3) 
            dfs3.append(dataframe4)   
        combined_df = pd.concat(dfs, ignore_index=True)
        combined_df1 = pd.concat(dfs2,ignore_index=True)
        combined_df3 = pd.concat(dfs3,ignore_index=True)

        da = combined_df.merge(combined_df1,on="Dataset ID").merge(combined_df3,on="Dataset ID")
        da.to_excel('merge.xlsx',index=False)
        combined_df2 = pd.concat(dataframes,ignore_index=True)
        dataset = combined_df2.merge(da, on='Dataset ID')
        dataset['Tool']="Power Bi"
        dataset.to_excel("output4.xlsx",index=False)
    return dataset    


def main():

    df2 = get_poowerbi()
    df1 = get_SSIS()
    combined_df = pd.concat([df2,df1],ignore_index=True)

    combined_df.to_excel('result.xlsx',index=False)
    
    # if combined_df is not None:
    #     engine = connect_mysql_database(hostname,usernamedb,passworddb,dbname)
    #     combined_df.to_sql("combined_data",con=engine,if_exists='replace',index=False)
    # else:
    #     print("Failed to fetch data for the table.") 
     

if __name__=='__main__':
    main()