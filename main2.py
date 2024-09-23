from playwright.sync_api import sync_playwright
import requests
import pandas as pd
import pypyodbc as odbc
import pyodbc
import asyncio
from sqlalchemy import create_engine

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

