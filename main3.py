import asyncio
from playwright.async_api import async_playwright
import time
import json

TOKEN_FILE_NEW = 'bearer_token.json'
username = 'dawaood.ahmed@bitsglobalconsulting.com'
password = '198924Aa.'

async def run():
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

asyncio.run(run())


# import xml.etree.ElementTree as ET
# from bs4 import BeautifulSoup
# import pandas as pd
# import os,glob


# def get_package_data(file_path):
#     with open(file_path, 'r', encoding='utf-8') as file:
#         content = file.read()
#         soup = BeautifulSoup(content, 'lxml-xml')
        
#         # Example: Extracting all 'Executable' elements
#         with open('output2.xml', 'w', encoding='utf-8') as output_file:
#             output_file.write(soup.prettify())
#         # if executable:
#         #     executable_data = {attr: executable.get(attr) for attr in executable.attrs}
#         #     df = pd.DataFrame([executable_data])
#         #     print(df)
#         # else:
#         #     print("No 'DTS:Executable' element found.")

# def handle_duplicate_columns(data_dict):
#     seen = {}
#     for key in list(data_dict.keys()):
#         if key in seen:
#             seen[key] += 1
#             new_key = f"{key}_{seen[key]}"
#             data_dict[new_key] = data_dict.pop(key)
#         else:
#             seen[key] = 0
#     return data_dict

# def parse_nested_properties(value):
#     nested_dict = {}
#     for item in value.split(';'):
#         if '=' in item:
#             key, val = item.split('=', 1)
#             nested_dict[key.strip()] = val.strip()
#     return nested_dict

# def extract_properties(element, prefix=''):
#     properties = {}
#     for attr, value in element.attrs.items():
#         properties[f"{prefix}{attr}"] = value
#     for child in element.find_all(recursive=False):
#         child_prefix = f"{prefix}{child.name}_"
#         for attr, value in child.attrs.items():
#             properties[f"{child_prefix}{attr}"] = value
#         # Check if the child text contains nested key-value pairs
#         if child.text and ';' in child.text and '=' in child.text:
#             nested_properties = parse_nested_properties(child.text.strip())
#             for key, val in nested_properties.items():
#                 properties[f"{child_prefix}{key}"] = val
        
#     return properties
    
# def get_element(file_path,element):
#     with open(file_path, 'r', encoding='utf-8') as file:
#         content = file.read()
#         soup = BeautifulSoup(content, 'lxml-xml')
#         connection_managers = soup.find_all({element})

#         data = []
#         for cm in connection_managers:
#             cm_data = extract_properties(cm)
#             cm_data = handle_duplicate_columns(cm_data)
#             data.append(cm_data)
#         print(data)
#         df = pd.DataFrame(data)
#         df.dropna(how='all', inplace=True)
#         return df


# def get_inner_element_names(element):
#     """Recursively get the names of all inner elements."""
#     element_names = set()
#     for child in element.find_all(recursive=False):
#         element_names.add(child.name)
#         element_names.update(get_inner_element_names(child))
#     return element_names

# def get_names(file_path):
#     with open(file_path, 'r', encoding='utf-8') as file:
#         content = file.read()
#         soup = BeautifulSoup(content, 'lxml-xml')
#         connection_managers = soup.find_all('DTS:Executable')
#         for cm in connection_managers:
#             element_names = get_inner_element_names(cm)
#             print(f"Parent Element: {cm.name}")
#             print(f"Inner Elements: {element_names}")

# def get_SSIS(file_paths):
    
#     dataframes = []
#     i=1
#     for file_path in file_paths:
        
#         connections = get_element(file_path, 'DTS:Executable')
#         component = get_element(file_path, 'component')
#         component['Source'] = 'component'
#         connections['Source'] = 'connection'

#         df = pd.concat([connections,component], ignore_index=True)
#         df['package'] = f'package_{i}'
#         dataframes.append(df)
#         print(f"Data added for {file_path}")
#         i+=1

#     SSIS_df = pd.concat(dataframes, ignore_index=True)
#     SSIS_df.fillna('S', inplace=True)
#     SSIS_df['Tool'] = 'SSIS'
#     SSIS_df.to_excel('SSISdata.xlsx',index=False)
    
#     return SSIS_df

# # def main():
# #     directory = r'D:\arch\sample2'
# #     file_paths = glob.glob(os.path.join(directory, '**', '*.dtsx'), recursive=True)
# #     unique_files = set()

# # # List to store unique file paths
# #     unique_file_paths = []

# #     for filep in file_paths:
# #         file_name = os.path.basename(filep)
# #         if file_name not in unique_files:
# #             unique_files.add(file_name)
# #             unique_file_paths.append(filep)
    
# #     df1 = get_SSIS(unique_file_paths)
    

# # if __name__=="__main__":
# #     main()




# # from lxml import etree
# # import json
# # from collections import defaultdict

# # def xml_to_json(file_path):
# #     tree = etree.parse(file_path)
# #     root = tree.getroot()
    
# #     def etree_to_dict(t):
# #         d = {t.tag: {} if t.attrib else None}
# #         children = list(t)
# #         if children:
# #             dd = defaultdict(list)
# #             for dc in map(etree_to_dict, children):
# #                 for k, v in dc.items():
# #                     dd[k].append(v)
# #             d = {t.tag: {k:v[0] if len(v) == 1 else v for k, v in dd.items()}}
# #         if t.attrib:
# #             d[t.tag].update(('@' + k, v) for k, v in t.attrib.items())
# #         if t.text:
# #             text = t.text.strip()
# #             if children or t.attrib:
# #                 if text:
# #                     d[t.tag]['#text'] = text
# #             else:
# #                 d[t.tag] = text
# #         return d

# #     data_dict = etree_to_dict(root)
# #     json_data = json.dumps(data_dict, indent=4)
# #     return json_data

# # # Example usage
# # file_path = r'D:\arch\Integration_Project\Integration_Project\Package.dtsx'
# # json_output = xml_to_json(file_path)
# # print(json_output)


# # tree = ET.parse(file_path)
#     # root = tree.getroot()
    
#     # namespace = {'dts': 'www.microsoft.com/SqlServer/Dts'}
    
#     # print(root)
#     # # Example: Print the names of all tasks in the package
#     # # for task in root.findall('.//dts:Executable', namespace):
#     # #     print(f"Task Name: {task.get('{www.microsoft.com/SqlServer/Dts}Name')}")
    
#     # # # Print all connections
#     # # for connection in root.findall('.//dts:ConnectionManager', namespace):
#     # #     print(f"Connection Name: {connection.get('{www.microsoft.com/SqlServer/Dts}Name')}")
#     # #     print(f"Connection String: {connection.get('{www.microsoft.com/SqlServer/Dts}ConnectionString')}")
