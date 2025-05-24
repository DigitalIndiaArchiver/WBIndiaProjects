"""Program to extract World Bank Projects data"""
from datetime import datetime, timedelta
import json
import os
import requests
import pandas as pd

headers = {
    'Content-Type': 'application/json'
}

SEARCH_API_BASE_V1 = "http://search.worldbank.org/api"
SEARCH_API_BASE_V2 = "http://search.worldbank.org/api/v2"
PROJECTS_URL_BASE = SEARCH_API_BASE_V2 + "/projects"
PROJECT_ARCHIVE_URL_BASE = SEARCH_API_BASE_V2 + "/projectsarchives"
CONTRACTS_URL_BASE = SEARCH_API_BASE_V1 + "/contractdata"
NOTICES_URL_BASE = SEARCH_API_BASE_V2 + "/procnotices"
NEWS_URL_BASE = SEARCH_API_BASE_V2 + "/news"
MULTIMEDIA_URL_BASE = SEARCH_API_BASE_V2 + "/multimedia"
PHOTO_ARCHIVE_BASE = SEARCH_API_BASE_V2 + "/photoarchives"
DOCUMENTS_URL_BASE = SEARCH_API_BASE_V2 + "/wds"
FINANCE_URL = 'https://finances.worldbank.org/resource/sfv5-tf7p.json'


def load_last_fetch_dates():
    """Loads the last fetch dates from data/last_fetch_dates.json"""
    try:
        with open('data/last_fetch_dates.json', 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return {}


def save_last_fetch_date(entity_name: str, end_date_str: str):
    """Saves the last fetch date for an entity to data/last_fetch_dates.json"""
    dates = load_last_fetch_dates()
    dates[entity_name] = end_date_str
    with open('data/last_fetch_dates.json', 'w') as f:
        json.dump(dates, f, indent=4)


def append_data_to_csv(new_data_df: pd.DataFrame, csv_filepath: str, unique_id_column: str):
    """Appends new data to a CSV file, removing duplicates based on a unique ID column."""
    if os.path.exists(csv_filepath):
        existing_df = pd.read_csv(csv_filepath)
        combined_df = pd.concat([existing_df, new_data_df], ignore_index=True)
        if unique_id_column and unique_id_column in combined_df.columns:
            combined_df.drop_duplicates(subset=[unique_id_column], keep='last', inplace=True)
        combined_df.to_csv(csv_filepath, index=False)
    else:
        new_data_df.to_csv(csv_filepath, index=False)


def paginate(url, entity, params, is_dict=True):
    """Helper method to paginate API data"""
    count = 0
    i = 0
    data = []
    while i <= count:
        try:
            params['os'] = str(i)
            response = requests.request(
                "GET", url, headers=headers, params=params, timeout=1800)
            response_data = json.loads(response.text)
            if is_dict:
                data = data + list(response_data[entity].values())
            else:
                data = data + response_data[entity]
            count = int(response_data['total'])
        except:
            print('Exception trying to fetch ' + response.url)
            continue
        finally:
            i = i + int(params['rows'])
    return data


def get_documents():
    """Get Document Information"""
    dates_data = load_last_fetch_dates()
    last_fetch_str = dates_data.get("documents")

    current_end_date = datetime.today()
    enddate_str = current_end_date.strftime('%Y-%m-%d')

    if last_fetch_str:
        last_fetch_date = datetime.strptime(last_fetch_str, '%Y-%m-%d')
        start_date = last_fetch_date + timedelta(days=1)
        if start_date > current_end_date:
            print(f"Documents data already up to date with {last_fetch_str}. No new data to fetch for the period until {enddate_str}.")
            # Still save the enddate_str as the last successful fetch attempt up to this date.
            save_last_fetch_date("documents", enddate_str)
            return
        strdate_str = start_date.strftime('%Y-%m-%d')
    else:
        strdate_str = '1948-01-01' # Original default

    params = {'format': 'json', 'rows': '1000',
              'fl': 'id,docdt,docty,projectid,display_title', # Ensure 'id' and 'display_title' are included
              'strdate': strdate_str,
              'enddate': enddate_str,
              'countrycode_exact': 'IN'}

    document_list = paginate(url=DOCUMENTS_URL_BASE,
                             entity='documents', params=params)

    if not document_list:
        print(f"No new documents found for India from {strdate_str} to {enddate_str}.")
        save_last_fetch_date("documents", enddate_str)
        return

    documents_dataframe = pd.DataFrame.from_dict(
        document_list, orient='columns')
    documents_dataframe['display_title'] = documents_dataframe['display_title'].str.replace(
        '\n', ' ')
    documents_dataframe['docdt'] = pd.to_datetime(
        documents_dataframe['docdt'].str.strip(), dayfirst=True, format='%Y-%m-%d').dt.date
    documents_dataframe.sort_values(
        by=['projectid', 'docdt'], ascending=False, inplace=True)
    append_data_to_csv(documents_dataframe, 'data/WB_India_Documents.csv', 'id')
    save_last_fetch_date("documents", enddate_str)
    print(f"Successfully fetched and updated document data up to {enddate_str}.")


def get_projects():
    """Get Project Information"""
    dates_data = load_last_fetch_dates()
    last_fetch_str = dates_data.get("projects")

    current_end_date = datetime.today()
    enddate_str = current_end_date.strftime('%Y-%m-%d') # Used for saving state

    if last_fetch_str:
        last_fetch_date = datetime.strptime(last_fetch_str, '%Y-%m-%d')
        start_date = last_fetch_date + timedelta(days=1)
        if start_date > current_end_date:
            print(f"Projects data already up to date with {last_fetch_str}. No new data to fetch for the period until {enddate_str}.")
            save_last_fetch_date("projects", enddate_str)
            return
        strdate_param_val = start_date.strftime('%Y-%m-%d')
    else:
        default_start_date = current_end_date - timedelta(days=2*365)
        strdate_param_val = default_start_date.strftime('%Y-%m-%d')

    enddate_param_val = current_end_date.strftime('%Y-%m-%d')

    params = {'format': 'json', 'kw': 'N', 'countryname_exact': 'Republic of India', 'rows': '1000',
              'fl': """id,countryname_mdk,prodline,borrower,lendinginstr,
                      lendinginstrcode,lendinginstrtype_mdk,supplementprojectflg,productlinetype,
                      projectstatusdisplay,status,pidrecdbwind,impagency,project_name,
                      boardapprovaldate,closingdate,lendprojectcost,ibrdcommamt,idacommamt,
                      totalamt""",
              'strdate': strdate_param_val,
              'enddate': enddate_param_val}
    projects = paginate(url=PROJECTS_URL_BASE,
                        entity='projects', params=params)

    if not projects:
        print(f"No new projects found for India with board approval dates from {strdate_param_val} to {enddate_param_val}.")
        save_last_fetch_date("projects", enddate_str)
        return

    project_dataframe = pd.DataFrame.from_dict(projects, orient='columns')
    project_dataframe.sort_values(by='id', ascending=False, inplace=True)
    append_data_to_csv(project_dataframe, 'data/WB_India_Projects.csv', 'id')
    save_last_fetch_date("projects", enddate_str)
    print(f"Successfully fetched and updated project data up to {enddate_str} (based on board approval date range query from {strdate_param_val}).")


def get_finances():
    """Get Finance data"""
    dates_data = load_last_fetch_dates()
    last_fetch_str = dates_data.get("finances")

    current_end_date = datetime.today()
    enddate_str = current_end_date.strftime('%Y-%m-%d') # Used for saving state

    start_date_filter_val = None
    if last_fetch_str:
        last_fetch_date = datetime.strptime(last_fetch_str, '%Y-%m-%d')
        start_date = last_fetch_date + timedelta(days=1)
        if start_date > current_end_date:
            print(f"Finances data already up to date with {last_fetch_str}. No new data to fetch for the period until {enddate_str}.")
            save_last_fetch_date("finances", enddate_str)
            return
        start_date_filter_val = start_date.strftime('%Y-%m-%d')
    else:
        default_start_date = current_end_date - timedelta(days=5*365)
        start_date_filter_val = default_start_date.strftime('%Y-%m-%d')

    # Construct the Socrata query
    base_query_params = ["country_code=IN"]
    where_clauses = []
    if start_date_filter_val:
        where_clauses.append(f"project_signing_date>'{start_date_filter_val}'")

    if where_clauses:
        base_query_params.append(f"$where={' AND '.join(where_clauses)}")

    request_url = f"{FINANCE_URL}?{'&'.join(base_query_params)}"

    print(f"Requesting finance data from: {request_url}")
    response = requests.get(request_url, timeout=1800)
    response.raise_for_status() 
    data = response.json()

    if not data:
        print(f"No new finance data found for India with project signing date after {start_date_filter_val}.")
        save_last_fetch_date("finances", enddate_str)
        return

    finance_dataframe = pd.DataFrame.from_dict(data, orient='columns')
    finance_dataframe.sort_values(
        by='project_id', ascending=False, inplace=True)
    append_data_to_csv(finance_dataframe, 'data/WB_India_Project_Finance.csv', 'project_id')
    save_last_fetch_date("finances", enddate_str)
    print(f"Successfully fetched and updated finance data for projects signed after {start_date_filter_val}, up to {enddate_str}.")


def get_contracts():
    """Get all contracts"""
    dates_data = load_last_fetch_dates()
    last_fetch_str = dates_data.get("contracts")

    current_end_date = datetime.today()
    enddate_str = current_end_date.strftime('%Y-%m-%d') # Used for saving state

    if last_fetch_str:
        last_fetch_date = datetime.strptime(last_fetch_str, '%Y-%m-%d')
        start_date = last_fetch_date + timedelta(days=1)
        if start_date > current_end_date:
            print(f"Contracts data already up to date with {last_fetch_str}. No new data to fetch for the period until {enddate_str}.")
            save_last_fetch_date("contracts", enddate_str)
            return
        strdate_param_val = start_date.strftime('%Y-%m-%d')
    else:
        # For initial load, let's fetch data for contracts signed in the last 2 years.
        default_start_date = current_end_date - timedelta(days=2*365)
        strdate_param_val = default_start_date.strftime('%Y-%m-%d')

    enddate_param_val = current_end_date.strftime('%Y-%m-%d')

    params = {"countryshortname": "India", "rows": "1000",
              "fl": "project_name,contr_id,contr_sgn_date,contr_desc,total_contr_amnt,contr_refnum,procurement_group,suppinfo,supplier_countryshortname,procu_meth_text,teammemfullname,rvw_type", # contr_id is here
              "strdate": strdate_param_val, # Attempt to filter by contract sign date
              "enddate": enddate_param_val}
    contracts_data = paginate(url=CONTRACTS_URL_BASE,
                              entity='contract', params=params, is_dict=False)

    if not contracts_data:
        print(f"No new contracts found for India with signing dates from {strdate_param_val} to {enddate_param_val}.")
        save_last_fetch_date("contracts", enddate_str)
        return

    contracts_dataframe = pd.DataFrame.from_dict(contracts_data)
    # Ensure 'contr_id' is suitable as a unique ID. If not, this needs adjustment.
    append_data_to_csv(contracts_dataframe, 'data/WB_India_Contracts.csv', 'contr_id')
    save_last_fetch_date("contracts", enddate_str)
    print(f"Successfully fetched and updated contract data up to {enddate_str} (based on contract signing date range query from {strdate_param_val}).")


def get_archives():
    """Get all archive records"""
    params = {'format': 'json',
              'countryname': 'Republic of India', 'rows': '1000'}
    archive_data = paginate(url=PROJECT_ARCHIVE_URL_BASE,
                            entity='projectsarchives', params=params, is_dict=False)
    archive_data = pd.DataFrame.from_dict(archive_data, orient='columns')
    archive_data.sort_values(by='project_id', ascending=False, inplace=True)
    archive_data.to_csv('data/WB_India_Project_Archives.csv')


def get_notices():
    """Get Procurement Notices"""
    dates_data = load_last_fetch_dates()
    last_fetch_str = dates_data.get("notices")

    current_end_date = datetime.today()
    enddate_str = current_end_date.strftime('%Y-%m-%d') # Used for saving state

    if last_fetch_str:
        last_fetch_date = datetime.strptime(last_fetch_str, '%Y-%m-%d')
        start_date = last_fetch_date + timedelta(days=1)
        if start_date > current_end_date:
            print(f"Notices data already up to date with {last_fetch_str}. No new data to fetch for the period until {enddate_str}.")
            save_last_fetch_date("notices", enddate_str)
            return
        strdate_param_val = start_date.strftime('%Y-%m-%d')
    else:
        # For initial load, let's fetch data for notices from the last 2 years.
        default_start_date = current_end_date - timedelta(days=2*365)
        strdate_param_val = default_start_date.strftime('%Y-%m-%d')

    enddate_param_val = current_end_date.strftime('%Y-%m-%d')

    params = {'format': 'json', 'rows': '1000',  # Increased from 10 to 1000
              'project_ctry_name': 'India',
              'fl': """id,notice_type,noticedate,notice_lang_name,notice_status,
                       project_name,bid_reference_no,bid_description,procurement_group,
                       procurement_method_code,submission_date,notice_text""", # id and noticedate are here
              'srt': 'id desc', # Keep existing sort if it's beneficial
              'strdate': strdate_param_val, # Add strdate to filter by noticedate
              'enddate': enddate_param_val}   # Add enddate to filter by noticedate
    notices_data = paginate(url=NOTICES_URL_BASE,
                            entity='procnotices', params=params, is_dict=False)

    if not notices_data:
        print(f"No new notices found for India with notice dates from {strdate_param_val} to {enddate_param_val}.")
        save_last_fetch_date("notices", enddate_str)
        return

    notices_data = pd.DataFrame.from_dict(notices_data, orient='columns')
    append_data_to_csv(notices_data, 'data/WB_India_Procurement_Notices.csv', 'id')
    save_last_fetch_date("notices", enddate_str)
    print(f"Successfully fetched and updated notices data up to {enddate_str} (based on notice date range query from {strdate_param_val}).")


def get_news():
    """Get News"""
    dates_data = load_last_fetch_dates()
    last_fetch_str = dates_data.get("news")

    current_end_date = datetime.today()
    enddate_str = current_end_date.strftime('%Y-%m-%d') # Used for saving state

    if last_fetch_str:
        last_fetch_date = datetime.strptime(last_fetch_str, '%Y-%m-%d')
        start_date = last_fetch_date + timedelta(days=1)
        if start_date > current_end_date:
            print(f"News data already up to date with {last_fetch_str}. No new data to fetch for the period until {enddate_str}.")
            save_last_fetch_date("news", enddate_str)
            return
        strdate_param_val = start_date.strftime('%Y-%m-%d')
    else:
        # For initial load, let's fetch data for news from the last 2 years.
        default_start_date = current_end_date - timedelta(days=2*365)
        strdate_param_val = default_start_date.strftime('%Y-%m-%d')

    enddate_param_val = current_end_date.strftime('%Y-%m-%d')

    params = {'format': 'json', 'rows': '1000', 'countcode': 'IN',
              'fl': """id,url,topic,lnchdt,conttype,displayconttype,
                      originating_unit,funding_source""", # id and lnchdt are here
              'strdate': strdate_param_val, # Add strdate to filter by lnchdt (launch date)
              'enddate': enddate_param_val}   # Add enddate to filter by lnchdt
    news_data = paginate(url=NEWS_URL_BASE,
                         entity='documents', params=params)

    if not news_data:
        print(f"No new news items found for India with launch dates from {strdate_param_val} to {enddate_param_val}.")
        save_last_fetch_date("news", enddate_str)
        return

    news_data = pd.DataFrame.from_dict(news_data, orient='columns')
    append_data_to_csv(news_data, 'data/WB_India_News.csv', 'id')
    save_last_fetch_date("news", enddate_str)
    print(f"Successfully fetched and updated news data up to {enddate_str} (based on launch date range query from {strdate_param_val}).")


def get_multimedia():
    """Get Multimedia"""
    # https://search.worldbank.org/api/v2/multimedia?format=json&rows=1000&country=India
    params = {'format': 'json', 'rows': '1000', 'country': 'India'}
    multimedia_data = paginate(
        url=MULTIMEDIA_URL_BASE, entity='multimedia', params=params)
    multimedia_data = pd.DataFrame.from_dict(multimedia_data, orient='columns')
    multimedia_data.to_csv('data/WB_India_Multimedia.csv')


def main():
    """Main method"""
    get_projects()

    get_notices()
    get_contracts()

    get_documents()
    get_archives()

    get_news()
    get_multimedia()

    get_finances()


if __name__ == "__main__":
    main()
