"""Program to extract World Bank Projects data"""
from datetime import datetime
import json
import requests
import pandas as pd

headers = {
    'Content-Type': 'application/json'
}

SEARCH_API_BASE_V1 = "http://search.worldbank.org/api"
SEARCH_API_BASE_V2 = "http://search.worldbank.org/api/v2"
PROJECTS_URL_BASE = SEARCH_API_BASE_V2 + "/projects"
CONTRACTS_URL_BASE = SEARCH_API_BASE_V1 + "/contractdata"

DOCUMENTS_URL_BASE = SEARCH_API_BASE_V2 + "/wds"
FINANCE_URL = 'https://finances.worldbank.org/resource/sfv5-tf7p.json'


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
            i = i + 1000
    return data


def get_documents():
    """Get Document Information"""
    params = {'format': 'json', 'fl': 'docdt,docty,projectid', 'strdate': '1948-01-01',
              'enddate': datetime.today().strftime('%Y-%m-%d'),
              'countrycode_exact': 'IN', 'rows': '1000'}

    response = requests.request(
        "GET", DOCUMENTS_URL_BASE, headers=headers, params=params, timeout=1800)
    # data = json.loads(response.text)
    # doc_count = data['total']
    # document_list = list(data['documents'].values())
    # i = 1000
    # while i < doc_count:
    #     try:
    #         params['os'] = str(i)
    #         response = requests.request(
    #             "GET", DOCUMENTS_URL_BASE, headers=headers, params=params, timeout=1800)
    #         data = json.loads(response.text)
    #         document_list = document_list + list(data['documents'].values())
    #         doc_count = data['total']
    #         i = i + 1000
    #     except:
    #         print('Exception trying to fetch ' + response.url)
    #         continue
    document_list = paginate(url=DOCUMENTS_URL_BASE,
                             entity='documents', params=params)
    documents_dataframe = pd.DataFrame.from_dict(
        document_list, orient='columns')
    documents_dataframe = documents_dataframe.drop(
        ['entityids', 'url', 'url_friendly_title'], axis=1)
    documents_dataframe['display_title'] = documents_dataframe['display_title'].str.replace(
        '\n', ' ')
    documents_dataframe['docdt'] = pd.to_datetime(
        documents_dataframe['docdt'].str.strip(), dayfirst=True, format='%Y-%m-%d').dt.date
    documents_dataframe.sort_values(by='docdt', ascending=False)
    documents_dataframe.to_csv('data/WB_India_Documents.csv')


def get_projects():
    """Get Project Information"""
    params = {'format': 'json', 'kw': 'N', 'countryname_exact': 'Republic of India', 'rows': '1000',
              'fl': """id,countryname_mdk,prodline,borrower,lendinginstr,
              lendinginstrcode,lendinginstrtype_mdk,supplementprojectflg,productlinetype,
              projectstatusdisplay,status,pidrecdbwind,impagency,project_name,
              boardapprovaldate,closingdate,lendprojectcost,ibrdcommamt,idacommamt,
              totalamt"""}
    projects = paginate(url=PROJECTS_URL_BASE,
                        entity='projects', params=params)
    project_dataframe = pd.DataFrame.from_dict(projects, orient='columns')
    project_dataframe.sort_values(by='id', ascending=False)
    project_dataframe.to_csv('data/WB_India_Projects.csv')


def get_finances():
    """Get Finance data"""
    data = json.loads(requests.request("GET", FINANCE_URL).text)
    finance_dataframe = pd.DataFrame.from_dict(data, orient='columns')
    finance_dataframe = finance_dataframe.loc[finance_dataframe['country_code'] == 'IN']
    finance_dataframe.sort_values(by='project_id', ascending=False)
    finance_dataframe.to_csv('data/WB_India_Project_Finance.csv')


def get_contracts():
    """Get all contracts"""
    params = {"countryshortname": "India", "rows": "1000",
              "fl": """project_name,contr_id,contr_sgn_date,contr_desc,
              total_contr_amnt,contr_refnum,procurement_group,suppinfo,
              supplier_countryshortname,procu_meth_text,teammemfullname,
              rvw_type"""}
    contracts_data = paginate(url=CONTRACTS_URL_BASE,
                              entity='contract', params=params, is_dict=False)
    with open('data/WB_India_Contracts.json', 'w') as f:
        json.dump(contracts_data, f)


def main():
    """Main method"""
    get_projects()
    get_documents()
    # get_finances()
    get_contracts()


if __name__ == "__main__":
    main()
