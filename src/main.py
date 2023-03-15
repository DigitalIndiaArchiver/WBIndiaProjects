"""Program to extract World Bank Projects data"""
from datetime import datetime
import json
import requests
import pandas as pd

headers = {
    'Content-Type': 'application/json'
}

PROJECTS_URL_BASE = "http://search.worldbank.org/api/v2/projects"
DOCUMENTS_URL_BASE = "https://search.worldbank.org/api/v2/wds"


def get_documents():
    """Get Document Information"""
    params = {'format': 'json', 'fl': 'docdt,docty,projectid', 'strdate': '1948-01-01',
              'enddate': datetime.today().strftime('%Y-%m-%d'),
              'countrycode_exact': 'IN', 'rows': '1000'}

    response = requests.request(
        "GET", DOCUMENTS_URL_BASE, headers=headers, params=params, timeout=1800)
    data = json.loads(response.text)
    doc_count = data['total']
    document_list = list(data['documents'].values())
    i = 1000
    while i < doc_count:
        try:
            params['os'] = str(i)
            response = requests.request(
                "GET", DOCUMENTS_URL_BASE, headers=headers, params=params, timeout=1800)
            data = json.loads(response.text)
            document_list = document_list + list(data['documents'].values())
            doc_count = data['total']
            i = i + 1000
        except:
            print('Exception trying to fetch ' + response.url)
            continue
    documents_dataframe = pd.DataFrame.from_dict(
        document_list, orient='columns')
    documents_dataframe = documents_dataframe.drop(['entityids','url','url_friendly_title'], axis=1)
    documents_dataframe['display_title'] =  documents_dataframe['display_title'].str.replace('\n',' ')
    documents_dataframe['docdt'] = pd.to_datetime(documents_dataframe['docdt'].str.strip(), dayfirst=True,format= '%Y-%m-%d').dt.date
    documents_dataframe.sort_values(by='docdt', ascending=False)
    documents_dataframe.to_csv('data/WB_India_Documents.csv')


def get_projects():
    """Get Project Information"""
    params = {'format': 'json', 'kw': 'N', 'countryname_exact': 'Republic of India', 'rows': '1000',
              'fl': """id,regionname,countryname_mdk,prodline,borrower,lendinginstr,
              lendinginstrcode,lendinginstrtype_mdk,supplementprojectflg,productlinetype,
              projectstatusdisplay,status,pidrecdbwind,impagency,project_name,
              boardapprovaldate,closingdate,lendprojectcost,ibrdcommamt,idacommamt,
              totalamt"""}
    response = requests.request(
        "GET", PROJECTS_URL_BASE, headers=headers, params=params, timeout=1800)
    data = json.loads(response.text)
    projects = data['projects'].values()
    project_dataframe = pd.DataFrame.from_dict(projects, orient='columns')
    project_dataframe.sort_values(by='id', ascending=False)
    project_dataframe.to_csv('data/WB_India_Projects.csv')


def main():
    """Main method"""
    get_projects()
    get_documents()


if __name__ == "__main__":
    main()
