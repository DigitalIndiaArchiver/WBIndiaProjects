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
PROJECT_ARCHIVE_URL_BASE = SEARCH_API_BASE_V2 + "/projectsarchives"
CONTRACTS_URL_BASE = SEARCH_API_BASE_V1 + "/contractdata"
NOTICES_URL_BASE = SEARCH_API_BASE_V2 + "/procnotices"
NEWS_URL_BASE = SEARCH_API_BASE_V2 + "/news"
MULTIMEDIA_URL_BASE = SEARCH_API_BASE_V2 + "/multimedia"
PHOTO_ARCHIVE_BASE = SEARCH_API_BASE_V2 + "/photoarchives"
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
            i = i + int(params['rows'])
    return data


def get_documents():
    """Get Document Information"""
    params = {'format': 'json', 'rows': '1000',
              'fl': 'docdt,docty,projectid', 'strdate': '1948-01-01',
              'enddate': datetime.today().strftime('%Y-%m-%d'),
              'countrycode_exact': 'IN'}

    response = requests.request(
        "GET", DOCUMENTS_URL_BASE, headers=headers, params=params, timeout=1800)
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
    documents_dataframe.sort_values(
        by=['projectid', 'docdt'], ascending=False, inplace=True)
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
    project_dataframe.sort_values(by='id', ascending=False, inplace=True)
    project_dataframe.to_csv('data/WB_India_Projects.csv')


def get_finances():
    """Get Finance data"""
    data = json.loads(requests.request("GET", FINANCE_URL).text)
    finance_dataframe = pd.DataFrame.from_dict(data, orient='columns')
    finance_dataframe = finance_dataframe.loc[finance_dataframe['country_code'] == 'IN']
    finance_dataframe.sort_values(
        by='project_id', ascending=False, inplace=True)
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
    # https://search.worldbank.org/api/v2/procnotices?project_ctry_name=India&format=json&row=1000&fl=id,notice_type,noticedate,notice_lang_name,notice_status,project_name,bid_reference_no,bid_description,procurement_group,procurement_method_code,submission_date,notice_text&srt=id%20desc
    params = {'format': 'json', 'rows': '10', 'project_ctry_name': 'India',
              'fl': """id,notice_type,noticedate,notice_lang_name,notice_status,
               project_name,bid_reference_no,bid_description,procurement_group,
               procurement_method_code,submission_date,notice_text""",
              'srt': 'id desc'}
    notices_data = paginate(url=NOTICES_URL_BASE,
                            entity='procnotices', params=params, is_dict=False)
    notices_data = pd.DataFrame.from_dict(notices_data, orient='columns')
    notices_data.to_csv('data/WB_India_Procurement_Notices.csv')


def get_news():
    """Get News"""
    # https://search.worldbank.org/api/v2/news?format=json&rows=1000&countcode=IN&srt=start_date%20desc&fl=id,url,topic,lnchdt,conttype,displayconttype,originating_unit,funding_source
    params = {'format': 'json', 'rows': '1000', 'countcode': 'IN',
              'fl': """id,url,topic,lnchdt,conttype,displayconttype,
              originating_unit,funding_source"""}
    news_data = paginate(url=NEWS_URL_BASE,
                         entity='documents', params=params)
    news_data = pd.DataFrame.from_dict(news_data, orient='columns')
    news_data.to_csv('data/WB_India_News.csv')


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
