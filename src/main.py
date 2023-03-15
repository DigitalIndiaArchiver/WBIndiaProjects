# """Program to extract World Bank Projects data"""
import json
import requests
import pandas as pd

headers = {
    'Content-Type': 'application/json'
}

PROJECTS_URL_BASE = "http://search.worldbank.org/api/v2/projects"


def main():
    """Main method"""
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
    projectDataFrame = pd.DataFrame.from_dict(projects, orient='columns')
    projectDataFrame.to_csv('data/WB_India_Projects.csv')


if __name__ == "__main__":
    main()
