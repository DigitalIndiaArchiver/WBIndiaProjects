# World Bank API Documentation

This document provides a description of the World Bank APIs used in this project to retrieve data about projects in India.

## Projects API

*   **Endpoint:** `http://search.worldbank.org/api/v2/projects`
*   **Description:** Retrieves information about World Bank projects.
*   **Parameters used in `src/main.py`:**
    *   `format`: `json`
    *   `kw`: `N`
    *   `countryname_exact`: `Republic of India`
    *   `rows`: `1000`
    *   `fl`: `id,countryname_mdk,prodline,borrower,lendinginstr,lendinginstrcode,lendinginstrtype_mdk,supplementprojectflg,productlinetype,projectstatusdisplay,status,pidrecdbwind,impagency,project_name,boardapprovaldate,closingdate,lendprojectcost,ibrdcommamt,idacommamt,totalamt`

## Documents API

*   **Endpoint:** `http://search.worldbank.org/api/v2/wds`
*   **Description:** Retrieves project documents.
*   **Parameters used in `src/main.py`:**
    *   `format`: `json`
    *   `rows`: `1000`
    *   `fl`: `docdt,docty,projectid`
    *   `strdate`: `1948-01-01`
    *   `enddate`: current date
    *   `countrycode_exact`: `IN`

## Finances API

*   **Endpoint:** `https://finances.worldbank.org/resource/sfv5-tf7p.json`
*   **Description:** Retrieves project finance data.
*   **Parameters used in `src/main.py`:**
    *   The script filters the data for `country_code` equal to `IN`.

## Project Archives API

*   **Endpoint:** `http://search.worldbank.org/api/v2/projectsarchives`
*   **Description:** Retrieves archived project records.
*   **Parameters used in `src/main.py`:**
    *   `format`: `json`
    *   `countryname`: `Republic of India`
    *   `rows`: `1000`

## News API

*   **Endpoint:** `http://search.worldbank.org/api/v2/news`
*   **Description:** Retrieves news articles related to World Bank projects.
*   **Parameters used in `src/main.py`:**
    *   `format`: `json`
    *   `rows`: `1000`
    *   `countcode`: `IN`
    *   `fl`: `id,url,topic,lnchdt,conttype,displayconttype,originating_unit,funding_source`

## Multimedia API

*   **Endpoint:** `http://search.worldbank.org/api/v2/multimedia`
*   **Description:** Retrieves multimedia content related to World Bank projects.
*   **Parameters used in `src/main.py`:**
    *   `format`: `json`
    *   `rows`: `1000`
    *   `country`: `India`

## Disabled APIs

The following APIs are currently disabled in `src/main.py` due to issues with data fetching or file size.

### Procurement Notices API

*   **Endpoint:** `http://search.worldbank.org/api/v2/procnotices`
*   **Description:** Retrieves procurement notices for projects.

### Contracts API

*   **Endpoint:** `http://search.worldbank.org/api/v1/contractdata`
*   **Description:** Retrieves contract data for projects.
