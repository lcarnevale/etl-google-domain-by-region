#!/usr/bin/env python
"""ETL process for creating Google Domain and Languages by Region.
"""
__author__ = "Lorenzo Carnevale"
__license__ = "MIT"

# standard libraries
import os
import re
# third parties libraries
import requests
from bs4 import BeautifulSoup
import pandas as pd

URL_LANGUAGES_BY_REGION = 'https://en.wikipedia.org/wiki/List_of_official_languages_by_country_and_territory'
URL_DOMAIN_BY_REGION = 'https://ipfs.io/ipfs/QmXoypizjW3WknFiJnKLwHCnL72vedxjQkDDP1mXWo6uco/wiki/List_of_Google_domains.html'


def extract():
    languages_by_region = extract_languages_by_region()
    domain_by_region = extract_domain_by_region()
    return languages_by_region, domain_by_region


def extract_languages_by_region():
    regions, languages = get_languages_by_region()
    columns = ['Region', 'Languages']
    return pd.DataFrame(list(zip(regions, languages)), columns=columns)


def get_languages_by_region():
    page = requests.get(URL_LANGUAGES_BY_REGION)
    regions = list()
    languages = list()

    soup = BeautifulSoup(page.content, 'html.parser')
    language_table_rows = soup.find('table', {"class": "wikitable sortable"}). \
        find_all('tr')
    for language_table_row in language_table_rows[1:]:
        language_table_cells = language_table_row.find_all('td')
        if language_table_cells:
            regions.append(language_table_cells[0].text)

            language_cells = language_table_cells[1].find_all('li')
            if language_cells:
                _languages = list()
                for language_cell in language_cells:
                    _languages.append(language_cell.text)
            else:
                _languages = language_table_cells[1].text

            languages.append(_languages)

    return regions, languages


def extract_domain_by_region():
    regions, domains = get_domain_by_region()
    columns = ['Region', 'Domain']
    return pd.DataFrame(list(zip(regions, domains)), columns=columns)


def get_domain_by_region():
    page = requests.get(URL_DOMAIN_BY_REGION)

    regions = list()
    domains = list()

    soup = BeautifulSoup(page.content, 'html.parser')
    domain_table_rows = soup.find('table', {"id": "mwCw"}). \
        find_all('tr')

    for domain_table_row in domain_table_rows[2:]:
        domain_table_cells = domain_table_row.find_all('td')
        if domain_table_cells:
            regions.append(domain_table_cells[0].text)
            domains.append(domain_table_cells[2].text)

    return regions, domains


def transform(languages_by_region, domain_by_region):
    languages_by_region['Region'] = languages_by_region['Region'].apply(lambda entry: transform_region(entry))
    languages_by_region['Languages'] = languages_by_region['Languages'].apply(lambda entry: transform_languages(entry))
    domain_by_region['Region'] = domain_by_region['Region'].apply(lambda entry: transform_region(entry))
    domain_by_region['Domain'] = domain_by_region['Domain'].apply(lambda entry: transform_region(entry))
    left_joined = pd.merge(languages_by_region, domain_by_region, on='Region', how='left')
    return left_joined


def transform_region(data):
    data = data.strip()
    data = remove_square_brackets_and_its_content(data)
    return data


def transform_languages(data):
    if type(data) == list:
        data = ','.join(data)
    data = remove_square_brackets_and_its_content(data)
    data = remove_brackets_and_its_content(data)
    data = data.strip()
    return data


def remove_square_brackets_and_its_content(data):
    pattern = r'\[.*?\]'
    return re.sub(pattern, '', data)


def remove_brackets_and_its_content(data):
    pattern = r'\(.*?\)'
    return re.sub(pattern, '', data)


def load(left_joined):
    outdir = './result'
    if not os.path.exists(outdir):
        os.mkdir(outdir)
    left_joined.to_csv(r'%s/data.csv' % (outdir), index=False)


def main():
    languages_by_region, domain_by_region = extract()
    left_joined = transform(languages_by_region, domain_by_region)
    load(left_joined)


if __name__ == "__main__":
    main()
