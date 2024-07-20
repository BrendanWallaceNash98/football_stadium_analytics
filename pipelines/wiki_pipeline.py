import json

import pandas as pd


def get_wiki_page(url):
    import requests

    print("getting page...")

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()

        return response.text
    except requests.RequestException as e:
        print(f"error making requests {e}")

def get_wiki_data(html):
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, 'html.parser')
    table = soup.find_all("table", {"class": "wikitable sortable sticky-header"})[0]
    table_rows = table.find_all('tr')

    return table_rows

def clean_text(text):
    text = str(text).strip()
    text = text.replace("&nsp", '')
    if text.find(' ♦'):
        text = text.split(' ♦')[0]
    if text.find('[') != -1:
        text = text.split('[')[0]
    if text.find(' (formerly)') != -1:
        text = text.split(' (formerly)')[0]
    if text == '\n':
        return ''
    return text.replace('\n', '')

def extract_wiki_data(**kwargs):
    import json
    url = kwargs['url']
    html = get_wiki_page(url)
    print(html)
    rows = get_wiki_data(html)

    data = []
    ## skipping header
    for i in range(1, len(rows)):
        tds = rows[i].find_all('td')
        values = {
            'rank': i,
            'stadium': clean_text(tds[0].text),
            'capacity': clean_text(tds[1].text).replace(',', ''),
            'region': clean_text(tds[2].text),
            'country': clean_text(tds[3].text),
            'city': clean_text(tds[4].text),
            'images': clean_text(tds[5].find('img').get('src').split("//")[1] if tds[5].find('img') else 'NO_IMAGE'),
            'home_team': clean_text(tds[6].text),
        }

        data.append(values)

    json_rows = json.dumps(data)
    kwargs['ti'].xcom_push(key='rows', value=json_rows)

    return 'OK'


def transform_wikipedia_data(**kwargs):
    import pandas as pd
    data = kwargs['id'].xcom_pull(key='row', task_ids='extract_data_from_wiki')

    data = json.load(data)

    stadium_df = pd.DataFrame(data)

    stadium_df


