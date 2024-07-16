
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
    table = soup.find_all("table", {"class": "wikitable sortable"})[0]
    table_rows = table.find_all('tr')

    return table_rows

def extract_wiki_data(**kwargs):
    url = kwargs['url']
    html = get_wiki_page(url)
    rows = get_wiki_data(html)

    print(rows)