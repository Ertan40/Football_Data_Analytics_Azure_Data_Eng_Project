import json
import geocoder
import pandas as pd


NO_IMAGE = ('https//:upload.wikimedia.org/wikipedia/commons/thumb/0/0a/No-image-available.png/480px-No-image-available'
            '.png')


def get_wikipedia_page(url):
    import requests

    print("Getting wikipedia page...", url)

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException as error:
        print(f"An error occurred: {str(error)}")
        return None


def get_wikipedia_data(html):
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, 'html.parser')
    table = soup.find("table", class_="wikitable sortable sticky-header")
    if table:
        rows = table.find_all('tr')  # find all rows in the table
        return rows

    else:
        print("Table not found!")
        return []


def clean_text(text):
    text = str(text).strip()
    text = text.replace('&nbsp', '')
    if text.find(' ♦'):
        text = text.split(' ♦')[0]
    if text.find('[') != -1:
        text = text.split('[')[0]
    if text.find(' (formerly)') != -1:
        text = text.split(' (formerly)')[0]

    return text.replace('\n', '')


def extract_wikipedia_data(**kwargs):
    url = kwargs['url']
    html = get_wikipedia_page(url)
    table_rows = get_wikipedia_data(html)

    data = []
    for i in range(1, len(table_rows)):
        tds = table_rows[i].find_all('td')
        values = {
            'rank': i,
            'stadium': clean_text(tds[0].text),
            'capacity': clean_text(tds[1].text).replace(',', '').replace('.', ''),
            'region': clean_text(tds[2].text),
            'country': clean_text(tds[3].text),
            'city': clean_text(tds[4].text),
            'images': 'https//:' + tds[5].find('img').get('src').split('//')[1] if tds[5].find('img') else NO_IMAGE,
            # 'images': tds[5].find('img')['src'] if tds[5].find('img') and tds[5].find('img').get(
            #     'src') else 'NO_IMAGE',
            'home team': clean_text(tds[6].text)
        }
        data.append(values)

    json_rows = json.dumps(data)
    kwargs['ti'].xcom_push(key='table_rows', value=json_rows)

    return "OK"


def get_lat_long(country, city):
    location = geocoder.arcgis(f'{city}, {country}')

    if location.ok:
        return location.latlng[0], location.latlng[1]

    return None


def transform_wikipedia_data(**kwargs):
    data = kwargs['ti'].xcom_pull(key='table_rows', task_ids='extract_data_from_wikipedia')
    data = json.loads(data)
    stadiums_df = pd.DataFrame(data)
    stadiums_df['location'] = stadiums_df.apply(lambda x: get_lat_long(x['country'], x['stadium']), axis=1)
    stadiums_df['images'] = stadiums_df['images'].apply(lambda x: x if x not in ['NO_IMAGE', '', None] else NO_IMAGE)
    stadiums_df['capacity'] = stadiums_df['capacity'].astype(int)

    # Handle duplicates
    duplicates = stadiums_df[stadiums_df.duplicated(['location'])]
    duplicates['location'] = duplicates.apply(lambda x: get_lat_long(x['country'], x['city']), axis=1)
    stadiums_df.update(duplicates)

    # Push to xcom
    kwargs['ti'].xcom_push(key='table_rows', value=stadiums_df.to_json())

    return "OK"


def write_wikipedia_data(**kwargs):
    from datetime import datetime

    # Pull data from XCom
    data = kwargs['ti'].xcom_pull(key='table_rows', task_ids='transform_wikipedia_data')
    print(f"Pulled data from XCom: {data}")  # Log data
    # Check if data exists
    if data is None:
        raise ValueError("No data found in XCom for key 'table_rows' from task 'transform_wikipedia_data'")
    # Convert JSON to DataFrame
    try:
        data = json.loads(data)
    except json.JSONDecodeError as e:
        raise ValueError(f"Failed to parse JSON data from XCom: {e}")
    data = pd.DataFrame(data)
    # Generate a unique filename
    file_name = ('stadiums_cleaned_' + str(datetime.now().date()) + "_" +
                 datetime.now().strftime("%H_%M_%S") + '.csv')
    # Write to CSV
    # data.to_csv('data/' + file_name, index=False)
    # print(f"File saved to: data/{file_name}")
    # return "File written successfully"

    # Save to Azure
    data.to_csv('abfs://footballdataeng@ertanfootballdatalake.dfs.core.windows.net/raw-data/' + file_name,
                storage_options={
                    'account_key': 'Your_account_key_goes_here'
                }, index=False)



