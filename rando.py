import pandas as pd
import numpy as np
from urllib.request import urlopen
import bs4
import requests

url = "http://www.psudataeng.com:8000/getStopEvents/"
page = requests.get(url)

soup = bs4.BeautifulSoup(page.text, 'lxml')

for i in range(1,600):
    table = soup.find_all('table')[i]
    # headers = [heading.text for heading in table.find_all('th')]
    # rows = [row for row in table.find_all('tr')]
    # results = [{headers[index]:cell.text for index, cell in enumerate(row.find_all("td"))} for row in rows]
    for children in table:
        for td in children:
            headers = [heading.text for heading in table.find_all('th')]
            rows = [row for row in table.find_all('tr')]
            results = [{headers[index]:cell.text for index, cell in enumerate(row.find_all("td"))} for row in rows]

    df = pd.DataFrame(results)
    out = df.to_json(orient='records')[1:-1]
    with open('file_name.json', 'a') as f:
        f.write(out)