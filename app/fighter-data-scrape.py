from aiohttp import ClientSession, TCPConnector
import asyncio
from bs4 import BeautifulSoup
import pandas as pd
import re
from time import time


# FIGHTER STATS PARSER FUNCTIONS

def parse_soup(soup, fighter, slug):
    record_tags = soup.find_all('div', {'class':'c-record__promoted'})
    accuracy_tags = soup.find_all('dl', {'class':'c-overlap__stats'})
    stats_tags = soup.find_all('div', {'class': re.compile('c-stat-compare__group')})
    wins_tags = soup.find_all('div', {'class': 'c-stat-3bar__group'})
    meta_tags = soup.find_all('div', {'class': 'c-bio__field'})

    records = extract(record_tags, 'c-record__promoted-text', 'c-record__promoted-figure')
    accuracy = extract(accuracy_tags, 'c-overlap__stats-text', 'c-overlap__stats-value', 'accuracy')
    stats = extract(stats_tags, 'c-stat-compare__label', 'c-stat-compare__number', 'stats')
    wins = extract(wins_tags, 'c-stat-3bar__label', 'c-stat-3bar__value', 'wins')
    meta = extract(meta_tags, 'c-bio__label', 'c-bio__text')

    return_obj = {'Fighter':fighter, 'fighter-slug':slug}

    for obj in [records, accuracy, stats, wins, meta]:
        if obj is not None:
            return_obj.update(obj)

    return return_obj

def extract(tags, label, value, spec=None):
    if tags is []: return
    
    obj = {}
    for tag in tags:
        labels = tag.find_all('div', {'class':label}) if spec != 'accuracy' else tag.find_all('dt', {'class':label})
        values = tag.find_all('div', {'class':value}) if spec != 'accuracy' else tag.find_all('dd', {'class':value})
       
        if labels == []: return
        
        for idx in range(len(labels)):
            try:
                key, val = labels[idx].text.strip(), values[idx].text.strip()

                if '%' in val and spec == 'stats': val = float(val[0:2])/100
                if spec == 'wins': val = val.split(' ')[0]
                if key == '': continue

                obj[key] = val
            except:
                pass

    return obj



# DRIVER CODE FOR ASYNCHRONOUS REQUESTS

async def fetch(url, session, fighter, slug):
    async with session.get(url) as response:
        text = await response.text()
        soup = BeautifulSoup(text, 'html.parser')
        info = await get_fighter_data(soup, fighter, slug)
        return info

async def fetch_with_sem(url, session, sem, fighter, slug):
    async with sem:
        return await fetch(url, session, fighter, slug)

async def get_fighter_data(soup, fighter, slug):
    fighter_obj = parse_soup(soup, fighter, slug)
    print(fighter)
    return fighter_obj

async def main():
    tasks = []
    sem = asyncio.Semaphore(1000)

    df = pd.read_csv("./static/csvfiles/fighters.csv")
    df['slug'] = df['name'].apply(lambda n: n.lower().replace(' ','-'))

    base_url = "https://www.ufc.com/athlete/"

    async with ClientSession(connector=TCPConnector(ssl=False)) as session:
        for idx in range(len(df)):
            tasks.append(
                fetch_with_sem(
                    base_url + df.loc[idx, "slug"], 
                    session, 
                    sem,
                    df.iloc[idx]["name"],
                    df.iloc[idx]["slug"]
                )
            )

        records = await asyncio.gather(*tasks)
        return records



if __name__ == "__main__":
    start = time()
    records = asyncio.run(main())
    end = time()

    df = pd.DataFrame.from_records(records)
    df.to_csv('./static/csvfiles/stats.csv')

    print('Asynchronous html allocation and synchronous parsing took {} seconds'.format(end-start))

