from multiprocessing.connection import Client
from aiohttp import ClientSession, TCPConnector
import asyncio
from bs4 import BeautifulSoup
import pandas as pd
from time import time


# CUSTOM PARSERS FOR FIGHTER CARDS

def extract_info(tags):
    img = parse_tags(tags, 'img', 'image-style-teaser')
    nickName = parse_tags(tags, 'div', 'field field--name-nickname field--type-string field--label-hidden')
    name = parse_tags(tags, 'span', 'c-listing-athlete__name')
    weightClass = parse_tags(tags, 'div', 'field field--name-stats-weight-class field--type-entity-reference field--label-hidden field__items')
    record = parse_tags(tags, 'span', 'c-listing-athlete__record')

    return {'img': img, 'nickName': clean(nickName), 'name': clean(name), 'weightClass': clean(weightClass), 'record': record}

def parse_tags(ex, tag, attr_name):
    try:
        if tag == 'img':
            text = ex.find(tag, attrs={'class': attr_name})['src']
        else:
            text = ex.find(tag, attrs={'class': attr_name}).text
    except:
        text = ''
    return text


def clean(arg):
    return arg.strip().replace('"', '').replace("'", '')



# DRIVER CODE FOR ASYNCHRONOUS REQUESTS

async def fetch(url, session):
    async with session.get(url) as response:
        text = await response.text()
        soup = BeautifulSoup(text, 'html.parser')
        info = await get_fighter_cards(soup)
        return info

async def fetch_with_sem(url, session, sem):
    async with sem:
        return await fetch(url, session)

async def get_fighter_cards(soup):
    fighter_info = []
    fighter_cards = soup.find_all('div', attrs={'class': 'c-listing-athlete-flipcard__front'})
    for fighter in fighter_cards:
        fighter_info.append(extract_info(fighter))
    return fighter_info

# ENTRYPOINT
async def main():
    tasks = []
    sem = asyncio.Semaphore(10)

    async with ClientSession(connector=TCPConnector(ssl=False)) as session:

        # HACKY WAY TO GET NUMBER OF PAGES ROUGHLY NEEDED TO SEND REQUESTS TO,
        # SCRAPING ATHLETE COUNT ELEMENTS THEN DIVIDING BY THE NUMBER OF ATHLETES SHOWN ON EACH PAGE
        async with session.get('https://www.ufc.com/athletes/all?gender=All&page=0') as response:
            text = await response.text()
            soup = BeautifulSoup(text, 'html.parser')
            numOfFighters = soup.find('div', attrs={'class': "althelete-total"}).text.split(' ')[0]
            MAX_PAGES = int(numOfFighters) // 11

        for i in range(MAX_PAGES):
            url = f'https://www.ufc.com/athletes/all?gender=All&page={i}'
            tasks.append(
                fetch_with_sem(url, session, sem)
            )
        
        records = await asyncio.gather(*tasks)
        return records


# START OF SCRIPT
if __name__ == "__main__":
    start = time()
    pages = asyncio.run(main())
    end = time()

    records = []

    for page in pages:
        for fighter in page:
            records.append(fighter)

    df = pd.DataFrame.from_records(records)
    df.to_csv('fighters.csv')

    print('Asynchronous html allocation and synchronous parsing took {} seconds'.format(end-start))

