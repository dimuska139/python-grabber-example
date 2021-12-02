import asyncio
import dataclasses
import json
import os
import re
from dataclasses import dataclass
from typing import List

import aiohttp
import tqdm
from bs4 import BeautifulSoup
from dataclasses_json import dataclass_json
from fake_useragent import UserAgent
from pydantic import BaseModel
from termcolor import colored

ua = UserAgent()

URL_PREFIX = 'https://www.keramogranit.ru'
CONCURRENCY = 5
sem = asyncio.Semaphore(CONCURRENCY)


class Product(BaseModel):
    url: str
    name: str
    description: str
    image_url: str
    properties: dict
    price: int
    units: str


class Collection(BaseModel):
    url: str
    name: str
    description: str
    properties: dict
    images_url: List[str]
    products: List[Product]


class Brand(BaseModel):
    url: str
    name: str
    country: str
    site: str
    description: str
    image_url: str
    collections: List[Collection]


async def fetch(session: aiohttp.ClientSession, url: str, attempt: int = 0):
    headers = {
        'User-Agent': str(ua.chrome)
    }
    try:
        async with sem:
            async with session.get(url, headers=headers) as resp:
                body = await resp.text()
                return body
    except Exception as e:
        if attempt < 5:
            attempt += 1
            await asyncio.sleep(attempt)
            return await fetch(session, url, attempt)
        raise e


async def get_collections(session: aiohttp.ClientSession, url: str):
    brand_page = await fetch(session, url)
    soup = BeautifulSoup(brand_page, 'html5lib')
    return soup.select('.cat-list .cat-card')


async def get_items(session: aiohttp.ClientSession, url: str):
    brand_page = await fetch(session, url)
    soup = BeautifulSoup(brand_page, 'html5lib')
    return soup.select('.cat-list .cat-card[itemtype="http://schema.org/Product"]')


async def process_product(session: aiohttp.ClientSession, url: str, name: str) -> Product:
    collection_page = await fetch(session, url)
    soup = BeautifulSoup(collection_page, 'html5lib')
    description = str(soup.select_one('.static-text p'))[3:-4].strip()
    params = {}
    for i in zip(soup.select('.cat-article-params dt'), soup.select('.cat-article-params dd')):
        params[i[0].text.strip()] = i[1].text.strip()

    #if len(soup.select_one('.cat-price__cur').text.replace(" ", "")) == 0:
    #    print(url)
    #    quit()
    price = int(soup.select_one('.cat-price__cur').text.replace(" ", ""))
    units = soup.select_one('.cat-price__measure').text
    img_url = URL_PREFIX + soup.select_one('.cat-article-desc__image  img').attrs['src']
    return Product(
        url=url,
        name=name,
        description=description,
        image_url=img_url,
        properties=params,
        price=price,
        units=units,
    )


async def process_collection(session: aiohttp.ClientSession, url: str, name: str) -> Collection:
    collection_page = await fetch(session, url)
    soup = BeautifulSoup(collection_page, 'html5lib')
    description = str(soup.select_one('.article-text p'))[3:-4].strip()
    params = {}
    for i in zip(soup.select('.cat-article-params dt'), soup.select('.cat-article-params dd')):
        params[i[0].text.strip()] = i[1].text.strip()

    images = [URL_PREFIX + i.attrs['data-full'] for i in soup.select('.gallery__thumbs li') if 'data-full' in i.attrs]
    if len(images) == 0:
        images = [URL_PREFIX + soup.select_one('.gallery__port__img img').attrs['src']]
    pager_links = soup.select('a.pager__link')
    max_page_num = 1
    if pager_links:
        max_page_num = int(pager_links[len(pager_links) - 1].text.strip())

    products_cards = soup.select('.cat-list .cat-card[itemtype="http://schema.org/Product"]')
    if max_page_num > 1:
        print(url)
        tasks = []
        for p in [i + 1 for i in range(max_page_num) if i != 0]:
            task = asyncio.ensure_future(get_items(session, url + '?p=' + str(p)))
            tasks.append(task)
        results = await asyncio.gather(*tasks)
        for r in results:
            products_cards += r

    tasks = []
    for card in products_cards:
        if not card.select_one('.cat-card__price'):  # Если нет цены - значит, в архиве
            continue

        coll_link = card.select_one('.cat-card__title-link')
        coll_name = re.sub(' +', ' ', coll_link.text.strip())
        coll_link = URL_PREFIX + coll_link.attrs['href']
        task = asyncio.ensure_future(process_product(session, coll_link, coll_name))
        tasks.append(task)
    products = await asyncio.gather(*tasks)
    return Collection(
        url=url,
        name=name,
        description=description,
        properties=params,
        images_url=images,
        products=products,
    )


async def process_brand(session, url: str) -> Brand:
    brand_page = await fetch(session, url)
    soup = BeautifulSoup(brand_page, 'html5lib')
    image_url = URL_PREFIX + soup.select_one('.vendor-desc__image img').attrs['src']
    brand_name = None

    brands_links = soup.select('.top-vendors a')
    for i in brands_links:
        if URL_PREFIX + i.attrs['href'] == url:
            brand_name = i.text
            break
    country = None
    site = None
    for i in zip(soup.select('.vendor-desc__params dt'), soup.select('.vendor-desc__params dd')):
        if i[0].text.strip() == 'Страна':
            country = i[1].select_one('a').text.strip()
        if i[0].text.strip() == 'Официальный сайт':
            site = i[1].select_one('a').text.strip()
    description = str(soup.select_one('.article-text p'))[3:-4].strip()

    pager_links = soup.select('a.pager__link')
    max_page_num = 1
    if pager_links:
        max_page_num = int(pager_links[len(pager_links) - 1].text.strip())

    collection_cards = soup.select('.cat-list .cat-card')
    if max_page_num > 1:
        tasks = []
        for p in [i + 1 for i in range(max_page_num) if i != 0]:
            task = asyncio.ensure_future(get_collections(session, url + '?p=' + str(p)))
            tasks.append(task)
        results = await asyncio.gather(*tasks)
        for r in results:
            collection_cards += r
    tasks = []
    for card in collection_cards:
        coll_link = card.select_one('.cat-card__title-link')
        coll_name = re.sub(' +', ' ', coll_link.text.strip())
        coll_link = URL_PREFIX + coll_link.attrs['href']
        task = asyncio.ensure_future(process_collection(session, coll_link, coll_name))
        tasks.append(task)

    print(colored('%s...' % brand_name, 'green'))
    results = [await f for f in tqdm.tqdm(asyncio.as_completed(tasks), total=len(tasks))]

    return Brand(
        url=url,
        name=brand_name,
        country=country,
        site=site,
        description=description,
        image_url=image_url,
        collections=results,
    )


async def run():
    connector = aiohttp.TCPConnector(ssl=False)
    timeout = aiohttp.ClientTimeout(total=20)
    async with aiohttp.ClientSession(connector=connector, raise_for_status=True, timeout=timeout) as session:
        links = [
            'https://www.keramogranit.ru/brands/ragno/',
            'https://www.keramogranit.ru/brands/fabresa/',
            'https://www.keramogranit.ru/brands/decocer/',
            'https://www.keramogranit.ru/brands/kerranova/',
        ]
        for l in links:
            brand = await process_brand(session, l)
            filename = "./results/%s.json" % brand.name.lower()
            os.makedirs(os.path.dirname(filename), exist_ok=True)
            with open(filename, "w") as f:
                f.write(brand.json(ensure_ascii=False, indent=4))


if __name__ == "__main__":
    loop = asyncio.get_event_loop()

    loop.run_until_complete(run())
    loop.run_until_complete(asyncio.sleep(0))
    loop.close()
