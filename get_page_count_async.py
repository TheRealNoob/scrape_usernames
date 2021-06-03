import asyncio
import logging
import pickle
import random
import unicodedata
from math import floor
from statistics import median

import aiohttp
from bs4 import BeautifulSoup
from requests import Request

from inputs import game_mode_categories, user_agents

# setup logging
logger = logging.getLogger()
handler = logging.FileHandler('page_count.log')
formatter = logging.Formatter('%(asctime)s   %(levelname)-8s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel('DEBUG')

logger.info('Starting')

def scrape_ranks(html):
    """
    returns the list of ranks found in the html
    """
    soup = BeautifulSoup(html, 'html.parser')
    content = soup.find('div', {'id': 'contentHiscores'})
    if content is not None: # even if table is blank content will still exist
        rows = content.find('tbody').select('tr[class*="personal-hiscores__row"]')
        ranks = [row.find('td', {'class': 'right'}).string for row in rows]
        # the ranks come back containing newline chars.  example '\n1\n'
        # the normalize doesn't really do anything in this case, 
        #   but it was necessary in others so i'll keep it for now
        ranks = [unicodedata.normalize('NFKC', rank) for rank in ranks]
        ranks = [rank.replace("\n", "") for rank in ranks]
        return ranks
    return []


def is_page_one(html):
    """
    returns True if html is page 1 of the hiscores
    The hiscores don't redirect you when you enter an invalid page number # STUPID!!!
    it just feeds you the results from page 1, so function tells you if you're on page 1
    """

    # there is users on the hiscores
    if len(scrape_ranks(html)) > 0:
        # the first <td> is rank 1
        if scrape_ranks(html)[0] == '1':
            return True
    else:
        # TODO do we need to account for blank hiscores pages ?
        # it seems like it's not necessary
        return False


def contains_PageDown_button(html):
    """
    returns bool if page contains the PageDown button
    """
    soup = BeautifulSoup(html, 'html.parser')
    if soup.find('a', {'class': 'personal-hiscores__pagination-arrow personal-hiscores__pagination-arrow--down'}):
        return True
    else:
        return False


async def request_webpage(session, proxy, params, game_mode, worker_name):
    header = {"user-agent": random.choice(user_agents)}
    async with session.get(url=f'https://secure.runescape.com/m={game_mode}/overall', headers=header, params=params, proxy=proxy) as response:
        if response.status == 200:
            if response.history == ():
                await asyncio.sleep(6)
                return await response.text()
            else:
                logger.error(f'{worker_name}: 200 but redirect??')
                logger.error(f'{worker_name}: response status: {response.status}')
                logger.error(f'{worker_name}: request url: {response.url}')
                logger.error(f'{worker_name}: redirect history: {response.history}')
                logger.error(f'{worker_name}: response body: {await response.text()}')
                return await request_webpage(session, proxy, params, game_mode, worker_name)
        elif response.status == 504:
            logger.debug(f'{worker_name}: 504 response from {response.url}')
            await asyncio.sleep(6)
            return await request_webpage(session, proxy, params, game_mode, worker_name)
        else:    
            logger.warning(f'{worker_name}: abnormal response.  status {response.status_code} redirects {response.history} body {response.text}')
            await asyncio.sleep(6)
            return await request_webpage(session, proxy, params, game_mode, worker_name)


async def create_worker(session, proxy, worker_name, game_mode, params):
    """
    creates the task which finds out how many pages are in a hiscores category
    and appends a list of every valid URL for that category
    """
    logger.debug(f'{worker_name}: worker starting with proxy {proxy} searching {game_mode} / {params}')
    global pages_to_scrape
    # we start with a one-time check to make sure:
    # 1. if page 1 is blank return 0
    # 2. if page 1 has names but no page 2 return 1
    # 3. page 80,000 exists return 80000
    params['page'] = 1
    response_text = await request_webpage(session, proxy, params, game_mode, worker_name)

    if is_page_one(response_text):
        # if page 1 does not have names then kill this worker
        if len(scrape_ranks(response_text)) == 0:
            logger.info(f'{worker_name}: page 1 does not have any usernames.  killing worker')
            logger.debug(f'{worker_name}: worker exiting')
            return 0 # TODO kill worker
        # if page 1 has names but is the only page
        if not contains_PageDown_button(response_text):
            logger.info(f'{worker_name}: page 1 found')
            pages_to_scrape.append(Request('GET', f'https://secure.runescape.com/m={game_mode}/overall', params=params).prepare().url)
            logger.debug(f'{worker_name}: worker exiting')
            return 1 # TODO kill worker
    
    # if page 80,000 exists return 80,000
    params['page'] = 80000
    response_text = await request_webpage(session, proxy, params, game_mode, worker_name)
    if not is_page_one(response_text):
        # save URLs to pages_to_scrape
        # i've found that Request() takes 30s to prepare 80,000 URLs
        # and since it's blocking i'm going to just Request() once and do the rest myself
        logger.info(f'{worker_name}: page 80,000 found')
        page_eightythousand_url = Request('GET', f'https://secure.runescape.com/m={game_mode}/overall', params=params).prepare().url
        for page in range(1, 80000 + 1):
            url = page_eightythousand_url.replace("page=80000", f"page={page}")
            pages_to_scrape.append(url)
        logger.info(f'{worker_name}: appended urls to pages_to_scrape.  new length: {len(pages_to_scrape)}.  last entry: {pages_to_scrape[-1]}')
        logger.debug(f'{worker_name}: worker exiting')
        return 0 # TODO kill worker

    # there is somewhere between 2 and 79,999 pages
    previous_pages = (1, 80000)
    while True:
        # find the middle number between the ints in previous_pages, rounded down
        page = floor(median(previous_pages))
        logger.debug(f'{worker_name}: trying page {page}')

        params['page'] = page
        response_text = await request_webpage(session, proxy, params, game_mode, worker_name)

        if is_page_one(response_text):
            if contains_PageDown_button(response_text):
                previous_pages = (previous_pages[0], page)
                logger.debug(f'{worker_name}: less than {page}')
        else:
            if contains_PageDown_button(response_text):
                previous_pages = (page, previous_pages[1])
                logger.debug(f'{worker_name}: more than {page}')
            else:
                logger.info(f'{worker_name}: found final page {page}')
                # save URL to pages_to_scrape
                # i've found that Request() takes 30s to prepare 80,000 URLs
                # and since it's blocking i'm going to just Request() once and do the rest myself
                page_eightythousand = params
                page_eightythousand['page'] = 80000
                page_eightythousand_url = Request('GET', f'https://secure.runescape.com/m={game_mode}/overall', params=page_eightythousand).prepare().url
                for page in range(1, page + 1):
                    url = page_eightythousand_url.replace("page=80000", f"page={page}")
                    pages_to_scrape.append(url)
                logger.info(f'{worker_name}: pages_to_scrape length: {len(pages_to_scrape)}.  last entry: {pages_to_scrape[-1]}')
                logger.debug(f'{worker_name}: worker exiting')
                return 0 # TODO kill worker
    
#########

async def main():
    # define a list of all the game_modes and params that we need to search for
    list_of_gamemode_params = []
    for game_mode in game_mode_categories: # Main
        for category in game_mode_categories[game_mode]: # overall
            # create a list of game_modes and categories
            list_of_gamemode_params.append({
                'game_mode': game_mode_categories[game_mode][category]['game_mode'],
                'params': game_mode_categories[game_mode][category]['params']
        })

    async with aiohttp.ClientSession() as session:
        # get proxy list
        proxies = ["http://erwvnich-dest:s8ix5905h935@209.127.191.180:9279",
                   "http://erwvnich-dest:s8ix5905h935@45.95.96.132:8691",
                   "http://erwvnich-dest:s8ix5905h935@45.95.96.187:8746",
                   "http://erwvnich-dest:s8ix5905h935@45.95.96.237:8796",
                   "http://erwvnich-dest:s8ix5905h935@45.136.228.154:6209",
                   "http://erwvnich-dest:s8ix5905h935@45.94.47.66:8110",
                   "http://erwvnich-dest:s8ix5905h935@45.94.47.108:8152",
                   "http://erwvnich-dest:s8ix5905h935@193.8.56.119:9183",
                   "http://erwvnich-dest:s8ix5905h935@45.95.99.226:7786",
                   "http://erwvnich-dest:s8ix5905h935@45.95.99.20:7580", ]  # placeholder

        while len(list_of_gamemode_params) > 0:
            # create a mapping of proxies to inputs
            # this is necessary because we could have less proxies than categories to search
            #
            # creates:
            # [
            #   ('proxy_one', {'game_mode': 'hiscore_oldschool', 'params': {'table': 0}}), 
            #   ('proxy_two', {'game_mode': 'hiscore_oldschool', 'params': {'table': 1}}), 
            #   ('proxy_three', {'game_mode': 'hiscore_oldschool', 'params': {'table': 2}})
            # ]
            working_list = list(zip(proxies, list_of_gamemode_params))

            # lets change the object format to:
            # ({'proxy': 'http://proxy', 'game_mode': 'hiscore_oldschool', 'params': {'table': 0}})
            working_list = [{'proxy': item[0],'game_mode': item[1]['game_mode'],'params': item[1]['params']} for item in working_list]

            # remove our current working items from the list to prevent duplicates
            del list_of_gamemode_params[0:len(working_list)]

            # run workers
            logger.debug(f'starting a new batch of {len(working_list)} hiscores categories')
            workers = [asyncio.create_task(create_worker(session=session, proxy=value['proxy'], worker_name=f'worker_{count}', game_mode=value['game_mode'], params=value['params'])) for count, value in enumerate(working_list)]            
            await asyncio.gather(*workers)


# holds output
pages_to_scrape = []

asyncio.run(main())
logger.error(pages_to_scrape)
print(pages_to_scrape)

# TODO never used pickle.  does this work?
with open('outfile', 'wb') as file_handle:
    pickle.dump(pages_to_scrape, file_handle)

print('block')
