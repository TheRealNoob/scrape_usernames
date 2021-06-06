import asyncio
import logging
import os
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
formatter = logging.Formatter('%(asctime)s - %(levelname)-8s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

logger.info('Starting')

async def get_proxy_list(session):
    """
    returns the proxy list from webshare.io
    output format: ['http://user:pass@ip:port', 'http://user:pass@ip:port', ...]
    """
    logger.info('fetching proxy list from webshare.io')
    async with session.get(os.getenv('PROXY_DOWNLOAD_URL')) as response:
        if response.status == 200:
            proxies = [proxy.split(':') for proxy in (await response.text()).splitlines()]
            proxies = [
                f'http://{proxy[2]}:{proxy[3]}@{proxy[0]}:{proxy[1]}' for proxy in proxies]
            logging.info(f'{len(proxies)} proxies fetched')
            return proxies
        else:
            logging.error('error fetching proxy list')
            logging.error(f'response status: {response.status}')
            logging.error(f'response body: {await response.text()}')
            # pretty sure that by returning a blank list it'll cause the zip() to throw IndexError
            # problem for another day if it ever happens
            return []

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
    await asyncio.sleep(6)
    
    header = {"user-agent": random.choice(user_agents)}
    async with session.get(url=f'https://secure.runescape.com/m={game_mode}/overall', headers=header, params=params, proxy=proxy) as response:
        if response.status == 200:
            # request was flagged as a robot
            if '<META NAME="ROBOTS" CONTENT="NOINDEX, NOFOLLOW">' in await response.text():
                logger.error(f'{worker_name}: header detected as robot: {header["user-agent"]}')
                return await request_webpage(session, proxy, params, game_mode, worker_name)
            # request was redirected
            if response.history != ():
                logger.error(f'{worker_name}: request was redirected')
                logger.error(f'{worker_name}: request url: {response.url}')
                logger.error(f'{worker_name}: redirect history: {response.history}')
                logger.error(f'{worker_name}: response body: {await response.text()}')
                return await request_webpage(session, proxy, params, game_mode, worker_name)
            # happy path
            else:
                return await response.text()
        elif response.status == 502:
            # proxy errored
            logger.error(f'{worker_name}: 502 Bad Gateway response from proxy: {proxy}')
            return await request_webpage(session, proxy, params, game_mode, worker_name)
        elif response.status == 504:
            # hit the HiScores rate limiter
            logger.debug(f'{worker_name}: 504 Gateway Timeout from HiScores')
            return await request_webpage(session, proxy, params, game_mode, worker_name)
        else:
            # unexpected error
            logger.error(f'{worker_name}: unhandled response.  status: {response.status_code} redirect history: {response.history} body: {response.text}')
            return await request_webpage(session, proxy, params, game_mode, worker_name)


async def create_worker(session, proxy, worker_name, game_mode, category):
    """
    creates the task which finds out how many pages are in a hiscores category
    and appends a list of every valid URL for that category
    """
    logger.debug(f'{worker_name}: worker starting with proxy {proxy} searching {game_mode} / {category}')
    global pages_to_scrape
    _game_mode = game_mode_categories[game_mode][category]['game_mode']
    _params = game_mode_categories[game_mode][category]['params']


    # we start with a one-time check to speed the search up
    # 1. if there are no ranked players
    # 2. if page 1 has names but no page 2
    # 3. page 80,000 exists (2m players)

    
    _params['page'] = 1
    response_text = await request_webpage(session, proxy, _params, _game_mode, worker_name)
    # if the game mode / category has no ranked players
    if len(scrape_ranks(response_text)) == 0:
        _params['page'] = 0
        logger.info(f'{worker_name}: 0 pages in {game_mode} / {category}')
        logger.debug(f'{worker_name}: worker exiting')
        return 0
    # if page 1 has players but there is no page 2
    elif not contains_PageDown_button(response_text):
        _params['page'] = 1
        logger.info(f'{worker_name}: 1 pages in {game_mode} / {category}')
        logger.debug(f'{worker_name}: worker exiting')
        return 1
    

    _params['page'] = 80000
    response_text = await request_webpage(session, proxy, _params, _game_mode, worker_name)
    if game_mode == "Tournament" and category == 'Agility':
        logger.debug(f'{worker_name}: Inside Tournament / Agility debugging')
        logger.debug(f'{worker_name}: {response_text}') # TODO remove for debugging
    # page 80,000 exists
    if not is_page_one(response_text):
        logger.info(f'{worker_name}: 80000 pages in {game_mode} / {category}')
        logger.debug(f'{worker_name}: worker exiting')
        return 80000


    # there is somewhere between 2 and 79,999 pages
    previous_pages = (1, 80000)
    while True:
        # find the middle number between the values in previous_pages, rounded down
        page = floor(median(previous_pages))
        logger.debug(f'{worker_name}: trying page {page}')

        _params['page'] = page
        response_text = await request_webpage(session, proxy, _params, _game_mode, worker_name)

        # the page we requested doesn't exist
        if is_page_one(response_text):
            previous_pages = (previous_pages[0], page)
            logger.debug(f'{worker_name}: less than {page} pages')
        # requested page does exist
        else:
            # there is more pages
            if contains_PageDown_button(response_text):
                previous_pages = (page, previous_pages[1])
                logger.debug(f'{worker_name}: more than {page} pages')
            # this is the last page
            else:
                logger.info(f'{worker_name}: {_params["page"]} pages in {game_mode} / {category}')
                logger.debug(f'{worker_name}: worker exiting')
                return page
    


async def main():
    # define a list of all the game_modes and params that we need to search for
    list_of_gamemode_params = []
    for game_mode in game_mode_categories: # Main
        for category in game_mode_categories[game_mode]: # overall
            # create a list of game_modes and categories
            list_of_gamemode_params.append({
                'game_mode': game_mode,
                'params': category
        })

    async with aiohttp.ClientSession() as session:
        # get proxy list
        proxies = await get_proxy_list(session)
        proxies = proxies[0:2] # temporary debugging

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
            workers = [asyncio.create_task(create_worker(session=session, proxy=value['proxy'], worker_name=f'worker_{count}', game_mode=value['game_mode'], category=value['params'])) for count, value in enumerate(working_list)]            
            await asyncio.gather(*workers)


# holds output
pages_to_scrape = []

asyncio.run(main())

logger.info('Exiting')
