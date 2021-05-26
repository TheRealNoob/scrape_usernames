import logging
import random
import time
import unicodedata
from math import floor
from statistics import median

import requests
from bs4 import BeautifulSoup

from inputs import game_mode_categories, user_agents

# setup logging
logger = logging.getLogger()
handler = logging.FileHandler('page_count.log')
formatter = logging.Formatter('%(asctime)s   %(levelname)-8s   %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel('INFO')


def scrape_ranks(html):
    """
    returns the list of ranks found in the html
    """
    soup = BeautifulSoup(html, 'html.parser')
    content = soup.find('div', {'id': 'contentHiscores'})
    if content is not None: # even if table is blank content will still exist
        rows = content.find('tbody').find_all('tr', {'class': 'personal-hiscores__row'})
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


def request_webpage(header, params, game_mode):
    response = requests.get(url=f'https://secure.runescape.com/m={game_mode}/overall', headers=header, params=params)

    if response.status_code == 200:
        if response.history == []:
            time.sleep(10)
            return response
    elif response.status_code == 504:
        logger.debug(f'504 response from {response.url}')
        time.sleep(10)
        return request_webpage(header, params, game_mode)
    else:    
        logger.warning(f'abnormal response.  status {response.status_code} redirects {response.history} body {response.text}')
        time.sleep(10)
        return request_webpage(header, params, game_mode)


def get_page_count(game_mode, params):
    """
    return the number of pages in a hiscores category
    """
    
    header = {"user-agent": random.choice(user_agents)}
    params['page'] = 1
    response = request_webpage(header, params, game_mode)
    if is_page_one(response.text):
        # if page 1 does not have names return "dont search this category"
        if len(scrape_ranks(response.text)) == 0:
            return 0
        # if page 1 has names but is the only page
        if not contains_PageDown_button(response.text):
            return 1
    
    # if page 80,000 exists return 80,000
    header = {"user-agent": random.choice(user_agents)}
    params['page'] = 80000
    response = request_webpage(header, params, game_mode)
    if not is_page_one(response.text):
        return 80000

    previous_pages = (1, 80000)
    while True:
        # find the middle number between the ints in previous_pages, rounded down
        page = floor(median(previous_pages))
        logger.debug(f'trying page {page}')

        header = {"user-agent": random.choice(user_agents)}
        params['page'] = page
        response = request_webpage(header, params, game_mode)


        if is_page_one(response.text):
            if contains_PageDown_button(response.text):
                previous_pages = (previous_pages[0], page)
                logger.debug(f'less than {page}')
        else:
            if contains_PageDown_button(response.text):
                previous_pages = (page, previous_pages[1])
                logger.debug(f'more than {page}')
            else:
                logger.debug(f'found final page {page}')
                return str(page)

        time.sleep(10)
    
#########

results = {
    game_mode: {
        category: "" for category in game_mode_categories[game_mode]
     } for game_mode in game_mode_categories
}


for game_mode in game_mode_categories: # Main
    for category in game_mode_categories[game_mode]: # overall
        _game_mode = game_mode_categories[game_mode][category]['game_mode']
        _params = game_mode_categories[game_mode][category]['params']
        logger.debug(f'starting search for {game_mode} / {category}')
        page_count = get_page_count(_game_mode, _params)
        logger.info(f'found {page_count} pages for {game_mode} / {category}')
        
        results[game_mode][category] = page_count
        
print(results)

