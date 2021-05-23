import requests
from bs4 import BeautifulSoup
from inputs import game_modes, categories
from user_agents import USER_AGENTS
import random

"""
for game_mode in game_modes:
    for category in categories:
        url = f'https://secure.runescape.com/m={game_mode}/{category}'
"""

user_agent = {'User-Agent': random.choice(USER_AGENTS)}
response = requests.get('https://secure.runescape.com/m=hiscore_oldschool/overall?category_type=1&table=3&page=2', headers=user_agent)
soup = BeautifulSoup(response.text, 'html.parser')

print(soup.prettify())
