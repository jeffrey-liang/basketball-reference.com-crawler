#!/usr/bin/env python
# -*- coding: utf-8 -*-

import asyncio
from collections import OrderedDict, deque
import aiohttp
import pandas as pd
from bs4 import BeautifulSoup, Comment
import datetime

root = 'http://www.basketball-reference.com'
links = pd.read_csv('player_urls.csv', index_col=0, header=None)


def format_links(root, links):
    links = ['{}{}'.format(root, row[1]) for _, row in links.iterrows()]
    return links


links = format_links(root, links)


def get_table_data(html, table_kind):
    '''
    Scrape the player stats table. Totals, and/or Advanced table.

    Parameters:
    -----------
    html: str
        The html of the basketball-reference.com player page.
    table_kind: list
        A list of the table kind to scrape. Totals or advanced.

    Returns:
    --------
    player_table_stats: pandas.core.frame.DataFrame
        The stat table.
    '''

    options = {'totals': ['id="div_totals"', 'totals'],
               'advanced': ['id="div_advanced"', 'advanced']}

    if not isinstance(table_kind, list):
        print('table_kind not a list')

    elif html is not None:
        player_table_stats = OrderedDict()

        bs_obj = BeautifulSoup(html, 'lxml')

        # Find all comments in source
        comments = bs_obj.find_all(string=lambda x: isinstance(x, Comment))

        for table_type in table_kind:

            # find table in comment
            for comment in comments:
                if options[table_type][0] in comment:
                    target_table = comment

            # Turn the comment into BS obj, then find the target table (i.e. totals
            # table)
            target_table = BeautifulSoup(target_table, 'lxml').find_all(
                'table', {'id': options[table_type][1]})

            columns = deque()

            # Get the columns of the table
            for cols in target_table[0].find_all('thead')[0].find_all('th'):
                columns.append(cols.get_text())

            # Data structure to store stats
            player_stats = OrderedDict()

            for row in target_table[0].find_all('tr', {'class': 'full_table'}):
                season = row.th.get_text()

                # Some players played for more than one team during season,
                # use TOT row. Conveniently, TOT row is first. So store TOT row,
                # and ignore the part seasons.
                # i.e
                # 01-02 TOT ...
                # 01-02 AAA ...
                # 01-02 BBB ...
                if season not in player_stats.keys():
                    season_stats = deque()

                    for col in row:
                        season_stats.append(col.get_text())

                    season = season_stats.popleft()
                    player_stats[season] = season_stats

                else:
                    pass

            # Store in pandas dataframe
            player_stats = pd.DataFrame.from_dict(player_stats, orient='index')

            # remove season header
            columns.popleft()
            player_stats.columns = columns

            player_table_stats[table_type] = player_stats

    return player_table_stats


async def fetch(url, client):
    headers = {'user-agent': 'Mozilla/5.0 (X11; Linux x86_64)\
            AppleWebKit/537.36 (KHTML, like Gecko)\
                    Chrome/55.0.2883.87 Safari/537.36'}

    print('Fetching: {}'.format(url))

    asyncio.sleep(3)
    async with client.get(url, headers=headers) as response:

        print('Starting: {} @ {}'.format(url, datetime.datetime.now()))
        if response.status == 404:
            print('The page: {} does not exists.'.format(url))
            pass

        else:
            return await response.text()


async def crawler(loop):

    responses = []

    sem = asyncio.Semaphore(15)
    async with sem, aiohttp.ClientSession(loop=loop) as client:
        for task in asyncio.as_completed([fetch(url, client) for url in links]):
            response = await task
            responses.append(response)

    return responses


def run():

    loop = asyncio.get_event_loop()

    responses = loop.run_until_complete(crawler(loop))
    loop.close()

    for resp in responses:
        # Acquire the table data
        print(get_table_data(resp, ['totals']))

run()
