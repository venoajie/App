# -*- coding: utf-8 -*-

import time
import json
import asyncio
import httpx
from random import sample

from messaging.telegram_bot import telegram_bot_sendtext

headers = ['Coin','Pings','Net Vol BTC','Net Vol %','Recent Total Vol BTC', 'Recent Vol %', 'Recent Net Vol', 'Datetime (UTC)']

starttime = time.time()

async def scanning_volume():
    """
    
    https://stackoverflow.com/questions/66686458/how-to-scrape-an-updating-html-table-using-selenium

    """

    while True:
        async with httpx.AsyncClient(headers={'Connection': 'keep-alive'}) as client:
            
            response = await client.get('https://agile-cliffs-23967.herokuapp.com/ok')

            response_json = json.loads(response.text)['resu']
            
            rows = [str(i).split('|') for i in response_json[:-1]]
            
            if rows:
                data = [dict(zip(headers, l)) for l in rows]
                await telegram_bot_sendtext (
                    data,
                    "general_error"
                    )

        random_sleep_time = sample([5,10,15,30],1)
        print (random_sleep_time)
        await asyncio.sleep((random_sleep_time))
