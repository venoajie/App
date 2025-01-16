# -*- coding: utf-8 -*-

import time
import json
import asyncio
import httpx
from random import sample
from collections import deque

from messaging.telegram_bot import telegram_bot_sendtext

headers = ['Coin','Pings','Net Vol BTC','Net Vol %','Recent Total Vol BTC', 'Recent Vol %', 'Recent Net Vol', 'Datetime (UTC)']

starttime = time.time()

async def scanning_volume():
    """
    
    https://stackoverflow.com/questions/66686458/how-to-scrape-an-updating-html-table-using-selenium

    """
    
    print("Scanning volume")

    while True:
        async with httpx.AsyncClient(headers={'Connection': 'keep-alive'}) as client:
            
            response = await client.get('https://agile-cliffs-23967.herokuapp.com/ok')

            response_json = json.loads(response.text)['resu']
            
            rows = [str(i).split('|') for i in response_json[:-1]]
            result = []
            if rows:
                data_all = [dict(zip(headers, l)) for l in rows]
                
                for data in data_all:
                    data_was_in_result = [] if result == [] else [o for o in result if data in  result]
                    
                    print (f"data_was_in_result {data_was_in_result}")
                    
                    if data_was_in_result != []:
                        await telegram_bot_sendtext (
                        data,
                        "general_error"
                        )
                        result.append (data)
                        print (f"result {result}")

        random_sleep_time = max(
            sample(
                [5,10,15,20,30],
                1
                )
                                )

        await asyncio.sleep((random_sleep_time))
