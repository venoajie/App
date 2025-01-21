# -*- coding: utf-8 -*-

import time
import orjson
import asyncio
import httpx
from random import sample

from messaging.telegram_bot import telegram_bot_sendtext

headers = [
    "coin",
    "pings",
    "net_vol_btc",
    "net_vol_pct",
    "recent_total_vol_btc",
    "recent_vol_pct",
    "recent_net_vol",
    "datetime",#  (UTC)
]

starttime = time.time()


async def scanning_volume():
    """

    https://stackoverflow.com/questions/66686458/how-to-scrape-an-updating-html-table-using-selenium
    https://medium.com/@fyattani/caching-in-python-261564d64a4e

    """

    print("Scanning volume")
    
    cached_data = []

    while True:
        async with httpx.AsyncClient(headers={"Connection": "keep-alive"}) as client:

            response = await client.get("https://agile-cliffs-23967.herokuapp.com/ok")
            
            print(f"response {response}")

            response_json = orjson.loads(response.text)["resu"]

            #rows = [str(i).split("|") for i in response_json[:-1]]
            rows = [str((i.replace('%', ''))).split("|") for i in response_json[:-1]]
    
            if rows:

                data_all = [dict(zip(headers, l)) for l in rows]

                for single_data in data_all:

                    data_has_exist_before = (
                        []
                        if cached_data == []
                        else [
                            o
                            for o in cached_data
                            if int(o["pings"]) == int(single_data["pings"])
                            and single_data["coin"] in o["coin"]
                            ]
                    )

                    if data_has_exist_before == []:
                        single_data.update({"counter": int(response_json[1])})
                        cached_data.append(single_data)

                        await telegram_bot_sendtext(
                            f"""{single_data}""", 
                            "general_error"
                        )

        random_sleep_time = max(sample([5, 10, 15, 20, 30], 
                                       1))

        await asyncio.sleep((random_sleep_time))
