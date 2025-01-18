# -*- coding: utf-8 -*-

import time
import json
import asyncio
import httpx
from random import sample
from collections import deque

from loguru import logger as log
from messaging.telegram_bot import telegram_bot_sendtext

headers = [
    "Coin",
    "Pings",
    "Net Vol BTC",
    "Net Vol %",
    "Recent Total Vol BTC",
    "Recent Vol %",
    "Recent Net Vol",
    "Datetime",#  (UTC)
]

starttime = time.time()


async def scanning_volume():
    """

    https://stackoverflow.com/questions/66686458/how-to-scrape-an-updating-html-table-using-selenium

    """

    print("Scanning volume")

    while True:
        async with httpx.AsyncClient(headers={"Connection": "keep-alive"}) as client:

            response = await client.get("https://agile-cliffs-23967.herokuapp.com/ok")

            response_json = json.loads(response.text)["resu"]

            rows = [str(i).split("|") for i in response_json[:-1]]

            if rows:

                data_all = [dict(zip(headers, l)) for l in rows]

                cached_data = []

                for single_data in data_all:

                    data_has_exist_before = (
                        []
                        if cached_data == []
                        else [
                            o
                            for o in cached_data
                            if int(o["Pings"]) == int(single_data["Pings"])
                            and single_data["Coin"] in o["Coin"]                         ]
                    )

                    if not data_has_exist_before:
                        cached_data.append(single_data)

                        await telegram_bot_sendtext(
                            f"""single_data - {single_data} data_has_exist_before {data_has_exist_before} single_data["Pings"] {single_data["Pings"]} single_data["Coin"] {single_data["Coin"]} cached ping {[
                            o ["Pings"]
                            for o in cached_data
                        ]} cached Coin {[
                            o ["Coin"]
                            for o in cached_data
                        ]}""", "general_error"
                        )

                        await telegram_bot_sendtext(
                            f"cached_data - {cached_data}", "general_error"
                        )

        random_sleep_time = max(sample([5, 10, 15, 20, 30], 1))

        await asyncio.sleep((random_sleep_time))
