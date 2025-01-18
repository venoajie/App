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
    "Datetime (UTC)",
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

            result = []
            if rows:

                data_all = [dict(zip(headers, l)) for l in rows]

                for single_data in data_all:

                    log.warning(f"single_data {single_data}")

                    if not result:
                        data = [
                            o
                            for o in result
                            if o["datetime"] == single_data["datetime"]
                        ]

                    else:
                        data = data_all

                    log.debug(f"data {data}")

                    if not data:
                        result.append(single_data)

                        log.debug(f"result {result}")

                        await telegram_bot_sendtext(f"data - {data}", "general_error")

        random_sleep_time = max(sample([5, 10, 15, 20, 30], 1))

        await asyncio.sleep((random_sleep_time))
