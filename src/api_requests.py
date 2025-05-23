"""
why aiohttp over httpx?
    - Our module is fully using asynchronous which is aiohttp spesialization
    - has more mature asyncio support than httpx
    - aiohttp is more suitable for applications that require high concurrency and low latency, such as web scraping or real-time data processing.

references:
    - https://github.com/encode/httpx/issues/3215#issuecomment-2157885121
    - https://github.com/encode/httpx/discussions/3100
    - https://brightdata.com/blog/web-data/requests-vs-httpx-vs-aiohttp


"""

# built ins
import asyncio
from typing import Dict

# installed
import uvloop
import redis.asyncio as aioredis

# from redistimeseries.client import Client

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

# installed
from loguru import logger as log

# user defined formula

from streaming_helper.restful_api.telegram import (
    end_point_params_template as end_point_telegram,
)
from streaming_helper.restful_api import connector
from streaming_helper.utilities import error_handling, string_modification as str_mod

from streaming_helper.restful_api.deribit import (
    end_point_params_template as end_point_deribit,
)
from configuration import config, config_oci


async def main():

    # registering strategy config file
    file_toml = "config_strategies.toml"

    exchange = "deribit"

    try:

        pool = aioredis.ConnectionPool.from_url(
            "redis://localhost",
            port=6379,
            db=0,
            protocol=3,
            encoding="utf-8",
            decode_responses=True,
        )

        client_redis: object = aioredis.Redis.from_pool(pool)

        connection_url_telegram = end_point_telegram.basic_https()

        client_id: str = "1297409216:AAEYu9r7FNd_GQWnxQdM-K6PUSYSQsKuBgE"
        client_secret: str = "-439743060"
        params: str = "Tes"

        await connector.get_connected(
            connection_url_telegram,
            None,
            client_id,
            client_secret,
            params,
        )

        connection_url_telegram = end_point_telegram.basic_https()

        basic_https_connection_url = end_point_deribit.basic_https()

        endpoint_tickers = end_point_deribit.get_tickers_end_point("BTC-PERPETUAL")

        result_instrument = await connector.get_connected(
            basic_https_connection_url,
            endpoint_tickers,
        )

        print(result_instruments)

    except Exception as error:

        await error_handling.parse_error_message_with_redis(
            client_redis,
            error,
        )


if __name__ == "__main__":

    try:

        uvloop.run(main())

    except (KeyboardInterrupt, SystemExit):

        asyncio.get_event_loop().run_until_complete(main())

    except Exception as error:

        error_handling.parse_error_message(error)
