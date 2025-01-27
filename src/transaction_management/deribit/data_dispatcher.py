#!/usr/bin/python3
# -*- coding: utf-8 -*-

# built ins
import asyncio
import json
import os
from collections import deque
from datetime import datetime, timedelta, timezone
import sys

import numpy as np
import orjson
import redis
import tomli
import uvloop
import websockets
from redis import ConnectionPool

# installed
from dataclassy import dataclass, fields
from loguru import logger as log

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

# user defined formula
from configuration import config, config_oci, id_numbering
from configuration.label_numbering import get_now_unix_time

from market_understanding.price_action.candles_analysis import (
    combining_candles_data,
    get_market_condition,
)
from messaging.telegram_bot import telegram_bot_sendtext
from transaction_management.deribit.api_requests import (
    SendApiRequest,
    get_currencies,
    get_end_point_result,
    get_instruments,
)
from transaction_management.deribit.get_instrument_summary import (
    get_futures_instruments,
)
from transaction_management.deribit.managing_deribit import (
    ModifyOrderDb,
    currency_inline_with_database_address,
)

from utilities.caching import (
    combining_ticker_data as cached_ticker,
    combining_order_data,
    update_cached_orders,
    update_cached_ticker,
)
from utilities.pickling import replace_data
from utilities.string_modification import (
    extract_currency_from_text,
    remove_double_brackets_in_list,
    remove_redundant_elements,
)
from utilities.system_tools import parse_error_message, provide_path_for_file


def parse_dotenv(sub_account) -> dict:
    return config.main_dotenv(sub_account)


def get_config(file_name: str) -> list:
    """ """
    config_path = provide_path_for_file(file_name)

    try:
        if os.path.exists(config_path):
            with open(config_path, "rb") as handle:
                read = tomli.load(handle)
                return read
    except:
        return []


async def update_db_pkl(path, data_orders, currency) -> None:

    my_path_portfolio = provide_path_for_file(path, currency)

    if currency_inline_with_database_address(currency, my_path_portfolio):

        replace_data(my_path_portfolio, data_orders)


def get_settlement_period(strategy_attributes) -> list:

    return remove_redundant_elements(
        remove_double_brackets_in_list(
            [o["settlement_period"] for o in strategy_attributes]
        )
    )


def redis_connect() -> redis.client.Redis:
    try:
        client = redis.Redis(
            host="localhost",
            port=6379,
            password=None,
            db=0,
            socket_timeout=5,
        )
        
        redis_pool = ConnectionPool(host='localhost', port=6379, db=0)
        client = redis.Redis(connection_pool=redis_pool)
        #pipeline = client.pipeline()
        
        #pubsub = client.pubsub()
        
        ping = client.ping()
        if ping is True:
            return client
    except redis.ConnectionError as error:
        parse_error_message(error)
        


pipeline = redis_connect().pipeline()
pubsub = redis_connect().pubsub()


def dispatch_to_redis(queue_redis) -> None:
    
    while True:
        message = queue_redis.get_nowait()
