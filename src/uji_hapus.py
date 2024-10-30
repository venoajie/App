"""
Description:
    Deribit WebSocket Asyncio Example.

    - Authenticated connection.

Usage:
    python3.9 dbt-ws-authenticated-example.py

Requirements:
    - websocket-client >= 1.2.1
"""

# built ins
import asyncio
import sys
import json
import logging
from typing import Dict
from datetime import datetime, timedelta

# installed
import websockets


#!/usr/bin/python3
# -*- coding: utf-8 -*-
#
# built ins
from datetime import datetime, timedelta
import asyncio
import functools
import os

# installed
from dataclassy import dataclass, fields
from loguru import logger as log
import json
import orjson
import tomli
import websockets

# user defined formula
from configuration import id_numbering, config
from configuration.label_numbering import get_now_unix_time
from db_management.sqlite_management import (
    querying_table,
    executing_query_based_on_currency_or_instrument_and_strategy as get_query,
    insert_tables,)
from transaction_management.deribit.api_requests import (
    ModifyOrderDb,)
from transaction_management.deribit.telegram_bot import (
    telegram_bot_sendtext,)
from strategies.basic_strategy import(
    is_label_and_side_consistent,)
from strategies.hedging_spot import HedgingSpot  
from transaction_management.deribit.api_requests import (
    get_instruments,)
from transaction_management.deribit.orders_management import (
    saving_traded_orders,
    saving_orders,)
from utilities.number_modification import get_closest_value
from utilities.pickling import (
    replace_data,
    read_data,)
from utilities.system_tools import (
    provide_path_for_file,
    async_raise_error_message,
    raise_error_message,
    sleep_and_restart,)
from utilities.string_modification import (
    extract_currency_from_text,
    remove_double_brackets_in_list,
    remove_redundant_elements,)
from websocket_management.ws_management import (
    cancel_all,
    currency_inline_with_database_address,
    distribute_ticker_result_as_per_data_type,
    get_futures_instruments,
    labelling_the_unlabelled_and_resend_it,)
from websocket_management.allocating_ohlc import (
    ohlc_result_per_time_frame,
    inserting_open_interest,)
from websocket_management.cleaning_up_transactions import (
    check_whether_order_db_reconciled_each_other,
    check_whether_size_db_reconciled_each_other,
    clean_up_closed_transactions,)

@functools.lru_cache(maxsize=128)
def memoization (sub_account) -> dict:
    """_summary_
    https://dbader.org/blog/python-memoization
    https://rednafi.com/python/lru_cache_on_methods/
    https://gist.github.com/Morreski/c1d08a3afa4040815eafd3891e16b945
    https://www.tutorialspoint.com/clear-lru-cache-in-python

    Args:
        sub_account (_type_): _description_

    Returns:
        dict: _description_
    """
    return config.main_dotenv(sub_account)

def parse_dotenv (sub_account) -> dict:
    return config.main_dotenv(sub_account)

def reading_from_pkl_data(
    end_point, 
    currency,
    status: str = None
    ) -> dict:
    """ """

    path: str = provide_path_for_file (end_point,
                                      currency,
                                      status)

    data = read_data(path)

    return data

def get_config(file_name: str) -> list:
    """ """
    config_path = provide_path_for_file (file_name)
    
    try:
        if os.path.exists(config_path):
            with open(config_path, "rb") as handle:
                read= tomli.load(handle)
                return read
    except:
        return []


def compute_notional_value(
    index_price: float,
    equity: float
    ) -> float:
    """ """
    return index_price * equity


async def update_db_pkl(
    path, 
    data_orders,
    currency
    ) -> None:

    my_path_portfolio = provide_path_for_file (path,
                                               currency)
        
    if currency_inline_with_database_address(
        currency,
        my_path_portfolio):
        
        replace_data (
            my_path_portfolio, 
            data_orders
            )

def get_settlement_period (strategy_attributes) -> list:
    
    return (remove_redundant_elements(
        remove_double_brackets_in_list(
            [o["settlement_period"]for o in strategy_attributes]))
            )

def get_index (
    data_orders: dict, 
    ticker: dict
    ) -> float:

    try:
        index_price= data_orders["index_price"]
        
    except:
        
        index_price= ticker["index_price"]
        
        if index_price==[]:
            index_price = ticker ["estimated_delivery_price"]
        
    return index_price


class main:
    def __init__(
        self,
        ws_connection_url: str,
        client_id: str,
        client_secret: str
            ) -> None:
        # Async Event Loop
        self.loop = asyncio.get_event_loop()

        # Instance Variables
        self.ws_connection_url: str = ws_connection_url
        self.client_id: str = client_id
        self.client_secret: str = client_secret
        self.websocket_client: websockets.WebSocketClientProtocol = None
        self.refresh_token: str = None
        self.refresh_token_expiry_time: int = None

        # Start Primary Coroutine
        self.loop.run_until_complete(
            self.ws_manager()
            )

    async def ws_manager(self) -> None:
        async with websockets.connect(
            self.ws_connection_url,
            ping_interval=None,
            compression=None,
            close_timeout=60
            ) as self.websocket_client:

            # Authenticate WebSocket Connection
            await self.ws_auth()

            # Establish Heartbeat
            await self.establish_heartbeat()

            # Start Authentication Refresh Task
            self.loop.create_task(
                self.ws_refresh_auth()
                )

            # Subscribe to the specified WebSocket Channel
            self.loop.create_task(
                self.ws_operation(
                    operation='subscribe',
                    ws_channel='trades.BTC-PERPETUAL.raw'
                    )
                )

            while self.websocket_client.open:
                message: bytes = await self.websocket_client.recv()
                message: Dict = json.loads(message)
                logging.info(message)

                if 'id' in list(message):
                    if message['id'] == 9929:
                        if self.refresh_token is None:
                            logging.info('Successfully authenticated WebSocket Connection')
                        else:
                            logging.info('Successfully refreshed the authentication of the WebSocket Connection')

                        self.refresh_token = message['result']['refresh_token']

                        # Refresh Authentication well before the required datetime
                        if message['testnet']:
                            expires_in: int = 300
                        else:
                            expires_in: int = message['result']['expires_in'] - 240

                        self.refresh_token_expiry_time = datetime.utcnow() + timedelta(seconds=expires_in)

                    elif message['id'] == 8212:
                        # Avoid logging Heartbeat messages
                        continue

                elif 'method' in list(message):
                    # Respond to Heartbeat Message
                    if message['method'] == 'heartbeat':
                        await self.heartbeat_response()

            else:
                logging.info('WebSocket connection has broken.')
                sys.exit(1)

    async def establish_heartbeat(self) -> None:
        """
        Requests DBT's `public/set_heartbeat` to
        establish a heartbeat connection.
        """
        msg: Dict = {
                    "jsonrpc": "2.0",
                    "id": 9098,
                    "method": "public/set_heartbeat",
                    "params": {
                              "interval": 10
                               }
                    }

        await self.websocket_client.send(
            json.dumps(
                msg
                )
                )

    async def heartbeat_response(self) -> None:
        """
        Sends the required WebSocket response to
        the Deribit API Heartbeat message.
        """
        msg: Dict = {
                    "jsonrpc": "2.0",
                    "id": 8212,
                    "method": "public/test",
                    "params": {}
                    }

        await self.websocket_client.send(
            json.dumps(
                msg
                )
                )

    async def ws_auth(self) -> None:
        """
        Requests DBT's `public/auth` to
        authenticate the WebSocket Connection.
        """
        msg: Dict = {
                    "jsonrpc": "2.0",
                    "id": 9929,
                    "method": "public/auth",
                    "params": {
                              "grant_type": "client_credentials",
                              "client_id": self.client_id,
                              "client_secret": self.client_secret
                               }
                    }

        await self.websocket_client.send(
            json.dumps(
                msg
                )
            )

    async def ws_refresh_auth(self) -> None:
        """
        Requests DBT's `public/auth` to refresh
        the WebSocket Connection's authentication.
        """
        while True:
            if self.refresh_token_expiry_time is not None:
                if datetime.utcnow() > self.refresh_token_expiry_time:
                    msg: Dict = {
                                "jsonrpc": "2.0",
                                "id": 9929,
                                "method": "public/auth",
                                "params": {
                                          "grant_type": "refresh_token",
                                          "refresh_token": self.refresh_token
                                            }
                                }

                    await self.websocket_client.send(
                        json.dumps(
                            msg
                            )
                            )

            await asyncio.sleep(150)

    async def ws_operation(
        self,
        operation: str,
        ws_channel: str
            ) -> None:
        """
        Requests `public/subscribe` or `public/unsubscribe`
        to DBT's API for the specific WebSocket Channel.
        """
        await asyncio.sleep(5)

        msg: Dict = {
                    "jsonrpc": "2.0",
                    "method": f"public/{operation}",
                    "id": 42,
                    "params": {
                        "channels": [ws_channel]
                        }
                    }

        await self.websocket_client.send(
            json.dumps(
                msg
                )
            )


if __name__ == "__main__":
    # Logging
    logging.basicConfig(
        level='INFO',
        format='%(asctime)s | %(levelname)s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
        )

    # DBT LIVE WebSocket Connection URL
    ws_connection_url: str = 'wss://www.deribit.com/ws/api/v2'
    # DBT TEST WebSocket Connection URL
    #ws_connection_url: str = 'wss://test.deribit.com/ws/api/v2'

    # DBT Client ID
    client_id: str = 'nOTD9z-n'
    # DBT Client Secret
    client_secret: str = '6euDzySQqjNRAG9Rt8EnFPS4ZVfv1vPtTfVjf_8237c'

    main(
         ws_connection_url=ws_connection_url,
         client_id=client_id,
         client_secret=client_secret
         )