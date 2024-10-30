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

@dataclass(unsafe_hash=True, slots=True)
class main:

    sub_account_id: str
    client_id: str = fields 
    client_secret: str = fields 
    modify_order_and_db: object = fields 
    # Async Event Loop
    loop = asyncio.get_event_loop()
    ws_connection_url: str = "wss://www.deribit.com/ws/api/v2"
    # Instance Variables
    connection_url: str = "https://www.deribit.com/api/v2/"
    websocket_client: websockets.WebSocketClientProtocol = None
    refresh_token: str = None
    refresh_token_expiry_time: int = None
            
    def __post_init__(self):
        self.modify_order_and_db: str = ModifyOrderDb(self.sub_account_id)
        self.client_id: str = parse_dotenv(self.sub_account_id)["client_id"]
        self.client_secret: str = parse_dotenv(self.sub_account_id)["client_secret"]

        # Start Primary Coroutine
        self.loop.run_until_complete(self.ws_manager())

    async def ws_manager(self) -> None:
        async with websockets.connect(
            self.ws_connection_url,
            ping_interval=None,
            compression=None,
            close_timeout=60
            ) as self.websocket_client:
            
            
            try:
                    
                # registering strategy config file    
                file_toml = "config_strategies.toml"
                
                # parsing config file
                config_app = get_config(file_toml)

                # get tradable strategies
                tradable_config_app = config_app["tradable"]
                
                # get tradable currencies
                currencies= [o["spot"] for o in tradable_config_app] [0]
                
                strategy_attributes = config_app["strategies"]
                                        
                active_strategies =   [o["strategy_label"] for o in strategy_attributes \
                    if o["is_active"]==True]

                # get strategies that have not short/long attributes in the label 
                non_checked_strategies =   [o["strategy_label"] for o in strategy_attributes \
                    if o["non_checked_for_size_label_consistency"]==True]
                
                cancellable_strategies =   [o["strategy_label"] for o in strategy_attributes \
                    if o["cancellable"]==True]
                
                trade_db_table= "my_trades_all_json"
                
                order_db_table= "orders_all_json"         
                                
                settlement_periods= get_settlement_period (strategy_attributes)
                                
                futures_instruments=await get_futures_instruments (currencies,
                                                                    settlement_periods)  
                
                active_futures = futures_instruments["active_futures"]   

                instruments_name = futures_instruments["instruments_name"]   
                                
                resolution = 1                
                    
                # filling currencies attributes
                my_path_cur = provide_path_for_file("currencies")
                replace_data(
                    my_path_cur,
                    currencies
                    )
                
                while True:
                            

                    # Authenticate WebSocket Connection
                    await self.ws_auth()

                    # Establish Heartbeat
                    await self.establish_heartbeat()

                    for currency in currencies:
                        #await self.modify_order_and_db.cancel_the_cancellables (currency,
                        #                                                        cancellable_strategies)
                        await self.modify_order_and_db.resupply_sub_accountdb (currency)

                        await self.modify_order_and_db.resupply_portfolio (currency)
                        
                        currency_lower = currency.lower ()
                        archive_db_table= f"my_trades_all_{currency_lower}_json"                    
                        transaction_log_trading= f"transaction_log_{currency_lower}_json"
                        
                        instruments = await get_instruments(currency)

                        my_path_instruments = provide_path_for_file("instruments", 
                                                                    currency)

                        replace_data(my_path_instruments, instruments)

                        await self.modify_order_and_db.resupply_transaction_log(
                            currency, 
                            transaction_log_trading,
                            archive_db_table)
                        
                        ws_channel_currency = [
                            f"user.portfolio.{currency}",
                            f"user.changes.any.{currency.upper()}.raw",
                            ]
                    
                        for ws in ws_channel_currency:

                            self.loop.create_task(
                                self.ws_operation(
                                    operation="subscribe", ws_channel=ws))
                        
                    for instrument in instruments_name:
                                            
                        ws_channel_instrument = [f"incremental_ticker.{instrument}",
                                                f"chart.trades.{instrument}.{resolution}"]
                        
                        for ws in ws_channel_instrument:
                            self.loop.create_task(
                            self.ws_operation(
                                operation="subscribe",
                                ws_channel= ws,
                            ))
                        

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
                    
            except Exception as error:
                await raise_error_message (error)
                await telegram_bot_sendtext (
                    error,
                    "general_error")
                    
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
    
    sub_account_id = "deribit-148510"

    main(sub_account_id)
