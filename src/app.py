#!/usr/bin/env/python
# -*- coding: utf-8 -*-

import asyncio
from datetime import datetime, timedelta, timezone
import os

from cachetools import cached, LRUCache, TTLCache

# installed
from dataclassy import dataclass, fields
import httpx
import json, orjson
from loguru import logger as log
import tomli
import websockets

from configuration import id_numbering, config
from configuration.label_numbering import get_now_unix_time
from db_management.sqlite_management import (
    querying_table,
    executing_query_based_on_currency_or_instrument_and_strategy as get_query,
    insert_tables,)
from market_understanding.technical_analysis import (
    insert_market_condition_result)
from strategies.combo_auto import ComboAuto
from transaction_management.deribit.api_requests import (
    get_currencies,
    get_instruments,)

from transaction_management.deribit.managing_deribit import (
    ModifyOrderDb,
    currency_inline_with_database_address,)

from transaction_management.deribit.telegram_bot import (
    telegram_bot_sendtext,)
from utilities.pickling import (
    replace_data,
    read_data,)
from utilities.string_modification import (
    extract_currency_from_text,
    remove_double_brackets_in_list,
    remove_redundant_elements,)
from utilities.system_tools import (
    async_raise_error_message,
    provide_path_for_file,
    raise_error_message)
from websocket_management.ws_management import (
    get_futures_instruments,
    )
from websocket_management.cleaning_up_transactions import (
    check_whether_order_db_reconciled_each_other,
    check_whether_size_db_reconciled_each_other,
    clean_up_closed_transactions, 
    count_and_delete_ohlc_rows,
    get_unrecorded_trade_and_order_id,
    reconciling_sub_account_and_db_open_orders)

# API endpoint for fetching TODOs
endpoint = "<https://jsonplaceholder.typicode.com/todos/>"
    
# Using the LRUCache decorator function with a maximum cache size of 3

def ticker_data(currencies):
    """_summary_
    https://blog.apify.com/python-cache-complete-guide/

    Args:
        instrument_ticker (_type_): _description_

    Returns:
        _type_: _description_
    """
    #result = (orjson.loads(data))
    
    log.critical ("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
    
    result=[]
    for currency in currencies:
        instrument_name = f"{currency}-PERPETUAL"
        
        result_instrument= ((reading_from_pkl_data(
                "ticker",
                instrument_name
                )))[0]
        result.append (result_instrument)

    return result

result_json = {}
def ticker_data_alt(data_orders):
    instrument_name = data_orders["instrument_name"]

    if "snapshot" in data_orders["type"]:
        my_path_ticker = provide_path_for_file(
            "ticker", instrument_name)
        replace_data(my_path_ticker,data_orders)
        return result_json |data_orders


def parse_dotenv (sub_account) -> dict:
    return config.main_dotenv(sub_account)
    
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


def compute_notional_value(
    index_price: float,
    equity: float
    ) -> float:
    """ """
    return index_price * equity



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


def get_settlement_period (strategy_attributes) -> list:
    
    return (remove_redundant_elements(
        remove_double_brackets_in_list(
            [o["settlement_period"]for o in strategy_attributes]))
            )
    
    
def future_spread_attributes (
    position_without_combo:  list,
    active_futures: list,
    currency_upper: str,
    notional: float,
    best_ask_prc: float,
    server_time: int,
    ) -> dict:

    #log.info (f"{}")    
    return  [{'instrument_name':o['instrument_name'], 
              'is_weekly': o["settlement_period"]=="week", 
              'premium': (reading_from_pkl_data("ticker",  o["instrument_name"])[0]["mark_price"])   - best_ask_prc, 
              'contango':((reading_from_pkl_data("ticker",  o["instrument_name"])[0]["mark_price"])   - best_ask_prc) > 0, 
              'leverage_instrument': 0 if ([i["size"]/notional for i in position_without_combo if i["instrument_name"] == o["instrument_name"]]) == []\
                                else abs([i["size"]/notional for i in position_without_combo if i["instrument_name"] == o["instrument_name"]][0]), 
                'pct_premium_per_day':((reading_from_pkl_data("ticker", 
                                                              o["instrument_name"])[0]["mark_price"]) - best_ask_prc
                                                   )/((o["expiration_timestamp"] - server_time)/1000/60/60/24
                                                      )/best_ask_prc,
                                                   } for o in active_futures if "PERP" not in o["instrument_name"]\
                                                       and currency_upper  in o["instrument_name"]
                            ]
                          
@dataclass(unsafe_hash=True, slots=True)
class RunningStrategy(ModifyOrderDb):

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
    

    # @lru_cache(maxsize=None)
    async def ws_manager(self) -> None:
        async with websockets.connect(
            self.ws_connection_url,
            ping_interval=None,
            compression=None,
            close_timeout=60,
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

                active_combo_perp = futures_instruments["active_combo_perp"]  

                instruments_name = futures_instruments["instruments_name"]   
                
                min_expiration_timestamp = futures_instruments["min_expiration_timestamp"]    
                
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

                    # Start Authentication Refresh Task
                    self.loop.create_task(self.ws_refresh_auth())
                    
                    ticker = ticker_data(currencies)    
                    for currency in currencies:

                        self.loop.create_task(
                            self.ws_operation(
                                operation="subscribe",
                                ws_channel= f"incremental_ticker.{currency}-PERPETUAL",
                            ))
                        
                
                    while self.websocket_client.open:
                        
                        try:
                                
                            # Receive WebSocket messages
                            message: bytes = await self.websocket_client.recv()
                            message: dict = orjson.loads(message)
                            message_channel: str = None
                            #log.warning (message)
                            if "id" in list(message):
                                if message["id"] == 9929:
                                    
                                    if self.refresh_token is None:
                                        log.info ("Successfully authenticated WebSocket Connection")

                                    else:
                                        log.info ("Successfully refreshed the authentication of the WebSocket Connection")

                                    self.refresh_token = message["result"]["refresh_token"]

                                    # Refresh Authentication well before the required datetime
                                    if message["testnet"]:
                                        expires_in: int = 300
                                    else:
                                        expires_in: int = message["result"]["expires_in"] - 240

                                    now_utc = datetime.now(timezone.utc)
                                    
                                    self.refresh_token_expiry_time = now_utc + timedelta(
                                        seconds=expires_in
                                    )

                                elif message["id"] == 8212:
                                    # Avoid logging Heartbeat messages
                                    continue

                            elif "method" in list(message):
                                # Respond to Heartbeat Message
                                if message["method"] == "heartbeat":
                                    await self.heartbeat_response()

                            if "params" in list(message):
                                
                                if message["method"] != "heartbeat":
                                    message_channel = message["params"]["channel"]

                                    data_orders: list = message["params"]["data"] 

                                    log.info (f"data_orders {data_orders}")

                                    if "snapshot" in data_orders["type"]:
                                        my_path_ticker = provide_path_for_file(
                                            "ticker", data_orders["instrument_name"])
                                        replace_data(my_path_ticker,data_orders)
                                    
                                    for instrument in ticker:
                                        if data_orders["instrument_name"]  == instrument ["instrument_name"]:                                            
                                            for item in data_orders:
                                                ticker[0][item] = data_orders[item]
                                    
                                    instrument_name =data_orders["instrument_name"]
                                    ticker_perpetual = [o for o in ticker if instrument_name in o["instrument_name"]]
                                    log.info (f"ticker {ticker_perpetual}")
                                    
                        
                                
                        except Exception as error:
                            await raise_error_message (error)
                            await telegram_bot_sendtext (
                                error,
                                "general_error")


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
        msg: dict = {
            "jsonrpc": "2.0",
            "id": 9098,
            "method": "public/set_heartbeat",
            "params": {"interval": 10},
        }

        try:
            
            message = await self.websocket_client.send(json.dumps(msg))
            
            log.error (f"message {message}")
        
            await self.websocket_client.send(json.dumps(msg))
        except Exception as error:
            log.warning(error)

    async def heartbeat_response(self) -> None:
        """
        Sends the required WebSocket response to
        the Deribit API Heartbeat message.
        """
        msg: dict = {
            "jsonrpc": "2.0",
            "id": 8212,
            "method": "public/test",
            "params": {},
        }
        
        #orjson_dump= (orjson.dumps(msg))
        
        #log.error (f" orjson_dump {orjson_dump}")
        json_dump= (json.dumps(msg))
        #log.error (f" json_dump {json_dump}")
        
        message = await self.websocket_client.send(json.dumps(msg))
        
        log.error (f"message {message}")
        
        try:
            await self.websocket_client.send(json.dumps(msg))

        except Exception as error:
            log.warning(error)

    async def ws_auth(self) -> None:
        """
        Requests DBT's `public/auth` to
        authenticate the WebSocket Connection.
        """
        msg: dict = {
            "jsonrpc": "2.0",
            "id": 9929,
            "method": "public/auth",
            "params": {
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            },
        }
        
        
        message = await self.websocket_client.send(json.dumps(msg))
        
        log.error (f"message {message}")
        
        await self.websocket_client.send(json.dumps(msg))

    async def ws_refresh_auth(self) -> None:
        """
        Requests DBT's `public/auth` to refresh
        the WebSocket Connection's authentication.
        """
        while True:
            now_utc = datetime.now(timezone.utc)
            if self.refresh_token_expiry_time is not None:
                if now_utc > self.refresh_token_expiry_time:
                    
                    msg: dict = {
                        "jsonrpc": "2.0",
                        "id": 9929,
                        "method": "public/auth",
                        "params": {
                            "grant_type": "refresh_token",
                            "refresh_token": self.refresh_token,
                        },
                    }
                    
                    message = await self.websocket_client.send(json.dumps(msg))
                    
                    log.error (f"message {message}")
                    

                    await self.websocket_client.send(json.dumps(msg))

            await asyncio.sleep(150)
    
                        
                                    
    async def ws_operation(
        self, 
        operation: str, 
        ws_channel: str, 
        id: int = 100
    ) -> None:
        """
        Requests `public/subscribe` or `public/unsubscribe`
        to DBT's API for the specific WebSocket Channel.
        """
        sleep_time: int = 5
        
        await asyncio.sleep(sleep_time)

        id = id_numbering.id(operation, ws_channel)

        msg: dict = {
            "jsonrpc": "2.0",
            "method": f"private/{operation}",
            "id": id,
            "params": {"channels": [ws_channel]},
        }

        #log.info(ws_channel)
        await self.websocket_client.send(json.dumps(msg))



                        # check for delivered instrument               
                

    @cached(cache=LRUCache(maxsize=3))
    def ticker_data(
        self,
        data):
        return orjson.loads(data)


def main():
    # https://www.codementor.io/@jflevesque/python-asynchronous-programming-with-asyncio-library-eq93hghoc
    sub_account_id = "deribit-148510"
    
    try:
        RunningStrategy(sub_account_id)

    except Exception as error:
        raise_error_message (
            error, 
            5,
            "app"
            )
                
if __name__ == "__main__":
    
    try:
        (main())
        
    except(
        KeyboardInterrupt, 
        SystemExit
        ):
        asyncio.get_event_loop().run_until_complete(main().stop_ws())
        

    except Exception as error:
        raise_error_message(
        error, 
        5, 
        "app"
        )
