#!/usr/bin/python3
# -*- coding: utf-8 -*-

# built ins
import asyncio
from datetime import datetime, timedelta, timezone
import os

# installed
from dataclassy import dataclass, fields
import json
from loguru import logger as log
import orjson
import tomli
import websockets
from multiprocessing.queues import Queue

# user defined formula
from configuration import id_numbering, config, config_oci
from db_management.sqlite_management import (
    deleting_row,
    executing_query_based_on_currency_or_instrument_and_strategy as get_query,
    executing_query_with_return,
    insert_tables,)
from transaction_management.deribit.api_requests import (
    get_end_point_result,
    get_currencies,
    get_instruments,)
from transaction_management.deribit.get_instrument_summary import (
    get_futures_instruments,)
from transaction_management.deribit.telegram_bot import (
    telegram_bot_sendtext,)
from transaction_management.deribit.managing_deribit import (
    ModifyOrderDb,
    currency_inline_with_database_address,)
from transaction_management.deribit.orders_management import (
    labelling_unlabelled_order,
    labelling_unlabelled_order_oto,
    saving_order_based_on_state,
    saving_traded_orders,)
from utilities.pickling import (
    replace_data,)
from utilities.system_tools import (
    provide_path_for_file,
    parse_error_message,)
from utilities.string_modification import (
    extract_currency_from_text,
    remove_double_brackets_in_list,
    remove_redundant_elements,)
from websocket_management.allocating_ohlc import (
    ohlc_result_per_time_frame,
    inserting_open_interest,)


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
        
@dataclass(unsafe_hash=True, slots=True)
class StreamAccountData(ModifyOrderDb):

    """

    +----------------------------------------------------------------------------------------------+
    reference: https://github.com/ElliotP123/crypto-exchange-code-samples/blob/master/deribit/websockets/dbt-ws-authenticated-example.py
    +----------------------------------------------------------------------------------------------+

    """

    sub_account_id: str
    client_id: str = fields 
    client_secret: str = fields 
    modify_order_and_db: object = fields 
    # Async Event Loop
    loop = asyncio.get_event_loop()
    ws_connection_url: str = "wss://www.deribit.com/ws/api/v2"
    # Instance Variables
    websocket_client: websockets.WebSocketClientProtocol = None
    refresh_token: str = None
    refresh_token_expiry_time: int = None
            
    def __post_init__(self):
        self.modify_order_and_db: str = ModifyOrderDb(self.sub_account_id)
        self.client_id: str = parse_dotenv(self.sub_account_id)["client_id"]
        self.client_secret: str = config_oci.get_oci_key(parse_dotenv(self.sub_account_id)["key_ocid"])
    
    async def ws_manager(
        self,
        queue: Queue) -> None:
        
        async with websockets.connect(
            self.ws_connection_url,
            ping_interval=None,
            compression=None,
            close_timeout=60,
        ) as self.websocket_client:
            
            # registering strategy config file    
            file_toml = "config_strategies.toml"

            try:                    
                
                # get ALL traded currencies in deribit
                get_currencies_all = await get_currencies()

                currencies = [o["currency"] for o in get_currencies_all["result"]]

                for currency in currencies:
                    
                    instruments = await get_instruments(currency)

                    my_path_instruments = provide_path_for_file("instruments", 
                                                                currency)

                    replace_data(
                        my_path_instruments,
                        instruments
                        )

                my_path_cur = provide_path_for_file("currencies")

                replace_data(
                    my_path_cur,
                    currencies
                    )
                
                # parsing config file
                config_app = get_config(file_toml)

                relevant_tables = config_app["relevant_tables"][0]
                
                trade_db_table= relevant_tables["my_trades_table"]
                
                order_db_table= relevant_tables["orders_table"]        
                                
                # get tradable strategies
                tradable_config_app = config_app["tradable"]
                
                # get TRADABLE currencies
                currencies = [o["spot"] for o in tradable_config_app] [0]

                strategy_attributes = config_app["strategies"]
                
                settlement_periods = get_settlement_period (strategy_attributes)
                                
                futures_instruments =  await get_futures_instruments (
                    currencies,
                    settlement_periods)  
                
                instruments_name = futures_instruments["instruments_name"]   
                
                strategy_attributes = config_app["strategies"]
                
                # get strategies that have not short/long attributes in the label 
                non_checked_strategies =   [o["strategy_label"] for o in strategy_attributes \
                    if o["non_checked_for_size_label_consistency"]==True]
                                            
                while True:
                    
                    # Authenticate WebSocket Connection
                    await self.ws_auth()
                    
                    # Establish Heartbeat
                    await self.establish_heartbeat()

                    # Start Authentication Refresh Task
                    self.loop.create_task(self.ws_refresh_auth())
                                
                    resolution = 1                
                    
                    for currency in currencies:
                        
                        currency_upper = currency.upper()
                        
                        instrument_perpetual = (f"{currency_upper}-PERPETUAL")
                            
                        ws_channel_currency = [
                            f"user.portfolio.{currency}",
                            f"user.changes.any.{currency_upper}.raw",
                            f"chart.trades.{instrument_perpetual}.{resolution}"
                            ]
                    
                        for ws in ws_channel_currency:

                            asyncio.create_task(
                                self.ws_operation(
                                    operation = "subscribe", 
                                    ws_channel = ws)
                                )
                      
                    for instrument in instruments_name:
                                            
                        ws_channel_instrument = [
                            f"incremental_ticker.{instrument}",
                            ]
                        
                        for ws in ws_channel_instrument:
                            asyncio.create_task(
                                self.ws_operation(
                                    operation = "subscribe",
                                    ws_channel = ws,
                                    ))
                    
                    incremental_ticker_buffer = []
                    user_changes_buffer = []
                    chart_trades_buffer = []
                    
                    while True:
                        
                        # Receive WebSocket messages
                        message: bytes = await self.websocket_client.recv()
                        message: dict = orjson.loads(message)

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
                                                                
                                # queing result
                                
                                data = message["params"]["data"]
                                
                                queue.put(data)
                                
                                message_channel: str = message["channel"]
                                                        
                                currency: str = extract_currency_from_text(message_channel)
                                
                                currency_lower: str = currency.lower()
                                
                                            
                                if "user.changes.any" in message_channel:
                                    user_changes_buffer.append(data)
                                    
                                    log.error (f"user_changes_buffer {user_changes_buffer}")
                                    
                                    if len(user_changes_buffer) > 0:
                                        
                                        trades = data["trades"]
                                        
                                        orders = data["orders"]

                                        instrument_name = data["instrument_name"]
                                        
                                        if orders:
                                                        
                                            if trades:
                                                
                                                archive_db_table= f"my_trades_all_{currency_lower}_json"
                                                
                                                for trade in trades:
                                                    
                                                    log.info (f"{trade}")
                                                
                                                    if f"f{currency.upper()}-FS-" not in instrument_name:
                                                    
                                                        await saving_traded_orders(
                                                            trade, 
                                                            archive_db_table, 
                                                            order_db_table
                                                            )
                                                        
                                            else:
                                            
                                                                                    
                                                for order in orders:
                                                    
                                                    if  'OTO' not in order["order_id"]:
                                                        
                                                        log.warning (f"order {order}")
                                                                                
                await saving_order_based_on_state (
                    order_db_table, 
                    order
                    )

                                        
                                        await modify_order_and_db. update_user_changes(
                                            non_checked_strategies,
                                            data_orders, 
                                            currency, 
                                            order_db_table,
                                            archive_db_table,
                                            )   
                                                       
                                                                         
                                        user_changes_buffer = []

                                                            
                                if "chart.trades" in message_channel:
                                    
                                    chart_trades_buffer.append(data)
                                    
                                    log.warning (f"chart_trades_buffer {chart_trades_buffer}")
                                    
                                    if len(chart_trades_buffer) > 0:

                                        instrument_ticker = ((message_channel)[13:]).partition('.')[0] 

                                        if "PERPETUAL" in instrument_ticker:

                                            TABLE_OHLC1: str = f"ohlc{resolution}_{currency_lower}_perp_json"
                                            WHERE_FILTER_TICK: str = "tick"
                                            DATABASE: str = "databases/trading.sqlite3"
                                            
                                            await ohlc_result_per_time_frame(
                                                instrument_ticker,
                                                resolution,
                                                chart_trades_buffer[0],
                                                TABLE_OHLC1,
                                                WHERE_FILTER_TICK,
                                                )
                                            
                                            chart_trades_buffer = []
                                    
                                
                                instrument_ticker = (message_channel)[19:]
                                if (message_channel  == f"incremental_ticker.{instrument_ticker}"):

                                    incremental_ticker_buffer.append(data)
                                    
                                    log.debug (f"incremental_ticker_buffer {incremental_ticker_buffer}")
                                    
                                    if len(incremental_ticker_buffer) > 0:                                      
                                            

                                        my_path_ticker = provide_path_for_file(
                                            "ticker", instrument_ticker)
                                        
                                        await distribute_ticker_result_as_per_data_type(
                                            my_path_ticker,
                                            incremental_ticker_buffer[0], 
                                            )
                                                        
                                        if "PERPETUAL" in incremental_ticker_buffer[0]["instrument_name"]:
                                            
                                            await inserting_open_interest(
                                                currency, 
                                                WHERE_FILTER_TICK, 
                                                TABLE_OHLC1, 
                                                incremental_ticker_buffer[0]
                                                )   
                                            
                                        incremental_ticker_buffer = []                        
                                    
                                                                    

            except Exception as error:

                await parse_error_message(error)  

                await telegram_bot_sendtext (
                    error,
                    "general_error"
                    )
                
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
            await self.websocket_client.send(json.dumps(msg))
        except Exception as error:

            await parse_error_message(error)  

            await telegram_bot_sendtext (
                error,
                "general_error"
                )

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
        
        try:
            await self.websocket_client.send(json.dumps(msg))
        except Exception as error:

            await parse_error_message(error)  

            await telegram_bot_sendtext (
                error,
                "general_error"
                )

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
        
        try:
            await self.websocket_client.send(json.dumps(msg))

        except Exception as error:
                            
            await parse_error_message(error)  

            await telegram_bot_sendtext (
                error,
                "general_error"
                )

                
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

                    await self.websocket_client.send(json.dumps(msg))

            await asyncio.sleep(150)

    async def ws_operation(
        self, 
        operation: str, 
        ws_channel: str, 
        source: str = "ws"
    ) -> None:
        """
        Requests `public/subscribe` or `public/unsubscribe`
        to DBT's API for the specific WebSocket Channel.
        """
        sleep_time: int = 5
        
        await asyncio.sleep(sleep_time)

        id = id_numbering.id(
            operation,
            ws_channel
            )
        
        msg: dict = {
            "jsonrpc": "2.0",
        }
        
        if "ws" in source:

            extra_params: dict = dict(
                id= id,
                method= f"private/{operation}",
                params= {"channels": [ws_channel]}
            )
            
            msg.update(extra_params)

            await self.websocket_client.send(json.dumps(msg))

        if "rest_api" in source:

            extra_params: dict = await get_end_point_result(
                operation,
                ws_channel)
            
            msg.update(extra_params)
            
            await self.websocket_client.send(json.dumps(msg))


async def distribute_ticker_result_as_per_data_type(
    my_path_ticker: str, 
    data_orders: dict, 
    ) -> None:
    """ """

    try:
    
        if data_orders["type"] == "snapshot":
            replace_data(
                my_path_ticker, 
                data_orders
                )

        else:
            ticker_change: list = read_data(my_path_ticker)

            if ticker_change != []:

                for item in data_orders:
                    
                    ticker_change[0][item] = data_orders[item]
                    
                    replace_data(
                        my_path_ticker, 
                        ticker_change
                        )

    except Exception as error:
        
        await parse_error_message(error)  

        await telegram_bot_sendtext (
            error,
            "general_error"
            )

