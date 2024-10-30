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
                        #logging.info(message)

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


                            if "params" in list(message):
                                if message["method"] != "heartbeat":
                                    message_channel = message["params"]["channel"]

                                    data_orders: list = message["params"]["data"]    
                                    
                                    currency: str = extract_currency_from_text(message_channel)
                                    
                                    currency_lower=currency
                                    
                                    currency_upper=currency.upper()
                                    
                                    archive_db_table= f"my_trades_all_{currency_lower}_json"
                                    
                                    transaction_log_trading= f"transaction_log_{currency_lower}_json"

                                    if message_channel == f"user.portfolio.{currency.lower()}":
                                        # also always performed at restart                                
                                        await update_db_pkl(
                                            "portfolio", 
                                            data_orders, 
                                            currency
                                            )
                        
                                        await self.modify_order_and_db.resupply_sub_accountdb(currency)    

                                        
                                    if "user.changes.any" in message_channel:
                                        #log.debug (f"user.changes.any.{currency.upper()}.raw")
                                        
                                        await self.update_user_changes(
                                            non_checked_strategies,
                                            data_orders, 
                                            currency, 
                                            order_db_table,
                                            trade_db_table, 
                                            archive_db_table,
                                            transaction_log_trading
                                            )
                                    
                                    TABLE_OHLC1: str = f"ohlc{resolution}_{currency_lower}_perp_json"
                                    WHERE_FILTER_TICK: str = "tick"
                                    DATABASE: str = "databases/trading.sqlite3"
                                    
                                    if "chart.trades" in message_channel:
                                        instrument_ticker = ((message_channel)[13:]).partition('.')[0] 
                                                                                                    
                                        await ohlc_result_per_time_frame(
                                            instrument_ticker,
                                            resolution,
                                            data_orders,
                                            TABLE_OHLC1,
                                            WHERE_FILTER_TICK,
                                            )
                                        
                                    instrument_ticker = (message_channel)[19:]  
                                    
                                    if (
                                        message_channel
                                        == f"incremental_ticker.{instrument_ticker}"):
                                        
                                        my_path_ticker = provide_path_for_file(
                                            "ticker", instrument_ticker)
                                        
                                        await distribute_ticker_result_as_per_data_type(
                                            my_path_ticker, data_orders, instrument_ticker)
                                                    
                                        try:
                                        
                                            if "PERPETUAL" in data_orders["instrument_name"]:
                                                
                                                await inserting_open_interest(
                                                    currency, 
                                                    WHERE_FILTER_TICK, 
                                                    TABLE_OHLC1, 
                                                    data_orders
                                                    )
                                                
                                                
                                                try:                                          
                                                                                    
                                                    perpetual_ticker= reading_from_pkl_data("ticker",
                                                                                instrument_ticker)
                                                    
                                                    try:
                                                        perpetual_ticker= perpetual_ticker[0]
                                                    except:
                                                        perpetual_ticker= []
                                                                                                    
                                                    if perpetual_ticker:
                                                                                                            
                                                        await self.running_critical_strategy(
                                                            currency, 
                                                            currency_upper,
                                                            order_db_table,
                                                            trade_db_table, 
                                                            archive_db_table,
                                                            transaction_log_trading,
                                                            data_orders,
                                                            perpetual_ticker,
                                                            active_strategies,
                                                            active_futures,
                                                            strategy_attributes,
                                                            non_checked_strategies,
                                                            cancellable_strategies,
                                                            instrument_ticker, 
                                                            )
                                                                                        
                                                except ValueError:
                                                    import traceback
                                                    traceback.format_exc()
                                                    log.info(f" error {error}")
                                                    #continue
                                                                    
                                        except Exception as error:
                                            await cancel_all()
                                            await async_raise_error_message(
                                                error, 
                                                0.1,
                                                "WebSocket connection - failed to process data - cancel_all",

                                            )#




                    else:
                        logging.info('WebSocket connection has broken.')
                        sys.exit(1)
                    
            except Exception as error:
                await raise_error_message (error)
                await telegram_bot_sendtext (
                    error,
                    "general_error")
                    
                    

    async def update_user_changes_non_ws(
    self,
    non_checked_strategies,
    data_orders, 
    currency, 
    order_db_table,
    trade_db_table, 
    archive_db_table,
    transaction_log_trading
    ) -> None:

        trades = data_orders["trades"]
        
        order = data_orders["order"]

        instrument_name = order["instrument_name"]
        
        log.debug (f"update_user_changes non ws {instrument_name} -START")
        
        log.info (f" {data_orders}")
        
        if trades:
            for trade in trades:
                
                if f"{currency.upper()}-FS-" not in instrument_name:
                
                    await saving_traded_orders(
                        trade, 
                        trade_db_table, 
                        order_db_table)                      

                    await saving_traded_orders(
                        trade,
                        archive_db_table,
                        order_db_table
                        )
            
        else:
                
            await self.saving_order(
                non_checked_strategies,
                instrument_name,
                order,
                order_db_table
                )
                                    
            await self.modify_order_and_db.resupply_sub_accountdb(currency)            

        await update_db_pkl("positions", data_orders, currency)

        await self.modify_order_and_db.resupply_transaction_log(
            currency, 
            transaction_log_trading,
            archive_db_table
            )


    async def running_critical_strategy(
        self,
        currency, 
        currency_upper,
        order_db_table,
        trade_db_table, 
        archive_db_table,
        transaction_log_trading,
        data_orders,
        perpetual_ticker,
        active_strategies,
        active_futures,
        strategy_attributes,
        non_checked_strategies,
        cancellable_strategies,
        instrument_ticker, 
        ) -> None:


        # get portfolio data  
        portfolio = reading_from_pkl_data ("portfolio",
                                              currency)
        
        equity: float = portfolio[0]["equity"]                                       
                                                            
        index_price= get_index (data_orders, perpetual_ticker)
        
        sub_account = reading_from_pkl_data("sub_accounts",
                                            currency)
        
        sub_account = sub_account[0]
                    
        if sub_account:
            column_trade: str= "instrument_name","label", "amount", "price","side"
            my_trades_currency: list= await get_query(trade_db_table, 
                                                        currency, 
                                                        "all", 
                                                        "all", 
                                                        column_trade)
                                                            
            column_list= "instrument_name", "position", "timestamp","trade_id","user_seq"        
            from_transaction_log = await get_query (transaction_log_trading, 
                                                        currency, 
                                                        "all", 
                                                        "all", 
                                                        column_list)                                       

            column_order= "instrument_name","label","order_id","amount","timestamp"
            orders_currency = await get_query(order_db_table, 
                                                    currency, 
                                                    "all", 
                                                    "all", 
                                                    column_order)     
        
            len_order_is_reconciled_each_other =  check_whether_order_db_reconciled_each_other (sub_account,
                                                                        instrument_ticker,
                                                                        orders_currency)

            size_is_reconciled_each_other = check_whether_size_db_reconciled_each_other(
                sub_account,
                instrument_ticker,
                my_trades_currency,
                from_transaction_log
                )
            
            log.info (f"index_price is not None {index_price is not None}  {index_price} equity > 0 {equity > 0} ")
            log.debug (f"size_is_reconciled_each_other {size_is_reconciled_each_other} len_order_is_reconciled_each_other {len_order_is_reconciled_each_other}")
            
            if  index_price is not None \
                and equity > 0 \
                    and  size_is_reconciled_each_other\
                        and  len_order_is_reconciled_each_other:
            
                TA_result_data_all = await querying_table("market_analytics_json")

                TA_result_data_only=  TA_result_data_all["list_data_only"]
                
                TA_result_data = [o for o in TA_result_data_only if currency_upper in o["instrument"]]                                                                                                    
                                                                
                tick_TA=  max([o["tick"] for o in TA_result_data])
                
                server_time = get_now_unix_time()  
                
                delta_time = server_time-tick_TA
                
                delta_time_seconds = delta_time/1000                                                
                                
                THRESHOLD_DELTA_TIME_SECONDS = 60 
                log.warning (f"delta_time_seconds < 120 {delta_time_seconds < THRESHOLD_DELTA_TIME_SECONDS} {delta_time_seconds} tick_TA {tick_TA} server_time {server_time}")
                
                #something was wrong because perpetuals were actively traded. cancell  orders
                if delta_time_seconds > THRESHOLD_DELTA_TIME_SECONDS:
                    await self.modify_order_and_db.cancel_the_cancellables (currency,
                                                                            cancellable_strategies)
                                                            
                else:#ensure freshness of ta
                    
                    notional: float = compute_notional_value(index_price, equity)
    
                    best_ask_prc: float = perpetual_ticker["best_ask_price"] 
                    
                    for strategy in active_strategies:
                        
                        my_trades_currency_strategy = [o for o in my_trades_currency if strategy in (o["label"]) ]
                        
                        orders_currency_strategy = [o for o in orders_currency if strategy in (o["label"]) ]
                        
                        orders_currency_strategy_label_closed = [o for o in orders_currency_strategy if "closed" in (o["label"]) ]
                    
                        my_trades_currency_strategy_open = [o for o in my_trades_currency_strategy if "open" in (o["label"])]
                        
                            
                        strategy_params= [o for o in strategy_attributes if o["strategy_label"] == strategy][0]   
                        
                        if "hedgingSpot" in strategy:
                            
                            log.debug (f"strategy {strategy} {currency.upper()}-START")
                            
                            max_position: int = notional * -1
                            
                            hedging = HedgingSpot(strategy,
                                                    strategy_params,
                                                    max_position,
                                                    my_trades_currency_strategy,
                                                    TA_result_data,
                                                    index_price,
                                                    server_time)
                            
                            send_order: dict = await hedging.is_send_and_cancel_open_order_allowed (
                                non_checked_strategies,
                                instrument_ticker,
                                active_futures,
                                orders_currency_strategy,
                                best_ask_prc,
                                )
                            
                            if send_order["order_allowed"]:
                                
                                #log.error  (f"send_order {send_order}")
                                result_order = await self.modify_order_and_db.if_order_is_true(
                                    non_checked_strategies,
                                    send_order, 
                                    instrument_ticker
                                    )

                                if result_order:
                                    
                                    log.error (f"result_order {result_order}")
                                    
                                    data_orders = result_order["result"]
                                    await self.update_user_changes_non_ws (
                                        non_checked_strategies,
                                        data_orders, 
                                        currency,
                                        order_db_table,
                                        trade_db_table,
                                        archive_db_table,
                                        transaction_log_trading
                                        )
                                    
                                    await sleep_and_restart ()
                                
                                await self.modify_order_and_db.if_cancel_is_true(send_order)

                            if my_trades_currency_strategy_open !=[]:
                                                                                                            
                                best_bid_prc: float = perpetual_ticker["best_bid_price"]
                                
                                get_prices_in_label_transaction_main = [o["price"] for o in my_trades_currency_strategy_open]

                                closest_price = get_closest_value(get_prices_in_label_transaction_main, best_bid_prc)

                                nearest_transaction_to_index = [o for o in my_trades_currency_strategy_open \
                                    if o["price"] == closest_price]
                                #log.debug (f"nearest_transaction_to_index {nearest_transaction_to_index}")
                                
                                send_closing_order: dict = await hedging.is_send_exit_order_allowed (
                                    orders_currency_strategy_label_closed,
                                    best_bid_prc,
                                    nearest_transaction_to_index,
                                    )

                                log.error (f"send_closing_order {send_closing_order}")
                                result_order = await self.modify_order_and_db.if_order_is_true(
                                    non_checked_strategies,
                                    send_closing_order, 
                                    instrument_ticker,
                                    )
                                
                                if result_order:
                                    log.error (f"result_order {result_order}")
                                    data_orders = result_order["result"]
                                    await self.update_user_changes_non_ws (
                                        non_checked_strategies,
                                        data_orders, 
                                        currency, 
                                        order_db_table,
                                        trade_db_table, 
                                        archive_db_table,
                                        transaction_log_trading)
                                    await sleep_and_restart ()
                                    
                                await self.modify_order_and_db.if_cancel_is_true(send_closing_order)

                            log.debug (f"strategy {strategy}-DONE")
            
    async def update_user_changes(
        self,
        non_checked_strategies,
        data_orders, 
        currency, 
        order_db_table,
        trade_db_table,
        archive_db_table,
        transaction_log_trading
        ) -> None:
        
        trades = data_orders["trades"]
        
        orders = data_orders["orders"]

        instrument_name = data_orders["instrument_name"]
        
        log.critical (f"update_user_changes {instrument_name} -START")
        
        log.info (f" {data_orders}")
        
        if orders:
            
            if trades:
                for trade in trades:
                
                    if f"{currency.upper()}-FS-" not in instrument_name:
                    
                        await saving_traded_orders(
                            trade,
                            trade_db_table,
                            order_db_table
                            )                      

                        await saving_traded_orders(
                            trade, 
                            archive_db_table, 
                            order_db_table
                            )
                
            else:
                for order in orders:
                    
                    await self.saving_order(non_checked_strategies,
                                            instrument_name,
                                            order,
                                            order_db_table)
                                            
                await self.modify_order_and_db. resupply_sub_accountdb(currency)            
        #log.debug (f"positions {positions}")
        await update_db_pkl(
            "positions", 
            data_orders,
            currency)

        await self.modify_order_and_db.resupply_transaction_log(
            currency,
            transaction_log_trading,
            archive_db_table)
        
        #await update_trades_from_exchange (currency,
        #                                   archive_db_table,
        #                                   10)

        log.info(f"update_user_changes-END")
    

    async def saving_order (
        self,
        non_checked_strategies,
        instrument_name,
        order,
        order_db_table
        ) -> None:

                    
        order_state= order["order_state"]
        #log.info (f"""order_state == "cancelled" or order_state == "filled" {order_state} {order_state == "cancelled" or order_state == "filled"}""")
        if order_state == "cancelled" or order_state == "filled":
            await saving_orders (order_db_table, 
                                 order)
        
        else:
            
            label= order["label"]

            order_id= order["order_id"]    
            
            label_and_side_consistent= is_label_and_side_consistent(non_checked_strategies,
                                                                    order)
            
            #log.critical (f' ORDERS label_and_side_consistent {label_and_side_consistent} label {label} everything_NOT_consistent {not label_and_side_consistent}')
            
            if label_and_side_consistent and label:
                
                await saving_orders (order_db_table, order)
                
            # check if transaction has label. Provide one if not any
            if  not label_and_side_consistent:
            
                await insert_tables(order_db_table, order)

                await self.modify_order_and_db. cancel_by_order_id (order_id)                    
            
                await telegram_bot_sendtext('size or open order is inconsistent', "general_error")

            if not label:
                
                await labelling_the_unlabelled_and_resend_it(non_checked_strategies,
                                                             order,
                                                             instrument_name)
    
                    
                    
                    
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
