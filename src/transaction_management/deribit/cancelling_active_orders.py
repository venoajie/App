#!/usr/bin/python3
# -*- coding: utf-8 -*-

# built ins
import asyncio

import numpy as np
import uvloop

# installed
from loguru import logger as log

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

from data_cleaning.reconciling_db import (
    is_size_sub_account_and_my_trades_reconciled,
)
from db_management.sqlite_management import executing_query_with_return
from market_understanding.price_action.candles_analysis import (
    combining_candles_data,
    get_market_condition,
)
from messaging.telegram_bot import telegram_bot_sendtext
from strategies.cash_carry.combo_auto import ComboAuto
from strategies.hedging.hedging_spot import HedgingSpot
from transaction_management.deribit.get_instrument_summary import (
    get_futures_instruments,
)
from transaction_management.deribit.managing_deribit import (
#    ModifyOrderDb,
    currency_inline_with_database_address,
)
from utilities.caching import (
    combining_ticker_data as cached_ticker,
    combining_order_data, 
    update_cached_orders)
from utilities.caching import update_cached_ticker
from utilities.pickling import read_data, replace_data
from utilities.string_modification import (
    extract_currency_from_text,
    remove_double_brackets_in_list,
    remove_redundant_elements,
)

from utilities.system_tools import parse_error_message, provide_path_for_file


async def update_db_pkl(path: str, data_orders: dict, currency: str) -> None:

    my_path_portfolio = provide_path_for_file(path, currency)

    if currency_inline_with_database_address(currency, my_path_portfolio):

        replace_data(my_path_portfolio, data_orders)


async def cancelling_orders(
    private_data: object,
    modify_order_and_db: object,
    config_app: list,
    queue: object,
):
    """ """

    try:

        # get tradable strategies
        tradable_config_app = config_app["tradable"]

        # get TRADABLE currencies
        currencies: list = [o["spot"] for o in tradable_config_app][0]
        
        strategy_attributes = config_app["strategies"]

        strategy_attributes_active = [
            o for o in strategy_attributes if o["is_active"] == True
        ]

        active_strategies = [o["strategy_label"] for o in strategy_attributes_active]

        relevant_tables = config_app["relevant_tables"][0]

        order_db_table = relevant_tables["orders_table"]

        settlement_periods = get_settlement_period(strategy_attributes)

        futures_instruments = await get_futures_instruments(
            currencies,
            settlement_periods,
        )

        instrument_attributes_futures_all = futures_instruments["active_futures"]

        instruments_name = futures_instruments["instruments_name"]

        # filling currencies attributes
        my_path_cur = provide_path_for_file("currencies")
        replace_data(my_path_cur, currencies)

        resolutions = [60, 15, 5]
        qty_candles = 5
        dim_sequence = 3

        cached_candles_data = combining_candles_data(
            np, currencies, qty_candles, resolutions, dim_sequence
        )

        ticker_all = cached_ticker(instruments_name)

        cached_orders: list = await combining_order_data(private_data, currencies)
        
        print (f"queue.empty() cancelling {queue.empty()}")
                
        while not queue.empty():
            message_params: str = await queue.get()
            # message: str = queue.get()
            log.debug(f"len_msg {message_params}")

            queue.task_done

    except Exception as error:

        parse_error_message(error)

        await telegram_bot_sendtext(
            f"cancelling active orders - {error}", "general_error"
        )


def get_settlement_period(strategy_attributes) -> list:

    return remove_redundant_elements(
        remove_double_brackets_in_list(
            [o["settlement_period"] for o in strategy_attributes]
        )
    )


async def chart_trade_in_msg(
    message_channel,
    data_orders,
    candles_data,
):
    """ """

    if "chart.trades" in message_channel:
        tick_from_exchange = data_orders["tick"]

        tick_from_cache = max(
            [o["max_tick"] for o in candles_data if o["resolution"] == 5]
        )

        if tick_from_exchange <= tick_from_cache:
            return True

        else:

            log.warning("update ohlc")

    else:

        return False


def reading_from_pkl_data(end_point, currency, status: str = None) -> dict:
    """ """

    path: str = provide_path_for_file(end_point, currency, status)
    return read_data(path)


def compute_notional_value(index_price: float, equity: float) -> float:
    """ """
    return index_price * equity


def get_index(data_orders: dict, ticker: dict) -> float:

    try:
        index_price = data_orders["index_price"]

    except:

        index_price = ticker["index_price"]

        if index_price == []:
            index_price = ticker["estimated_delivery_price"]

    return index_price
