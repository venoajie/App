#!/usr/bin/python3
# -*- coding: utf-8 -*-

# built ins
import asyncio

import numpy as np
import uvloop

# installed
from loguru import logger as log
import orjson

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
    update_cached_orders,
)
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
    client_redis: object,
    config_app: list,
    )-> None:
    """ """

    try:

        # get tradable strategies
        tradable_config_app = config_app["tradable"]

        # get TRADABLE currencies
        currencies: list = [o["spot"] for o in tradable_config_app][0]

        strategy_attributes = config_app["strategies"]

        cancellable_strategies = [
            o["strategy_label"] for o in strategy_attributes if o["cancellable"] == True
        ]

        relevant_tables = config_app["relevant_tables"][0]

        order_db_table = relevant_tables["orders_table"]

        for currency in currencies:
            await modify_order_and_db.cancel_the_cancellables(
                order_db_table, currency, cancellable_strategies
            )

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

        # resolutions = [60, 15, 5]
        # qty_candles = 5
        # dim_sequence = 3

        # cached_candles_data = combining_candles_data(
        #    np, currencies, qty_candles, resolutions, dim_sequence
        # )

        # ticker_all = cached_ticker(instruments_name)

        # cached_orders: list = await combining_order_data(private_data, currencies)

        # server_time = 0
        
        CHANNEL_NAME = "notification"
        
        not_cancel = True

        pubsub = client_redis.pubsub()
        
        await pubsub.subscribe(CHANNEL_NAME)                      
        
        while not_cancel:
  
            try:
                
                message = await pubsub.get_message()
                
                if message and message["type"] == "message":
            
                    message = orjson.loads(message["data"])["message"]
                    
                    message_params = (message["message_params"])

                    message_channel,data = (
                        message_params["channel"],
                     message_params["data"],
                     )

                    log.debug (message["sequence"])

                    cached_orders, ticker_all = (
                        message["cached_orders"],
                        message["ticker_all"],
                    )

                    chart_trade, server_time = (
                        message["chart_trade"],
                        message["server_time"],
                    )

                    currency: str = message["currency"]

                    currency_upper: str = currency.upper()

                    currency_lower: str = currency

                    # if "user.changes.any" in message_channel:


                    #    await update_cached_orders(cached_orders, data_orders)

                    instrument_name_perpetual = f"{currency_upper}-PERPETUAL"

                    # instrument_name_future = (message_channel)[19:]

                    # if message_channel == f"incremental_ticker.{instrument_name_future}":

                    #    update_cached_ticker(
                    #        instrument_name_future,
                    #        ticker_all,
                    #        data_orders,
                    #    )

                    #    server_time = data_orders["timestamp"] + server_time if server_time == 0 else data_orders["timestamp"]

                    # hart_trade = await chart_trade_in_msg(
                    #    message_channel,
                    #   data_orders,
                    #  cached_candles_data,
                    # )

                    if not chart_trade and server_time != 0:

                        # get portfolio data
                        portfolio = reading_from_pkl_data("portfolio", currency)[0]

                        equity: float = portfolio["equity"]

                        ticker_perpetual_instrument_name = [
                            o
                            for o in ticker_all
                            if instrument_name_perpetual in o["instrument_name"]
                        ][0]

                        index_price = get_index(ticker_perpetual_instrument_name)

                        sub_account = reading_from_pkl_data("sub_accounts", currency)

                        sub_account = sub_account[0]

                        market_condition = message["market_condition"]

                        if sub_account:

                            query_trades = (
                                f"SELECT * FROM  v_{currency_lower}_trading_active"
                            )

                            my_trades_currency_all_transactions: list = (
                                await executing_query_with_return(query_trades)
                            )

                            my_trades_currency_all: list = (
                                []
                                if my_trades_currency_all_transactions == 0
                                else [
                                    o
                                    for o in my_trades_currency_all_transactions
                                    if o["instrument_name"]
                                    in [
                                        o["instrument_name"]
                                        for o in instrument_attributes_futures_all
                                    ]
                                ]
                            )

                            orders_currency = (
                                []
                                if not cached_orders
                                else [
                                    o
                                    for o in cached_orders
                                    if currency_upper in o["instrument_name"]
                                ]
                            )

                            log.warning(f"orders_currency {len(orders_currency)}")

                            position = [o for o in sub_account["positions"]]
                            # log.debug (f"position {position}")
                            position_without_combo = [
                                o
                                for o in position
                                if f"{currency_upper}-FS" not in o["instrument_name"]
                            ]

                            if index_price is not None and equity > 0:

                                size_perpetuals_reconciled = (
                                    is_size_sub_account_and_my_trades_reconciled(
                                        position_without_combo,
                                        my_trades_currency_all,
                                        instrument_name_perpetual,
                                    )
                                )
                                my_trades_currency: list = [
                                    o
                                    for o in my_trades_currency_all
                                    if o["label"] is not None
                                ]

                                notional: float = compute_notional_value(
                                    index_price, equity
                                )

                                for strategy in active_strategies:

                                    strategy_params = [
                                        o
                                        for o in strategy_attributes
                                        if o["strategy_label"] == strategy
                                    ][0]

                                    my_trades_currency_strategy = [
                                        o
                                        for o in my_trades_currency
                                        if strategy in (o["label"])
                                    ]

                                    orders_currency_strategy = (
                                        []
                                        if not orders_currency
                                        else [
                                            o
                                            for o in orders_currency
                                            if strategy in (o["label"])
                                        ]
                                    )

                                    if "futureSpread" in strategy:

                                        strategy_params = [
                                            o
                                            for o in strategy_attributes
                                            if o["strategy_label"] == strategy
                                        ][0]

                                        combo_auto = ComboAuto(
                                            strategy,
                                            strategy_params,
                                            orders_currency_strategy,
                                            server_time,
                                            market_condition,
                                            my_trades_currency_strategy,
                                            ticker_perpetual_instrument_name,
                                        )

                                        if orders_currency_strategy:
                                            for order in orders_currency_strategy:
                                                cancel_allowed: dict = await combo_auto.is_cancelling_orders_allowed(
                                                    order,
                                                    server_time,
                                                )

                                                if cancel_allowed["cancel_allowed"]:
                                                    await modify_order_and_db.if_cancel_is_true(
                                                        order_db_table,
                                                        cancel_allowed,
                                                    )

                                                    not_cancel = False

                                                    break

                                    if (
                                        "hedgingSpot" in strategy
                                        and size_perpetuals_reconciled
                                    ):

                                        max_position: int = notional * -1

                                        hedging = HedgingSpot(
                                            strategy,
                                            strategy_params,
                                            max_position,
                                            my_trades_currency_strategy,
                                            market_condition,
                                            index_price,
                                            my_trades_currency_all,
                                        )

                                        if orders_currency_strategy:

                                            for order in orders_currency_strategy:
                                                cancel_allowed: dict = await hedging.is_cancelling_orders_allowed(
                                                    order,
                                                    orders_currency_strategy,
                                                    server_time,
                                                )

                                                log.error(
                                                    f"cancel_allowed {cancel_allowed} {order} server_time {server_time}"
                                                )

                                                if cancel_allowed["cancel_allowed"]:
                                                    await modify_order_and_db.if_cancel_is_true(
                                                        order_db_table,
                                                        cancel_allowed,
                                                    )

                                                    not_cancel = False

                                                    break
                            
            except Exception as error:
                parse_error_message (error)
                continue
                
            finally:
                await asyncio.sleep(.0001) 

            
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


def get_index(ticker: dict) -> float:

    try:

        index_price = ticker["index_price"]

    except:

        index_price = []

    if index_price == []:
        index_price = ticker["estimated_delivery_price"]

    return index_price
