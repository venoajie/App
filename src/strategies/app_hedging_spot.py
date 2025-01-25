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
from strategies.hedging.hedging_spot import (
    HedgingSpot,
    modify_hedging_instrument,
)
from transaction_management.deribit.get_instrument_summary import (
    get_futures_instruments,
)
from transaction_management.deribit.managing_deribit import (
#    ModifyOrderDb,
    currency_inline_with_database_address,
)
from transaction_management.deribit.processing_orders import processing_orders
from utilities.caching import (
    combining_ticker_data as cached_ticker,
    combining_order_data, 
    update_cached_orders)
from utilities.caching import update_cached_ticker
from utilities.number_modification import get_closest_value
from utilities.pickling import read_data, replace_data
from utilities.string_modification import (
    extract_currency_from_text,
    parsing_label,
    remove_double_brackets_in_list,
    remove_redundant_elements,
)
from utilities.system_tools import (
    parse_error_message,
    provide_path_for_file,
)


async def update_db_pkl(path: str, data_orders: dict, currency: str) -> None:

    my_path_portfolio = provide_path_for_file(path, currency)

    if currency_inline_with_database_address(currency, my_path_portfolio):

        replace_data(my_path_portfolio, data_orders)


async def hedging_spot(
    private_data: object,
    modify_order_and_db: object,
    config_app: list,
    queue: object,
    has_order: object
):
    """ """

    strategy = "hedgingSpot"
    log.critical(f"starting {strategy}")

    try:

#        modify_order_and_db: object = ModifyOrderDb(sub_account_id)

        # get tradable strategies
        tradable_config_app = config_app["tradable"]

        # get tradable currencies
        # currencies_spot= ([o["spot"] for o in tradable_config_app]) [0]
        currencies = ([o["spot"] for o in tradable_config_app])[0]

        # currencies= random.sample(currencies_spot,len(currencies_spot))

        strategy_attributes = config_app["strategies"]

        strategy_attributes_active = [
            o for o in strategy_attributes if o["is_active"] == True
        ]

        active_strategies = [o["strategy_label"] for o in strategy_attributes_active]

        # get strategies that have not short/long attributes in the label
        non_checked_strategies = [
            o["strategy_label"]
            for o in strategy_attributes_active
            if o["non_checked_for_size_label_consistency"] == True
        ]

        contribute_to_hedging_strategies = [
            o["strategy_label"]
            for o in strategy_attributes_active
            if o["contribute_to_hedging"] == True
        ]

        relevant_tables = config_app["relevant_tables"][0]

        trade_db_table = relevant_tables["my_trades_table"]

        settlement_periods = get_settlement_period(strategy_attributes)

        futures_instruments = await get_futures_instruments(
            currencies,
            settlement_periods,
        )

        instrument_attributes_futures_all = futures_instruments["active_futures"]

        instruments_name = futures_instruments["instruments_name"]

        resolutions = [60, 15, 5]
        qty_candles = 5
        dim_sequence = 3

        cached_candles_data = combining_candles_data(
            np, currencies, qty_candles, resolutions, dim_sequence
        )

        ticker_all = cached_ticker(instruments_name)
        
        cached_orders: list = await combining_order_data(private_data, currencies)
        
        server_time_zero = 0

        while await has_order.acquire():
        
            try:

                not_order = True

                while not_order:

                    message_params = queue.get_nowait()

                    message_channel: str = message_params["channel"]
                    log.critical(f"message_channel {message_channel}")

                    data_orders: dict = message_params["data"]

                    if "user.changes.any" in message_channel:
                        
                        log.error (f"data_orders user.changes.any {data_orders}")
                        
                        await update_cached_orders(cached_orders, data_orders)                                    

                    currency: str = extract_currency_from_text(
                            message_channel
                        )

                    currency_upper: str = currency.upper()

                    currency_lower: str = currency
                    instrument_name_perpetual = f"{currency_upper}-PERPETUAL"

                    instrument_name_future = (message_channel)[19:]
                    if message_channel == f"incremental_ticker.{instrument_name_future}":

                        update_cached_ticker(
                            instrument_name_future,
                            ticker_all,
                            data_orders,
                        )

                        server_time = data_orders["timestamp"] + server_time_zero if server_time_zero == 0 else data_orders["timestamp"]
    
                    chart_trade = await chart_trade_in_msg(
                        message_channel,
                        data_orders,
                        cached_candles_data,
                    )
                    
                            

                    archive_db_table: str = f"my_trades_all_{currency_lower}_json"

                    if not chart_trade and server_time != 0:

                        archive_db_table = f"my_trades_all_{currency_lower}_json"

                        # get portfolio data
                        portfolio = reading_from_pkl_data("portfolio", currency)[0]

                        equity: float = portfolio["equity"]

                        ticker_perpetual_instrument_name = [
                            o
                            for o in ticker_all
                            if instrument_name_perpetual in o["instrument_name"]
                        ][0]

                        index_price = get_index(
                            data_orders, ticker_perpetual_instrument_name
                        )

                        sub_account = reading_from_pkl_data("sub_accounts", currency)

                        sub_account = sub_account[0]

                        # sub_account_orders = sub_account["open_orders"]

                        market_condition = get_market_condition(
                            np, cached_candles_data, currency_upper
                        )

                        log.warning(market_condition)

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

                            len_cleaned_orders = len(orders_currency)
                            
                            log.info (f"orders_currency {orders_currency}")

                            # if orders_currency:

                            position = [o for o in sub_account["positions"]]
                            # log.debug (f"position {position}")
                            position_without_combo = [
                                o
                                for o in position
                                if f"{currency_upper}-FS" not in o["instrument_name"]
                            ]

                            size_perpetuals_reconciled = (
                                is_size_sub_account_and_my_trades_reconciled(
                                    position_without_combo,
                                    my_trades_currency_all,
                                    instrument_name_perpetual,
                                )
                            )

                            if not size_perpetuals_reconciled:

                                not_order = False

                                break

                            if index_price is not None and equity > 0:
                                my_trades_currency: list = [

                                    o
                                    for o in my_trades_currency_all
                                    if o["label"] is not None
                                ]

                                my_trades_currency_contribute_to_hedging = [
                                    o
                                    for o in my_trades_currency
                                    if (parsing_label(o["label"])["main"])
                                    in contribute_to_hedging_strategies
                                ]

                                my_trades_currency_contribute_to_hedging_sum = (
                                    0
                                    if not my_trades_currency_contribute_to_hedging
                                    else sum(
                                        [
                                            o["amount"]
                                            for o in my_trades_currency_contribute_to_hedging
                                        ]
                                    )
                                )

                                ONE_PCT = 1 / 100

                                INSTRUMENT_EXPIRATION_THRESHOLD = 60 * 8  # 8 hours

                                ONE_SECOND = 1000

                                ONE_MINUTE = ONE_SECOND * 60

                                notional: float = compute_notional_value(
                                    index_price, equity
                                )

                                if (
                                    strategy in active_strategies
                                    and size_perpetuals_reconciled
                                ):

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

                                    log.info(
                                        f"orders_currency_strategy {len(orders_currency_strategy)}  {currency}"
                                    )
                                    log.info(
                                        f" {(orders_currency_strategy)} "
                                    )

                                    log.warning(f"strategy {strategy}-START")

                                    instrument_attributes_futures_for_hedging = [
                                        o
                                        for o in futures_instruments["active_futures"]
                                        if o["settlement_period"] != "month"
                                        and o["kind"] == "future"
                                    ]

                                    strong_bearish = market_condition["strong_bearish"]

                                    bearish = market_condition["bearish"]

                                    max_position: int = notional * -1

                                    instrument_ticker = await modify_hedging_instrument(
                                        strong_bearish,
                                        bearish,
                                        instrument_attributes_futures_for_hedging,
                                        ticker_all,
                                        ticker_perpetual_instrument_name,
                                        currency_upper,
                                    )

                                    instrument_name = instrument_ticker[
                                        "instrument_name"
                                    ]

                                    size_future_reconciled = (
                                        is_size_sub_account_and_my_trades_reconciled(
                                            position_without_combo,
                                            my_trades_currency_all,
                                            instrument_name,
                                        )
                                    )

                                    instrument_time_left = (
                                        max(
                                            [
                                                o["expiration_timestamp"]
                                                for o in instrument_attributes_futures_all
                                                if o["instrument_name"]
                                                == instrument_name
                                            ]
                                        )
                                        - server_time
                                    ) / ONE_MINUTE

                                    instrument_time_left_exceed_threshold = (
                                        instrument_time_left
                                        > INSTRUMENT_EXPIRATION_THRESHOLD
                                    )

                                    if not size_future_reconciled:

                                        queue.task_done
                                        
                                        not_order = False

                                        break

                                    if size_future_reconciled:

                                        hedging = HedgingSpot(
                                            strategy,
                                            strategy_params,
                                            max_position,
                                            my_trades_currency_strategy,
                                            market_condition,
                                            index_price,
                                            my_trades_currency_all,
                                        )

                                        # something was wrong because perpetuals were actively traded. cancell  orders
                                        if (
                                            instrument_time_left_exceed_threshold
                                            and len_cleaned_orders < 50
                                        ):

                                            best_ask_prc: float = instrument_ticker[
                                                "best_ask_price"
                                            ]

                                            send_order: dict = (
                                                await hedging.is_send_open_order_allowed(
                                                    non_checked_strategies,
                                                    instrument_name,
                                                    instrument_attributes_futures_for_hedging,
                                                    orders_currency_strategy,
                                                    best_ask_prc,
                                                    archive_db_table,
                                                    trade_db_table,
                                                )
                                            )

                                            if send_order["order_allowed"]:

                                                await processing_orders(
                                                    modify_order_and_db,
                                                    config_app,
                                                    send_order,
                                                )

                                                queue.task_done
                                                
                                                not_order = False

                                                break

                                            status_transaction = [
                                                "open",
                                                "closed",
                                            ]

                                            if len_cleaned_orders < 50:

                                                # log.error (f"{orders_currency_strategy} ")

                                                for status in status_transaction:

                                                    my_trades_currency_strategy_status = [
                                                        o
                                                        for o in my_trades_currency_strategy
                                                        if status in (o["label"])
                                                    ]

                                                    orders_currency_strategy_label_contra_status = [
                                                        o
                                                        for o in orders_currency_strategy
                                                        if status not in o["label"]
                                                    ]

                                                    # log.error (f"{status} ")

                                                    if my_trades_currency_strategy_status:

                                                        transaction_instrument_name = remove_redundant_elements(
                                                            [
                                                                o["instrument_name"]
                                                                for o in my_trades_currency_strategy_status
                                                            ]
                                                        )

                                                        for (
                                                            instrument_name
                                                        ) in (
                                                            transaction_instrument_name
                                                        ):

                                                            instrument_ticker: list = [
                                                                o
                                                                for o in ticker_all
                                                                if instrument_name
                                                                in o["instrument_name"]
                                                            ]

                                                            if instrument_ticker:

                                                                instrument_ticker = (
                                                                    instrument_ticker[0]
                                                                )

                                                                get_prices_in_label_transaction_main = [
                                                                    o["price"]
                                                                    for o in my_trades_currency_strategy_status
                                                                    if instrument_name
                                                                    in o[
                                                                        "instrument_name"
                                                                    ]
                                                                ]

                                                                log.error(
                                                                    f"my_trades_currency_contribute_to_hedging_sum {my_trades_currency_contribute_to_hedging_sum}"
                                                                )

                                                                if (
                                                                    status == "open"
                                                                    and my_trades_currency_contribute_to_hedging_sum
                                                                    <= 0
                                                                ):

                                                                    best_bid_prc: (
                                                                        float
                                                                    ) = instrument_ticker[
                                                                        "best_bid_price"
                                                                    ]

                                                                    closest_price = get_closest_value(
                                                                        get_prices_in_label_transaction_main,
                                                                        best_bid_prc,
                                                                    )

                                                                    nearest_transaction_to_index = [
                                                                        o
                                                                        for o in my_trades_currency_strategy_status
                                                                        if o["price"]
                                                                        == closest_price
                                                                    ]

                                                                    send_closing_order: (
                                                                        dict
                                                                    ) = await hedging.is_send_exit_order_allowed(
                                                                        orders_currency_strategy_label_contra_status,
                                                                        best_bid_prc,
                                                                        nearest_transaction_to_index,
                                                                        # orders_currency_strategy
                                                                    )

                                                                    if send_order[
                                                                        "order_allowed"
                                                                    ]:

                                                                        await processing_orders(
                                                                            send_closing_order,
                                                                            config_app,
                                                                            modify_order_and_db,
                                                                        )

                                                                        queue.task_done
                                                                        
                                                                        not_order = (
                                                                            False
                                                                        )

                                                                        break

                                                                if status == "closed":

                                                                    best_ask_prc: (
                                                                        float
                                                                    ) = instrument_ticker[
                                                                        "best_ask_price"
                                                                    ]

                                                                    closest_price = get_closest_value(
                                                                        get_prices_in_label_transaction_main,
                                                                        best_ask_prc,
                                                                    )

                                                                    nearest_transaction_to_index = [
                                                                        o
                                                                        for o in my_trades_currency_strategy_status
                                                                        if o["price"]
                                                                        == closest_price
                                                                    ]

                                                                    send_closing_order: (
                                                                        dict
                                                                    ) = await hedging.send_contra_order_for_orphaned_closed_transctions(
                                                                        orders_currency_strategy_label_contra_status,
                                                                        best_ask_prc,
                                                                        nearest_transaction_to_index,
                                                                        # orders_currency_strategy
                                                                    )

                                                                    if send_order[
                                                                        "order_allowed"
                                                                    ]:

                                                                        await processing_orders(
                                                                            modify_order_and_db,
                                                                            config_app,
                                                                            send_closing_order,
                                                                        )

                                                                        queue.task_done
                                                                        
                                                                        not_order = (
                                                                            False
                                                                        )

                                                                        break
                    
                queue.task_done

                not_order = False

            except asyncio.QueueEmpty:
                continue
                    # check for stop
            if message_params is None:
                break

    except Exception as error:

        parse_error_message(f"app hedging spot {error}")

        await telegram_bot_sendtext(f"app hedging spot-{error}", "general_error")


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
            # await sleep_and_restart()

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
