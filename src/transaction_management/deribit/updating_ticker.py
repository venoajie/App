# -*- coding: utf-8 -*-

import asyncio

from loguru import logger as log
import orjson

from db_management.redis_client import saving_result
from messaging.telegram_bot import telegram_bot_sendtext
from transaction_management.deribit.api_requests import get_tickers
from utilities.pickling import read_data
from utilities.system_tools import (
    parse_error_message,
    provide_path_for_file,
)

from transaction_management.deribit.get_instrument_summary import (
    get_futures_instruments,
)
from utilities.string_modification import (
    extract_currency_from_text,
    remove_double_brackets_in_list,
    remove_redundant_elements,
)


def reading_from_pkl_data(
    end_point: str,
    currency: str,
    status: str = None,
) -> dict:
    """ """

    path: str = provide_path_for_file(end_point, currency, status)
    return read_data(path)


def combining_ticker_data(instruments_name: str) -> list:
    """_summary_
    https://blog.apify.com/python-cache-complete-guide/]
    https://medium.com/@jodielovesmaths/memoization-in-python-using-cache-36b676cb21ef
    data caching
    https://medium.com/@ryan_forrester_/python-return-statement-complete-guide-138c80bcfdc7

    Args:
        instrument_ticker (_type_): _description_

    Returns:
        _type_: _description_
    """

    result = []
    for instrument_name in instruments_name:

        result_instrument = reading_from_pkl_data("ticker", instrument_name)

        if result_instrument:
            result_instrument = result_instrument[0]

        else:
            result_instrument = get_tickers(instrument_name)
        result.append(result_instrument)

    return result


async def update_cached_ticker(
    client_redis: object,
    config_app: list,
) -> None:
    """_summary_
    https://stackoverflow.com/questions/73064997/update-values-in-a-list-of-dictionaries

    Args:
        instrument_ticker (_type_): _description_

    Returns:
        _type_: _description_
    """

    try:

        # connecting to redis pubsub
        pubsub: object = client_redis.pubsub()

        # get tradable strategies
        tradable_config_app = config_app["tradable"]

        # get TRADABLE currencies
        currencies = [o["spot"] for o in tradable_config_app][0]

        strategy_attributes = config_app["strategies"]

        settlement_periods = get_settlement_period(strategy_attributes)

        futures_instruments = await get_futures_instruments(
            currencies, settlement_periods
        )

        instruments_name = futures_instruments["instruments_name"]
        
        # get redis channels
        redis_channels: dict = config_app["redis_channels"][0]
        ticker_update_channel: str = redis_channels["ticker_update"]

        redis_keys: dict = config_app["redis_keys"][0]
        ticker_keys: str = redis_keys["ticker"]

        # prepare channels placeholders
        channels = [
            ticker_update_channel,
        ]

        # subscribe to channels
        [await pubsub.subscribe(o) for o in channels]
        
        ticker_all = combining_ticker_data(instruments_name)

        while True:

            try:
                
                message_byte = await pubsub.get_message()

                if message_byte and message_byte["type"] == "message":

                    message_byte_data = orjson.loads(message_byte["data"])
                    
                    message_channel = message_byte_data["channel"]  
                    
                    data = message_byte_data["data"]                  

                    instrument_name = message_byte_data["instrument_name"]                  

                    if ticker_update_channel in message_channel:

                        for item in data:

                            if (
                                "stats" not in item
                                and "instrument_name" not in item
                                and "type" not in item
                            ):
                                [o for o in ticker_all if instrument_name in o["instrument_name"]][0][
                                    item
                                ] = data[item]

                            if "stats" in item:

                                data_orders_stat = data[item]

                                for item in data_orders_stat:
                                    [o for o in ticker_all if instrument_name in o["instrument_name"]][0][
                                        "stats"
                                    ][item] = data_orders_stat[item]
                                    
                        
                        await saving_result(
                            client_redis,
                            ticker_update_channel,
                            ticker_keys,
                            ticker_all,
                            )
                            

            except Exception as error:

                parse_error_message(error)

                await telegram_bot_sendtext(
                    f"updating ticker - {error}",
                    "general_error",
                )

                continue

            finally:
                await asyncio.sleep(0.001)

    except Exception as error:

        parse_error_message(f"updating ticker {error}")

        await telegram_bot_sendtext(
            f"updating ticker - {error}",
            "general_error",
        )

# Using the LRUCache decorator function with a maximum cache size of 3
async def combining_order_data(
    private_data: object,
    currencies: list,
) -> list:
    """ """

    from utilities.pickling import replace_data

    result = []
    for currency in currencies:

        sub_accounts = await private_data.get_subaccounts_details(currency)

        my_path_sub_account = provide_path_for_file("sub_accounts", currency)

        replace_data(
            my_path_sub_account,
            sub_accounts,
        )

        if sub_accounts:

            sub_account = sub_accounts[0]

            sub_account_orders = sub_account["open_orders"]

            if sub_account_orders:

                for order in sub_account_orders:

                    result.append(order)

    return result

async def update_cached_orders(
    queue_orders_all,
    queue_orders,
):
    """_summary_
    https://stackoverflow.com/questions/73064997/update-values-in-a-list-of-dictionaries

    Args:
        instrument_ticker (_type_): _description_

    Returns:
        _type_: _description_
    """

    try:
        
        log.debug(f" queue_orders {queue_orders}")
        log.warning(f" queue_orders_all {queue_orders_all}")

        orders_all = queue_orders_all

        if queue_orders:

            orders = queue_orders["orders"]

            trades = queue_orders["trades"]

            if orders:

                if trades:

                    for trade in trades:

                        order_id = trade["order_id"]

                        selected_order = [
                            o for o in orders_all if order_id in o["order_id"]
                        ]

                        if selected_order:

                            orders_all.remove(selected_order[0])

                if orders:

                    for order in orders:

                        # print(f"cached order {order}")

                        order_state = order["order_state"]

                        if order_state == "cancelled" or order_state == "filled":

                            order_id = order["order_id"]

                            selected_order = [
                                o for o in orders_all if order_id in o["order_id"]
                            ]

                            # print(f"caching selected_order {selected_order}")

                            if selected_order:

                                orders_all.remove(selected_order[0])

                        else:

                            orders_all.append(order)

    except Exception as error:

        parse_error_message(error)



def get_settlement_period(strategy_attributes: list) -> list:

    return remove_redundant_elements(
        remove_double_brackets_in_list(
            [o["settlement_period"] for o in strategy_attributes]
        )
    )

