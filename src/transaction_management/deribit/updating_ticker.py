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
from websocket_management.allocating_ohlc import (
    inserting_open_interest,
    ohlc_result_per_time_frame,
)

from transaction_management.deribit.get_instrument_summary import (
    get_futures_instruments,
)
from utilities.string_modification import (
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

        resolution: int = 1

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
        ticker_channel: str = redis_channels["ticker_update"]

        redis_keys: dict = config_app["redis_keys"][0]
        ticker_keys: str = redis_keys["ticker"]

        # prepare channels placeholders
        channels = [ticker_channel]

        # subscribe to channels
        [await pubsub.subscribe(o) for o in channels]

        ticker_all = combining_ticker_data(instruments_name)

        while True:

            try:

                message_byte = await pubsub.get_message()

                if message_byte and message_byte["type"] == "message":

                    message_byte_data = orjson.loads(message_byte["data"])

                    message_channel =message_byte["channel"]

                    if ticker_channel in message_channel:

                        data = message_byte_data["data"]

                        instrument_name = message_byte_data["instrument_name"]

                        currency = message_byte_data["currency"]

                        for item in data:

                            if (
                                "stats" not in item
                                and "instrument_name" not in item
                                and "type" not in item
                            ):
                                [
                                    o
                                    for o in ticker_all
                                    if instrument_name in o["instrument_name"]
                                ][0][item] = data[item]

                            if "stats" in item:

                                data_orders_stat = data[item]

                                for item in data_orders_stat:
                                    [
                                        o
                                        for o in ticker_all
                                        if instrument_name in o["instrument_name"]
                                    ][0]["stats"][item] = data_orders_stat[item]

                        await saving_result(
                            client_redis,
                            ticker_channel,
                            ticker_keys,
                            ticker_all,
                        )

                        if "PERPETUAL" in instrument_name:

                            WHERE_FILTER_TICK: str = "tick"

                            TABLE_OHLC1: str = f"ohlc{resolution}_{currency}_perp_json"

                            await inserting_open_interest(
                                currency,
                                WHERE_FILTER_TICK,
                                TABLE_OHLC1,
                                data,
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


def get_settlement_period(strategy_attributes: list) -> list:

    return remove_redundant_elements(
        remove_double_brackets_in_list(
            [o["settlement_period"] for o in strategy_attributes]
        )
    )
