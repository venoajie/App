#!/usr/bin/python3
# -*- coding: utf-8 -*-

# installed
import asyncio
import json

# built ins
from datetime import datetime, timedelta

from loguru import logger as log

# user defined formula
from db_management import sqlite_management
from strategies.config_strategies import preferred_spot_currencies
from utilities import system_tools
from utilities.string_modification import transform_nested_dict_to_list
from utilities.time_modification import convert_time_to_unix


async def telegram_bot_sendtext(bot_message, purpose: str = "general_error") -> None:
    import deribit_get

    return await deribit_get.telegram_bot_sendtext(bot_message, purpose)


async def insert_ohlc(
    currency,
    instrument_name: str,
    resolution: int = 1,
    qty_candles: int = 6000,
) -> None:

    import requests

    now_utc = datetime.now()
    now_unix = convert_time_to_unix(now_utc)

    if resolution == "1D":
        resolution2 = 60 * 24
        start_timestamp = now_unix - (60000 * resolution2) * qty_candles
        ohlc_endPoint = f" https://deribit.com/api/v2/public/get_tradingview_chart_data?end_timestamp={now_unix}&instrument_name={instrument_name}&resolution={resolution}&start_timestamp={start_timestamp}"

    else:
        start_timestamp = now_unix - (60000 * resolution) * qty_candles
        ohlc_endPoint = f" https://deribit.com/api/v2/public/get_tradingview_chart_data?end_timestamp={now_unix}&instrument_name={instrument_name}&resolution={resolution}&start_timestamp={start_timestamp}"

    try:

        log.warning(ohlc_endPoint)
        # log.warning (requests.get(ohlc_endPoint).json())
        ohlc_request = requests.get(ohlc_endPoint).json()["result"]

        result = transform_nested_dict_to_list(ohlc_request)

        table = f"ohlc{resolution}_{currency.lower()}_perp_json"

        for data in result:
            log.debug(f"insert tables {table}")
            await sqlite_management.insert_tables(table, data)

    except Exception as error:
        system_tools.catch_error_message(
            error,
            10,
            "WebSocket connection - failed to get ohlc",
        )


async def main():

    try:

        currencies = preferred_spot_currencies()

        for currency in currencies:

            instrument_name = f"{currency.upper()}-PERPETUAL"

            log.critical(f"instrument_name {instrument_name} currency {currency}")

            resolutions = ["4H", 240]  # [60, 30,5,15,1,"1D",3]

            qty_candles = 6000

            for res in resolutions:

                await insert_ohlc(currency, instrument_name, res, qty_candles)

    except Exception as error:
        system_tools.catch_error_message(
            error, 10, "fetch and save MARKET data from deribit"
        )


if __name__ == "__main__":
    try:
        asyncio.get_event_loop().run_until_complete(main())

    except (KeyboardInterrupt, SystemExit):
        asyncio.get_event_loop().run_until_complete(main().stop_ws())

    except Exception as error:
        system_tools.catch_error_message(
            error, 10, "fetch and save MARKET data from deribit"
        )
