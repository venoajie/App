# # -*- coding: utf-8 -*-

import asyncio
import orjson
from loguru import logger as log

# user defined formula
from db_management.redis_client import saving_and_publishing_result, publishing_result
from messaging.telegram_bot import telegram_bot_sendtext
from transaction_management.deribit.api_requests import get_ohlc_data
from utilities.string_modification import (
    remove_list_elements,
    remove_redundant_elements,
)
from utilities.system_tools import parse_error_message

"""

https://www.dataleadsfuture.com/exploring-numexpr-a-powerful-engine-behind-pandas/

    """


def analysis_based_on_length(
    np: object,
    data_per_resolution: int,
):
    """_summary_
    https://www.tradingview.com/script/uuinZwsR-Big-Bar-Strategy/
        Args:
            tables (str, optional): _description_. Defaults to 'ohlc60_eth_perp_json'.

        Returns:
            _type_: _description_
    """

    candles_arrays = data_per_resolution  # [0]

    candle_type = candles_arrays[-1, :, 0]  # (last_column_third_row)
    wicks_up = candles_arrays[-1, :, 1]  # (last_column_third_row)
    wicks_down = candles_arrays[-1, :, 2]  # (last_column_third_row)
    body_size = candles_arrays[-1, :, 3]  # (last_column_third_row)
    body_length = candles_arrays[-1, :, 4]  # (last_column_third_row)
    is_long_body = candles_arrays[-1, :, 5]  # (last_column_third_row)
    avg_body_length = np.average(body_length)
    body_length_exceed_average = body_length > avg_body_length
    # print(candles_arrays)
    # log.warning (f"candle_type {candle_type}")
    # log.warning (f"wicks_up {wicks_up}")
    # log.warning (f"wicks_down {wicks_down}")
    # log.warning (f"body_size {body_size}")
    # log.warning (f"body_length {body_length}")
    # log.warning (f"is_long_body {is_long_body}")
    # log.warning (f" avg_body_length {avg_body_length}")
    # log.warning (f" body_length_exceed_average {body_length_exceed_average}")

    return dict(
        candle_type=candle_type,
        body_length_exceed_average=body_length_exceed_average,
        is_long_body=(is_long_body),
    )


def ohlc_to_candlestick(conversion_array):

    candlestick_data = [0, 0, 0, 0, 0, 0]

    open = conversion_array[0]
    high = conversion_array[1]
    low = conversion_array[2]
    close = conversion_array[3]

    body_size = abs(close - open)
    height = abs(high - low)

    if close > open:
        candle_type = 1
        wicks_up = abs(high - close)
        wicks_down = abs(low - open)

    else:
        candle_type = -1
        wicks_up = abs(high - open)
        wicks_down = abs(low - close)

    candlestick_data[0] = candle_type

    candlestick_data[1] = round(round(wicks_up, 5), 2)

    candlestick_data[2] = round(round(wicks_down, 5), 2)

    candlestick_data[3] = round(round(body_size, 5), 2)

    candlestick_data[4] = round(round(height, 5), 2)

    candlestick_data[5] = (round(round(body_size / height, 5), 2) > 70 / 100) * 1

    return candlestick_data


def my_generator_candle(np: object, data: object, lookback: int) -> list:
    """_summary_
        https://github.com/MikePapinski/DeepLearning/blob/master/PredictCandlestick/CandleSTick%20patterns%20prediction/JupyterResearch_0.1.ipynb
        https://mikepapinski.github.io/deep%20learning/machine%20learning/python/forex/2018/12/15/Predict-Candlestick-patterns-with-Keras-and-Forex-data.md.html

    Args:
        data (_type_): _description_
        lookback (_type_): _description_

    Returns:
        _type_: _description_
    """
    first_row = 0

    parameters = len(
        [
            "candle_type",
            "wicks_up",
            "wicks_down",
            "body_size",
            "length",
            "is_long_body",
        ]
    )

    arr = np.empty((1, lookback, parameters), int)

    for a in range(len(data) - lookback):

        #        log.debug (f"data my_generator_candle {data} lookback {lookback}")

        temp_list = []
        for candle in data[first_row : first_row + lookback]:

            converted_data = ohlc_to_candlestick(candle)
            # log.info (f"converted_data  {converted_data} candle {candle}")
            temp_list.append(converted_data)

        temp_list2 = np.asarray(temp_list)
        templist3 = [temp_list2]
        templist4 = np.asarray(templist3, dtype="f4")
        #        log.info (f"templist4  {templist4}")
        #        log.warning (f"arr  1 {arr}")
        arr = np.append(arr, templist4, axis=0)
        #        log.warning (f"arr  2 {arr}")
        first_row = first_row + 1

    return arr


def candles_analysis(
    np: object,
    ohlc_without_ticks: list,
    dim_sequence: int = 3,
):
    """ """

    dtype = [
        ("open", "f4"),
        ("high", "f4"),
        ("low", "f4"),
        ("close", "f4"),
    ]

    np_users_data = np.array(ohlc_without_ticks)

    np_data = np.array([tuple(user.values()) for user in np_users_data], dtype=dtype)

    three_dim_sequence = my_generator_candle(np, np_data[1:], dim_sequence)

    candles_analysis_result = analysis_based_on_length(np, three_dim_sequence)

    return candles_analysis_result

        
async def get_candles_data(
    currencies: list,
    qty_candles: int,
    resolutions: int,
):
    """ """
    result = []
    for currency in currencies:

        instrument_name = f"{currency}-PERPETUAL"

        for resolution in resolutions:

            ohlc = await get_ohlc_data(
                instrument_name,
                qty_candles,
                resolution,
            )
            
            result.append(
                dict(
                    instrument_name=instrument_name,
                    resolution=resolution,
                    ohlc=ohlc,
                )
            )

    return result


def combining_candles_data(
    np: object,
    currencies: list,
    candles_data: int,
    resolutions: int,
    dim_sequence: int = 3,
):
    """ """

    result = []
    for currency in currencies:

        instrument_name = f"{currency}-PERPETUAL"

        candles_per_instrument_name = [
            o for o in candles_data if o["instrument_name"] == instrument_name
        ]

        for resolution in resolutions:

            candles_per_resolution = [
                o["ohlc"]
                for o in candles_per_instrument_name
                if o["resolution"] == resolution
            ][0]

            ohlc_without_ticks = remove_list_elements(
                candles_per_resolution,
                "tick",
            )

            candles_analysis_result = candles_analysis(
                np,
                ohlc_without_ticks,
                dim_sequence,
            )

            max_tick = max([o["tick"] for o in candles_per_resolution])

            result.append(
                dict(
                    instrument_name=instrument_name,
                    resolution=(resolution),
                    max_tick=(max_tick),
                    # ohlc = (ohlc),
                    # candles_summary = (three_dim_sequence),
                    candles_analysis=(candles_analysis_result),
                )
            )

    return result


async def get_market_condition(
    client_redis: object,
    config_app: list,
    np: object,
) -> dict:
    """ """
    try:

        # connecting to redis pubsub
        pubsub: object = client_redis.pubsub()

        # get tradable strategies
        tradable_config_app = config_app["tradable"]

        # get TRADABLE currencies
        currencies = [o["spot"] for o in tradable_config_app][0]
        # get redis channels
        redis_channels: dict = config_app["redis_channels"][0]
        chart_update_channel: str = redis_channels["chart_update"]
        market_analytics_channel: str = redis_channels["market_analytics_update"]

        redis_keys: dict = config_app["redis_keys"][0]
        market_condition_keys: str = redis_keys["market_condition"]

        # prepare channels placeholders
        channels = [
            chart_update_channel,
        ]

        # subscribe to channels
        [await pubsub.subscribe(o) for o in channels]

        resolutions = [60, 15, 5]
        qty_candles = 5
        dim_sequence = 3

        cached_candles_data = await get_candles_data(
            currencies,
            qty_candles,
            resolutions,
        )

        cached_candles_data_is_updated = False

        while True:

            try:

                message_byte = await pubsub.get_message()

                if message_byte and message_byte["type"] == "message":

                    message_byte_data = orjson.loads(message_byte["data"])

                    message_channel = message_byte["channel"]

                    if chart_update_channel in message_channel:

                        instrument_name = message_byte_data["instrument_name"]
                        ohlc_from_exchange = message_byte_data["data"]
                        tick_from_exchange = ohlc_from_exchange["tick"]
                        high_from_exchange = ohlc_from_exchange["high"]
                        low_from_exchange = ohlc_from_exchange["low"]

                        candles_data_instrument = [
                            o
                            for o in cached_candles_data
                            if instrument_name in o["instrument_name"]
                        ]

                        log.error(f" {instrument_name}")
                        log.debug(f" ohlc_from_exchange {ohlc_from_exchange}")

                        for resolution in resolutions:

                            candles_data_resolution = [
                                o
                                for o in candles_data_instrument
                                if resolution == o["resolution"]
                            ]

                            # log.warning(
                            #    f" candles_data_resolution {candles_data_resolution}"
                            # )
                            ohlc_resolution = [
                                o for o in candles_data_resolution[0]["ohlc"]
                            ]
                            # log.warning(f" ohlc_resolution {ohlc_resolution}")

                            if ohlc_resolution:
                                
                                ohlc_tick_max = max(
                                    [o["tick"] for o in ohlc_resolution]
                                )
                                
                                tick_delta = (tick_from_exchange - ohlc_tick_max)/60000
                                log.critical(
                                        f" resolution {resolution} {tick_delta}")

                                log.info(f" ohlc_tick_max {ohlc_tick_max}")
                                log.info(f" test {[
                                o for o in [
                                o
                                for o in [
                            o
                            for o in cached_candles_data
                            if instrument_name in o["instrument_name"]
                        ]
                                if resolution == o["resolution"]
                            ][0]["ohlc"]
                            ]}")
                                
                                log.warning(f" ohlc_resolution before {ohlc_resolution}")
                                        
                                # update all under resolution
                                if tick_delta > resolution:

                                    log.critical(
                                        f" tick_delta > resolution {tick_delta > resolution}"
                                    )
                                    
                                    cached_candles_data_is_updated = True
                                    
                                    updated_data = await get_ohlc_data(
                                        instrument_name,
                                        qty_candles,
                                        resolution,
                                        )
                                    
                                    
                                    [
                                o for o in [i for i in [
                                y
                                for y in candles_data_instrument
                                if resolution == y["resolution"]
                            ] if instrument_name in i["instrument_name"]
                        ][0]["ohlc"]
                            ] = updated_data
                                    
                                    log.warning(f" ohlc_resolution after {ohlc_resolution}")
                                        
                                
                                # partial update
                                else:
                                    
                                    ohlc_tick_max_elements = [
                                        o
                                        for o in ohlc_resolution
                                        if o["tick"] == ohlc_tick_max
                                    ][0]
                                    
                                    ohlc_high = ohlc_tick_max_elements["high"]
                                    ohlc_low = ohlc_tick_max_elements["low"]

                                    log.info(
                                        f" resolution {resolution} ohlc_high {ohlc_high} high_from_exchange {high_from_exchange} ohlc_low {ohlc_low} low_from_exchange {low_from_exchange}"
                                    )

                                    if high_from_exchange > ohlc_high:
                                        
                                        cached_candles_data_is_updated = True

                                        log.critical(
                                            f" high_from_exchange > ohlc_high {high_from_exchange > ohlc_high}"
                                        )
                                        
                                        updating_cached_values(
                                            cached_candles_data,
                                            instrument_name,
                                            resolution,
                                            ohlc_tick_max,
                                            "high",
                                            high_from_exchange,
                                            )
                                        
                                        log.warning(f" ohlc_resolution after {ohlc_resolution}")
                                        
                                    if low_from_exchange < ohlc_low:
                                        
                                        cached_candles_data_is_updated = True
                                        
                                        log.critical(
                                            f" low_from_exchange < ohlc_low {low_from_exchange < ohlc_low}"
                                        )
                                        
                                        updating_cached_values(
                                            cached_candles_data,
                                            instrument_name,
                                            resolution,
                                            ohlc_tick_max,
                                            "low",
                                            low_from_exchange,
                                            )

                                        log.warning(f" ohlc_resolution after {ohlc_resolution}")
                                        
                        result = []

                        if cached_candles_data_is_updated:

                            # log.error(f" candles_data_instrument {candles_data_instrument}")

                            candles_data = combining_candles_data(
                                np,
                                currencies,
                                cached_candles_data,
                                resolutions,
                                dim_sequence,
                            )

                            candles_instrument_name = remove_redundant_elements(
                                [o["instrument_name"] for o in candles_data]
                            )

                            for instrument_name in candles_instrument_name:

                                if (
                                    instrument_name
                                    in message_byte_data["instrument_name"]
                                ):

                                    candles_data_instrument = [
                                        o
                                        for o in candles_data
                                        if instrument_name in o["instrument_name"]
                                    ]

                                    candle_60 = [
                                        o["candles_analysis"]
                                        for o in candles_data_instrument
                                        if o["resolution"] == 60
                                    ]

                                    candle_60_type = np.sum(
                                        [o["candle_type"] for o in candle_60]
                                    )

                                    candle_60_is_long = np.sum(
                                        [o["is_long_body"] for o in candle_60]
                                    )

                                    candle_5 = [
                                        o["candles_analysis"]
                                        for o in candles_data_instrument
                                        if o["resolution"] == 5
                                    ]

                                    candle_5_type = np.sum(
                                        [o["candle_type"] for o in candle_5]
                                    )

                                    candle_5_is_long = np.sum(
                                        [o["is_long_body"] for o in candle_5]
                                    )

                                    candle_15 = [
                                        o["candles_analysis"]
                                        for o in candles_data_instrument
                                        if o["resolution"] == 15
                                    ]

                                    candle_15_type = np.sum(
                                        [o["candle_type"] for o in candle_15]
                                    )

                                    candle_15_is_long = np.sum(
                                        [o["is_long_body"] for o in candle_15]
                                    )

                                    candle_60_long_body_more_than_2 = (
                                        candle_60_is_long >= 2
                                    )
                                    candle_5_long_body_any = candle_5_is_long > 0
                                    candle_15_long_body_any = candle_15_is_long > 0
                                    candle_60_long_body_any = candle_60_is_long > 0

                                    candle_60_no_long = candle_60_is_long == 0

                                    neutral = True
                                    weak_bullish, weak_bearish = False, False
                                    bullish, bearish = False, False
                                    strong_bullish, strong_bearish = False, False

                                    if candle_5_long_body_any:
                                        weak_bullish = (
                                            True if candle_5_type > 0 else False
                                        )
                                        weak_bearish = (
                                            True if candle_5_type < 0 else False
                                        )

                                    if (
                                        candle_60_long_body_any
                                        and candle_15_long_body_any
                                    ):
                                        bullish = (
                                            True
                                            if weak_bullish and candle_15_type > 0
                                            else False
                                        )
                                        bearish = (
                                            True
                                            if weak_bearish and candle_15_type < 0
                                            else False
                                        )

                                    if candle_60_long_body_more_than_2:
                                        strong_bullish = (
                                            True
                                            if bullish
                                            and candle_60_long_body_more_than_2
                                            else False
                                        )

                                        strong_bearish = (
                                            True
                                            if bearish
                                            and candle_60_long_body_more_than_2
                                            else False
                                        )

                                    neutral = (
                                        True
                                        if not weak_bearish and not weak_bullish
                                        else False
                                    )

                                    pub_message = dict(
                                        instrument_name=instrument_name,
                                        strong_bullish=strong_bullish,
                                        bullish=bullish,
                                        weak_bullish=weak_bullish,
                                        neutral=neutral,
                                        weak_bearish=weak_bearish,
                                        bearish=bearish,
                                        strong_bearish=strong_bearish,
                                    )

                                    result.append(pub_message)

                                #log.critical(f"result {result}")
                                await saving_and_publishing_result(
                                    client_redis,
                                    market_analytics_channel,
                                    market_condition_keys,
                                    result,
                                    result,
                                )

            except Exception as error:

                parse_error_message(error)

                await telegram_bot_sendtext(
                    f"get_market_condition - {error}",
                    "general_error",
                )

                continue

            finally:
                await asyncio.sleep(0.1)

    except Exception as error:

        parse_error_message(error)

        asyncio.run(
            telegram_bot_sendtext(f"get_market_condition - {error}", "general_error")
        )


def updating_cached_values(
    cached_candles_data: list,
    instrument_name: str,
    resolution: int,
    ohlc_tick_max: int,
    key_to_update: str,
    value_to_update: float,
):
    """ """
    
    log.debug(f" cached update {[
        y
        for y in [
            x
            for x in [
                o
                for o in [
                    i
                    for i in cached_candles_data
                    if instrument_name in i["instrument_name"]
                ]
                if resolution == o["resolution"]
            ][0]["ohlc"]
        ]
        if y["tick"] == ohlc_tick_max
    ][0][(f"{key_to_update}")]} ")

    [
        y
        for y in [
            x
            for x in [
                o
                for o in [
                    i
                    for i in cached_candles_data
                    if instrument_name in i["instrument_name"]
                ]
                if resolution == o["resolution"]
            ][0]["ohlc"]
        ]
        if y["tick"] == ohlc_tick_max
    ][0][(f"{key_to_update}")] =  value_to_update
