# # -*- coding: utf-8 -*-

import asyncio
import orjson
from loguru import logger as log

# user defined formula
from db_management.redis_client import saving_and_publishing_result
from messaging.telegram_bot import telegram_bot_sendtext
from utilities.string_modification import (
    remove_apostrophes_from_json,
    remove_list_elements,
    remove_redundant_elements,
)
from db_management.sqlite_management import executing_query_with_return
from utilities.system_tools import parse_error_message

"""

https://www.dataleadsfuture.com/exploring-numexpr-a-powerful-engine-behind-pandas/

    """


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

    candlestick_data[5] = (
        0 if body_size == 0 else (round(round(body_size / height, 5), 2) > 70 / 100) * 1
    )

    return candlestick_data


def my_generator_candle(
    np: object, 
    data: object,
    lookback: int,
    ) -> list:
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
    """ 
        https://www.tradingview.com/script/uuinZwsR-Big-Bar-Strategy/

    """

    dtype = [
        ("open", "f4"),
        ("high", "f4"),
        ("low", "f4"),
        ("close", "f4"),
    ]
    
    np_users_data = np.array(ohlc_without_ticks)

    np_data = np.array(
        [tuple(user.values()) for user in np_users_data],
        dtype=dtype,
    )

    candles_arrays = my_generator_candle(
        np,
        np_data[1:],
        dim_sequence,
    )

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


async def get_candles_data(
    currency: str,
    resolution: int,
    qty_candles: int,
):
    """ """

    table_ohlc = f"ohlc{resolution}_{currency.lower()}_perp_json"

    ohlc_query = (
        f"SELECT data FROM {table_ohlc} ORDER BY tick DESC LIMIT {qty_candles}"
    )

    result_from_sqlite = await executing_query_with_return(ohlc_query)

    ohlc_without_volume = remove_list_elements(
    remove_apostrophes_from_json(o["data"] for o in result_from_sqlite),
    "volume",
)

    ohlc_without_cost = remove_list_elements(
        ohlc_without_volume,
        "cost",
    )

    ohlc_without_ticks = remove_list_elements(
        ohlc_without_cost,
        "tick",
    )

    return ohlc_without_ticks

async def combining_candles_data(
    np: object,
    currencies: list,
    resolutions: int,
    qty_candles: int,
    dim_sequence: int = 3,
):
    """ """

    result = []
    for currency in currencies:

        instrument_name = f"{currency}-PERPETUAL"
        analysis_result = []
        for resolution in resolutions:
            
            if resolution !=1:

                candles_per_resolution = await get_candles_data(
                    currency,
                    resolution,
                    qty_candles,
                    )
                
                #log.info(f"candles_per_resolution {candles_per_resolution}")

                candles_analysis_result = candles_analysis(
                    np,
                    candles_per_resolution,
                    dim_sequence,
                )
                
                log.info(f"candles_analysis_result {candles_analysis_result}")

                #max_tick = max([o["tick"] for o in candles_per_resolution])

                analysis_result.append(
                    dict(
                        resolution=(resolution),
                        candles_analysis=(candles_analysis_result),
                    )
                )
        
        result.append(
            dict(
                instrument_name=instrument_name,
                result=(analysis_result),
            )
        )
    return result


def traslate_candles_data_to_market_condition(
    candles_data_instrument: list,
    np: object,
) -> dict:
    """ """
    try:

        #log.info (f"candles_data_instrument {candles_data_instrument}")
        candle_60 = [
            o["candles_analysis"]
            for o in candles_data_instrument
            if o["resolution"] == 60
        ]
        
        #log.info (f"candle_60 {candle_60}")

        candle_60_type = np.sum([o["candle_type"] for o in candle_60])

        candle_60_is_long = np.sum([o["is_long_body"] for o in candle_60])

        candle_5 = [
            o["candles_analysis"]
            for o in candles_data_instrument
            if o["resolution"] == 5
        ]

        candle_5_type = np.sum([o["candle_type"] for o in candle_5])

        candle_5_is_long = np.sum([o["is_long_body"] for o in candle_5])

        candle_15 = [
            o["candles_analysis"]
            for o in candles_data_instrument
            if o["resolution"] == 15
        ]

        candle_15_type = np.sum([o["candle_type"] for o in candle_15])

        candle_15_is_long = np.sum([o["is_long_body"] for o in candle_15])

        candle_60_long_body_more_than_2 = candle_60_is_long >= 2
        candle_5_long_body_any = candle_5_is_long > 0
        candle_15_long_body_any = candle_15_is_long > 0
        candle_60_long_body_any = candle_60_is_long > 0

        candle_60_no_long = candle_60_is_long == 0

        neutral = True
        weak_bullish, weak_bearish = False, False
        bullish, bearish = False, False
        strong_bullish, strong_bearish = False, False

        if candle_5_long_body_any:
            weak_bullish = True if candle_5_type > 0 else False
            weak_bearish = True if candle_5_type < 0 else False

        if candle_60_long_body_any and candle_15_long_body_any:
            bullish = True if weak_bullish and candle_15_type > 0 else False
            bearish = True if weak_bearish and candle_15_type < 0 else False

        if candle_60_long_body_more_than_2:
            strong_bullish = (
                True if bullish and candle_60_long_body_more_than_2 else False
            )

            strong_bearish = (
                True if bearish and candle_60_long_body_more_than_2 else False
            )

        neutral = True if not weak_bearish and not weak_bullish else False

        return dict(
            strong_bullish=strong_bullish,
            bullish=bullish,
            weak_bullish=weak_bullish,
            neutral=neutral,
            weak_bearish=weak_bearish,
            bearish=bearish,
            strong_bearish=strong_bearish,
        )

    except Exception as error:

        parse_error_message(error)

        asyncio.run(
            telegram_bot_sendtext(f"get_market_condition - {error}", "general_error")
        )


async def get_market_condition(
    client_redis: object,
    config_app: list,
    currencies,
    redis_channels,
    resolutions,
    np: object,
) -> dict:
    """ """
    try:

        # connecting to redis pubsub
        pubsub: object = client_redis.pubsub()

        market_analytics_channel: str = redis_channels["market_analytics_update"]
        chart_low_high_tick_channel: str = redis_channels["chart_low_high_tick"]

        redis_keys: dict = config_app["redis_keys"][0]
        market_condition_keys: str = redis_keys["market_condition"]

        # prepare channels placeholders
        channels = [
            market_analytics_channel,
            chart_low_high_tick_channel,
        ]

        # subscribe to channels
        [await pubsub.subscribe(o) for o in channels]

        qty_candles = 5
        dim_sequence = 3

        candles_data = await combining_candles_data(
            np,
            currencies,
            resolutions,
            dim_sequence,
            qty_candles,
        )
        
        log.debug(f"candles_data {candles_data}")

        cached_candles_data_is_updated = True

        market_analytics_data = []

        while cached_candles_data_is_updated:

            try:
                
                log.debug (f"market_analytics_data {market_analytics_data}")

                if market_analytics_data != []:

                    message_byte = await pubsub.get_message()

                    if message_byte and message_byte["type"] == "message":

                        message_byte_data = orjson.loads(message_byte["data"])

                        message_channel = message_byte["channel"]

                        if chart_low_high_tick_channel in message_channel:

                            instrument_name = message_byte_data["instrument_name"]
                            
    #                        log.warning(f"message_byte_data {message_byte_data}")
                            
                            log.debug (f"market_analytics_data {market_analytics_data}")

                            if instrument_name in message_byte_data["instrument_name"]:

                                candles_data_instrument = [
                                    o["result"]
                                    for o in candles_data
                                    if instrument_name in o["instrument_name"]
                                ][0]
                            
                            log.warning(f"candles_data_instrument {candles_data_instrument}")

                            pub_message = traslate_candles_data_to_market_condition(
                                candles_data_instrument,
                                np,
                            )

                            pub_message.update({"instrument_name": instrument_name})

                            market_analytics_data.append(pub_message)
                            
                            log.warning (f"result {pub_message}")


                else:
            
                    candles_instrument_name = remove_redundant_elements(
                        [o["instrument_name"] for o in candles_data]
                    )
                    
                    for instrument_name in candles_instrument_name:

                        candles_data_instrument = [
                            o["result"]
                            for o in candles_data
                            if instrument_name in o["instrument_name"]
                        ][0]
                        
                        pub_message = traslate_candles_data_to_market_condition(
                            candles_data_instrument,
                            np,
                        )

                        pub_message.update({"instrument_name": instrument_name})

                        market_analytics_data.append(pub_message)

                log.critical(f"result {pub_message}")
                await saving_and_publishing_result(
                    client_redis,
                    market_analytics_channel,
                    market_condition_keys,
                    market_analytics_data,
                    market_analytics_data,
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

