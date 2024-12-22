from db_management import sql_executing_queries
import asyncio
from loguru import logger as log

def analysis_based_on_length(
    np: object,
    data_per_resolution: int):
    """_summary_
    https://www.tradingview.com/script/uuinZwsR-Big-Bar-Strategy/
        Args:
            tables (str, optional): _description_. Defaults to 'ohlc60_eth_perp_json'.

        Returns:
            _type_: _description_
    """

    candles_arrays = data_per_resolution#[0]

    candle_type = candles_arrays [-1, :, 0] #(last_column_third_row)
    wicks_up = candles_arrays [-1, :, 1] #(last_column_third_row)
    wicks_down = candles_arrays [-1, :, 2] #(last_column_third_row)
    body_size = candles_arrays [-1, :, 3] #(last_column_third_row)
    body_length = candles_arrays [-1, :, 4] #(last_column_third_row)
    is_long_body = candles_arrays [-1, :, 5] #(last_column_third_row)
    avg_body_length = np.average(body_length)
    body_length_exceed_average = body_length > avg_body_length
    #print(candles_arrays)
    log.warning (f"candle_type {candle_type}")
    log.warning (f"wicks_up {wicks_up}")
    log.warning (f"wicks_down {wicks_down}")
    log.warning (f"body_size {body_size}")
    log.warning (f"body_length {body_length}")
    log.warning (f"is_long_body {is_long_body}")
    log.warning (f" avg_body_length {avg_body_length}")
    log.warning (f" body_length_exceed_average {body_length_exceed_average}")
    
    return  (dict(
            candle_type = candle_type,
            body_length_exceed_average = body_length_exceed_average,
            is_long_body = (is_long_body),
            )
)            


def get_market_condition(
    candles_data: object,
    currency_upper: str
    ):
    """
    """
    candles_data_instrument = [o for o in candles_data \
                                                if currency_upper in o["instrument_name"]]
    
    candle_60 = [o["candles_analysis"] for o in candles_data_instrument if o["resolution"] == 60]
    candle_60_type = np.sum([o["candle_type"] for o in candle_60])
    candle_60_is_long = np.sum([o["is_long_body"] for o in candle_60])
    
    candle_5 = [o["candles_analysis"] for o in candles_data_instrument if o["resolution"] == 5]
    candle_5_type = np.sum([o["candle_type"] for o in candle_5])
    candle_5_is_long = np.sum([o["is_long_body"] for o in candle_5])
    
    candle_15 = [o["candles_analysis"] for o in candles_data_instrument if o["resolution"] == 15]
    candle_15_type = np.sum([o["candle_type"] for o in candle_15])
    candle_15_is_long = np.sum([o["is_long_body"] for o in candle_15])
    
    log.warning (candle_60)
    log.debug (candle_5)
    log.debug (candle_60_type)
    log.warning (candle_15)
    
    candle_60_long_body_more_than_2 = candle_60_is_long >= 2
    
    candle_60_no_long = candle_60_is_long == 0
    
    strong_bullish = candle_60_type >= 2 and candle_60_long_body_more_than_2
    
    bullish = candle_15_type > 2 and candle_60_is_long >= 2
    
    weak_bullish = candle_5_type > 2
    
    neutral = candle_60_no_long and candle_15_is_long == 0 and candle_5_is_long == 0
    
    weak_bearish = True
    
    bearish = candle_15_type <= -2 and candle_60_is_long >= 2
    
    strong_bearish = candle_60_type <= -2 and candle_60_long_body_more_than_2
    
    return dict(
                strong_bullish = strong_bullish,
                bullish = bullish,
                weak_bullish = weak_bullish,
                neutral = neutral,
                weak_bearish = weak_bearish,
                bearish = bearish,
                strong_bearish = strong_bearish,
                )

