
from loguru import logger as log
from transaction_management.deribit.api_requests import (
    get_ohlc_data)
from utilities.string_modification import (
    remove_list_elements)

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
    #log.warning (f"candle_type {candle_type}")
    #log.warning (f"wicks_up {wicks_up}")
    #log.warning (f"wicks_down {wicks_down}")
    #log.warning (f"body_size {body_size}")
    #log.warning (f"body_length {body_length}")
    #log.warning (f"is_long_body {is_long_body}")
    #log.warning (f" avg_body_length {avg_body_length}")
    #log.warning (f" body_length_exceed_average {body_length_exceed_average}")
    
    return  (dict(
            candle_type = candle_type,
            body_length_exceed_average = body_length_exceed_average,
            is_long_body = (is_long_body),
            )
)            


def ohlc_to_candlestick(conversion_array):
    
    candlestick_data = [0,0,0,0,0,0]
    
    open = conversion_array[0]
    high = conversion_array[1]
    low = conversion_array[2]
    close = conversion_array[3]
    
    body_size=abs(close-open)
    height=abs(high-low)

    if close>open:
        candle_type=1
        wicks_up=abs(high-close)
        wicks_down=abs(low-open)

    else:
        candle_type=-1
        wicks_up=abs(high-open)
        wicks_down=abs(low-close)

    candlestick_data[0]=candle_type
    
    candlestick_data[1]=round(round(wicks_up,5),2)
    
    candlestick_data[2]=round(round(wicks_down,5),2)
    
    candlestick_data[3]=round(round(body_size,5),2)

    candlestick_data[4]=round(round(height,5),2)

    candlestick_data[5]= (round(round(body_size/height,5),2)>70/100) * 1
    
    return candlestick_data

def my_generator_candle(
    np: object,
    data: object,
    lookback: int
    )->list:
    
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
    
    parameters = len(["candle_type", "wicks_up", "wicks_down", "body_size", "length", "is_long_body"])

    arr = np.empty((1,lookback,parameters),
                   int)
    
    for a in range(len(data)-lookback):
        
#        log.debug (f"data my_generator_candle {data} lookback {lookback}")
        
        temp_list = []
        for candle in data[first_row:first_row+lookback]:

            converted_data = ohlc_to_candlestick(candle)
            #log.info (f"converted_data  {converted_data} candle {candle}")
            temp_list.append(converted_data)
        
        temp_list2 = np.asarray(temp_list)
        templist3 = [temp_list2]
        templist4 = np.asarray(
            templist3,
            dtype = "f4"
            )
#        log.info (f"templist4  {templist4}")
#        log.warning (f"arr  1 {arr}")
        arr = np.append(
            arr, 
            templist4, 
            axis=0
            )
#        log.warning (f"arr  2 {arr}")
        first_row=first_row+1
    
    return arr


def combining_candles_data(
    np: object,
    currencies: list,
    qty_candles: int,
    resolutions: int,
    dim_sequence: int = 3):
    """
    """
    
    dtype = [
        ("open", "f4"),
        ("high", "f4"),
        ("low", "f4"),
        ("close", "f4"),]
            
    result =[]
    for currency in currencies:
        instrument_name = f"{currency}-PERPETUAL"
        
        for resolution in resolutions:

            ohlc = get_ohlc_data (
                instrument_name, 
                qty_candles, 
                resolution
                )

            ohlc_without_ticks = remove_list_elements(ohlc, "tick")
            
            np_users_data = np.array(ohlc_without_ticks)

            np_data = np.array([tuple(user.values()) for user in np_users_data], dtype=dtype)

            three_dim_sequence = (my_generator_candle(
                np,
                np_data[1:],
                dim_sequence))
            
            candles_analysis_result = analysis_based_on_length(
                                            np,
                                            three_dim_sequence)

            max_tick = max([o['tick']  for o in ohlc ])
            
            result.append (dict(
                instrument_name = instrument_name,
                resolution = (resolution),
                max_tick = (max_tick),
                #ohlc = (ohlc),
                #candles_summary = (three_dim_sequence),
                candles_analysis = (candles_analysis_result),
                )
                           )
            
    return result



def get_market_condition(
    np: object,
    candles_data: list,
    currency_upper: str
    ):
    """
    """
    candles_data_instrument = [o for o in candles_data if currency_upper in o["instrument_name"]]
    #log.warning (candles_data_instrument)
    
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
    candle_5_long_body_any = candle_5_is_long >0
    candle_15_long_body_any = candle_15_is_long >0
    candle_60_long_body_any = candle_60_is_long >0
    
    candle_60_no_long = candle_60_is_long == 0
        
    neutral = True
    weak_bullish,  weak_bearish = False, False
    bullish,  bearish = False, False
    strong_bullish,  strong_bearish = False, False
    
    if candle_5_long_body_any:
        weak_bullish = candle_5_type > 0
        weak_bearish = candle_5_type < 0
    
    if candle_60_long_body_any and candle_15_long_body_any:
        bullish = weak_bullish and candle_15_type > 0 
        bearish = weak_bearish and candle_15_type < 0 
    
    if candle_60_long_body_more_than_2:
        strong_bullish = bullish and candle_60_long_body_more_than_2
        strong_bearish = bearish and candle_60_long_body_more_than_2
        
    neutral = not weak_bearish and not weak_bullish
    
    return dict(
                strong_bullish = strong_bullish,
                bullish = bullish,
                weak_bullish = weak_bullish,
                neutral = neutral,
                weak_bearish = weak_bearish,
                bearish = bearish,
                strong_bearish = strong_bearish,
                )


