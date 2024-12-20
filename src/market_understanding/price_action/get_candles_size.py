from db_management import sql_executing_queries
import asyncio
from loguru import logger as log

async def get_dataframe_from_ohlc_tables(tables: str = "ohlc60_eth_perp_json"):
    """_summary_
    https://www.tradingview.com/script/uuinZwsR-Big-Bar-Strategy/
        Args:
            tables (str, optional): _description_. Defaults to 'ohlc60_eth_perp_json'.

        Returns:
            _type_: _description_
    """
    import pandas as pd

    barsizeThreshold = 0.5
    period = 10
    mult = 2
    pd.set_option("display.max_rows", None)
    res = await sql_executing_queries.querying_tables_item_data(tables)
    df = pd.DataFrame(res)
    df["candle_size"] = df["high"] - df["low"]
    df["body_size"] = abs(df["open"] - df["close"])
    df["candle_size_avg"] = df["candle_size"].rolling(period).mean()
    df["bigbar"] = (df["candle_size"] >= df["candle_size_avg"] * mult) & (
        df["body_size"] > df["candle_size"] * barsizeThreshold
    )
    print(df)

    return df



def my_generator(
    np,
    data,
    lookback
    ):
    
    final_output = []
    counter = 0
    first_row = 0
    arr = np.empty((1,lookback,4), int)
    
    for a in range(len(data)-lookback):
        temp_list = []
        for candle in data[first_row:first_row+lookback]:
            temp_list.append(candle)
        temp_list2 = np.asarray(temp_list)
        templist3 = [temp_list2]
        templist4 = np.asarray(templist3)
        arr = np.append(arr, templist4, axis=0)
        first_row=first_row+1
    return arr

def ohlc_to_candlestick(conversion_array):
    
    candlestick_data = [0,0,0,0]
    
    log.warning (f"conversion_array {conversion_array}")

    if conversion_array[3]>conversion_array[0]:
        candle_type=1
        wicks_up=conversion_array[1]-conversion_array[3]
        wicks_down=conversion_array[2]-conversion_array[0]
        body_size=conversion_array[3]-conversion_array[0]

    else:
        candle_type=0
        wicks_up=conversion_array[1]-conversion_array[0]
        wicks_down=conversion_array[2]-conversion_array[3]
        body_size=conversion_array[1]-conversion_array[3]


    if wicks_up < 0: wicks_up=wicks_up*(-1)
    if wicks_down < 0: wicks_down=wicks_down*(-1)
    if body_size < 0: body_size=body_size*(-1)
    
    candlestick_data[0]=candle_type
    
    candlestick_data[1]=round(round(wicks_up,5),2)
    
    candlestick_data[2]=round(round(wicks_down,5),2)
    
    candlestick_data[3]=round(round(body_size,5),2)

    return candlestick_data

def my_generator_candle(
    np: object,
    data: object,
    lookback: int
    )->list:
    
    """_summary_
        https://mikepapinski.github.io/deep%20learning/machine%20learning/python/forex/2018/12/15/Predict-Candlestick-patterns-with-Keras-and-Forex-data.md.html
    
    Args:
        data (_type_): _description_
        lookback (_type_): _description_

    Returns:
        _type_: _description_
    """
    first_row = 0
    arr = np.empty(
        (1,
         lookback,
         4
         ), int
        )
    
    for a in range(len(data)-lookback):
        
        log.debug (f"data my_generator_candle {data} lookback {lookback}")
        
        temp_list = []
        for candle in data[first_row:first_row+lookback]:

            converted_data = ohlc_to_candlestick(candle)
            temp_list.append(converted_data)
        
        temp_list2 = np.asarray(temp_list)
        templist3 = [temp_list2]
        templist4 = np.asarray(templist3)
        arr = np.append(arr, templist4, axis=0)
        first_row=first_row+1
    
    return arr


if __name__ == "__main__":

    try:
        asyncio.get_event_loop().run_until_complete(
            get_dataframe_from_ohlc_tables("ohlc60_eth_perp_json")
        )

    except Exception as error:
        print(error)
