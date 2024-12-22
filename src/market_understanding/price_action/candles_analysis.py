from db_management import sql_executing_queries
import asyncio
from loguru import logger as log

def analysis_based_on_length(
    np: object,
    candles_data_instrument: list,
    resolutions: list):
    """_summary_
    https://www.tradingview.com/script/uuinZwsR-Big-Bar-Strategy/
        Args:
            tables (str, optional): _description_. Defaults to 'ohlc60_eth_perp_json'.

        Returns:
            _type_: _description_
    """
    
    
    for resolution in resolutions:
        
        data_per_resolution = [o for o in candles_data_instrument\
            if resolution == o["resolution"]]

        candles_arrays = [o["candles_summary"] for o in data_per_resolution][0]

        candle_type = candles_arrays [-1, :, 0] #(last_column_third_row)
        wicks_up = candles_arrays [-1, :, 1] #(last_column_third_row)
        wicks_down = candles_arrays [-1, :, 2] #(last_column_third_row)
        body_size = candles_arrays [-1, :, 3] #(last_column_third_row)
        body_length = candles_arrays [-1, :, 4] #(last_column_third_row)
        mean_body_length = np.average(body_length)
        
        print(candles_arrays)
        log.warning (body_length)
        log.warning (mean_body_length)
    
    return 
