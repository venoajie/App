from db_management import sql_executing_queries
import asyncio
from loguru import logger as log

def analysis_based_on_length(
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
        candles_summary = [o["candles_summary"] for o in data_per_resolution][0]
        print(candles_summary)
        log.error (f"candles_summary.dtype {candles_summary.dtype} candles_summary.shape {candles_summary.shape} candles_summary.ndim {candles_summary.ndim} candles_summary.size {candles_summary.size}")
        
        body_length =candles_summary[-1, :]
        
        log.warning (body_length)
    
    return 
