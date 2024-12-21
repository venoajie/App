from db_management import sql_executing_queries
import asyncio
from loguru import logger as log

async def analysis_based_on_length(
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
        log.error (data_per_resolution)
    
    return 
