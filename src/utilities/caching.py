# -*- coding: utf-8 -*-

from transaction_management.deribit.api_requests import (
    get_tickers,)
from utilities.pickling import (
    read_data,)
from utilities.system_tools import (
    provide_path_for_file)

def reading_from_pkl_data(
    end_point, 
    currency,
    status: str = None
    ) -> dict:
    """ """

    path: str = provide_path_for_file (end_point,
                                      currency,
                                      status)
    data = read_data(path)

    return data


# Using the LRUCache decorator function with a maximum cache size of 3
def cached_ticker_data(currencies):
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
    #result = (orjson.loads(data))
    
    result=[]
    for currency in currencies:
        instrument_name = f"{currency}-PERPETUAL"
                        
        result_instrument= reading_from_pkl_data(
                "ticker",
                instrument_name
                )
        
        if result_instrument:
            result_instrument = result_instrument[0]

        else:
            result_instrument = get_tickers (instrument_name)
        result.append (result_instrument)


    return result


def update_cached_ticker(
    instrument_name,
    ticker,
    data_orders):
    """_summary_
    https://stackoverflow.com/questions/73064997/update-values-in-a-list-of-dictionaries

    Args:
        instrument_ticker (_type_): _description_

    Returns:
        _type_: _description_
    """
    #result = (orjson.loads(data))
        
    instrument_ticker = [o for o in ticker if instrument_name in o["instrument_name"]]
    
    if instrument_ticker:
        
        for item in data_orders:
            if "stats" not in item and "instrument_name" not in item and "type" not in item:
                [o for o in ticker if instrument_name in o["instrument_name"]][0][item] = data_orders[item]
            #[o for o in ticker if instr
            if "stats"  in item:
                data_orders_stat = data_orders[item]
                for item in data_orders_stat:
                    [o for o in ticker if instrument_name in o["instrument_name"]][0]["stats"][item] = data_orders_stat[item]
    

