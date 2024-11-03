
from utilities.system_tools import (
    async_raise_error_message,
    provide_path_for_file,
    raise_error_message)
from loguru import logger as log
from utilities.pickling import (
    replace_data,
    read_data,)

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


def compute_notional_value(
    index_price: float,
    equity: float
    ) -> float:
    """ """
    return index_price * equity

data_orders =  {'funding_8h': -9.08e-06, 'current_funding': 0.0, 'best_bid_amount': 19590.0, 'best_ask_amount': 257620.0, 'estimated_delivery_price': 68365.13, 'best_bid_price': 68355.0, 'best_ask_price': 68355.5, 'interest_value': -0.0038330687407680634, 'mark_price': 68349.56, 'open_interest': 646665030, 'max_price': 70400.5, 'min_price': 66299.0, 'settlement_price': 68333.74, 'last_price': 68344.5, 'instrument_name': 'BTC-PERPETUAL', 'index_price': 68365.13, 'stats': {'volume_notional': 302392980.0, 'volume_usd': 302392980.0, 'volume': 4402.97283122, 'price_change': -1.8046, 'low': 67798.0, 'high': 69675.5}, 'state': 'open', 'type': 'snapshot', 'timestamp': 1730634262782}

result_json_ = []    
def ticker_data_alt(data_orders):
    log.info (f"result_json {result_json_}")

    instrument_name = data_orders["instrument_name"]

    if "snapshot" in data_orders["type"]:
        my_path_ticker = provide_path_for_file(
            "ticker", instrument_name)
        replace_data(my_path_ticker,data_orders)

    result_json = ((reading_from_pkl_data(
                "ticker",
                instrument_name
                )))

    log.info (result_json[0])
    log.info ( result_json[0]["instrument_name"])
    log.info (instrument_name in result_json[0]["instrument_name"])

    if instrument_name in result_json[0]["instrument_name"]:
        
        for item in data_orders:
            log.critical (item)
            result_json[0][item] = data_orders[item]
    else:
        result_json  += result_json_
    
    return result_json


data_orders = {'funding_8h': -9.34e-06, 'current_funding': 0.0, 'best_bid_amount': 14160.0, 'best_ask_amount': 231920.0, 'estimated_delivery_price': 68324.95, 'best_bid_price': 68311.0, 'best_ask_price': 68311.5, 'interest_value': -0.003970725424831761, 'mark_price': 68309.48, 'open_interest': 646807370, 'max_price': 70359.0, 'min_price': 66260.0, 'settlement_price': 68333.74, 'last_price': 68311.0, 'instrument_name': 'BTC-PERPETUAL', 'index_price': 68324.95, 'stats': {'volume_notional': 302129990.0, 'volume_usd': 302129990.0, 'volume': 4399.38068093, 'price_change': -1.7822, 'low': 67798.0, 'high': 69650.0}, 'state': 'open', 'type': 'snapshot', 'timestamp': 1730635135158}
data ={}

print (data | data_orders)