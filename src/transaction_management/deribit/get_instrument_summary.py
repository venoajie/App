# -*- coding: utf-8 -*-

# built ins
import asyncio
# user defined formula

from utilities.system_tools import (
    provide_path_for_file,)
from utilities.pickling import read_data
from utilities.string_modification import (
    remove_double_brackets_in_list,)

def get_instruments_kind(
    currency: str,
    settlement_periods,
    kind: str= "all"
    ) -> list:
    """_summary_

    Args:
        currency (str): _description_
        kind (str): "future_combo",  "future"
        Instance:  [
                    {'tick_size_steps': [], 'quote_currency': 'USD', 'min_trade_amount': 1,'counter_currency': 'USD', 
                    'settlement_period': 'month', 'settlement_currency': 'ETH', 'creation_timestamp': 1719564006000, 
                    'instrument_id': 342036, 'base_currency': 'ETH', 'tick_size': 0.05, 'contract_size': 1, 'is_active': True, 
                    'expiration_timestamp': 1725004800000, 'instrument_type': 'reversed', 'taker_commission': 0.0, 
                    'maker_commission': 0.0, 'instrument_name': 'ETH-FS-27SEP24_30AUG24', 'kind': 'future_combo', 
                    'rfq': False, 'price_index': 'eth_usd'}, ]
     Returns:
        list: _description_
        
        
    """ 
    
    my_path_instruments = provide_path_for_file(
        "instruments", 
        currency
    )

    instruments_raw = read_data(my_path_instruments)
    print (f" instruments_raw {instruments_raw}")
    instruments = instruments_raw[0]["result"]
    non_spot_instruments=  [
        o for o in instruments if o["kind"] != "spot"]
    instruments_kind= non_spot_instruments if kind =="all" else  [
        o for o in instruments if o["kind"] == kind]
    
    return  [o for o in instruments_kind \
        if o["settlement_period"] in settlement_periods]


def get_futures_for_active_currencies(
    active_currencies,
    settlement_periods) -> list:
    """_summary_

    Returns:
        list: _description_
    """
    
    instruments_holder_place= []
    for currency in active_currencies:

        future_instruments= get_instruments_kind (currency,
                                                  settlement_periods,
                                                  "future" )

        print (f" future_instruments {future_instruments}")
        future_combo_instruments= get_instruments_kind (currency,
                                                  settlement_periods,
                                                  "future_combo" )
        
        print (f" future_combo_instruments {future_combo_instruments}")
        active_combo_perp = [o for o in future_combo_instruments \
            if "_PERP" in o["instrument_name"]]
        
        print (f" active_combo_perp {active_combo_perp}")
        combined_instruments = future_instruments + active_combo_perp
        instruments_holder_place.append(combined_instruments)    
    
    #removing inner list 
    # typical result: [['BTC-30AUG24', 'BTC-6SEP24', 'BTC-27SEP24', 'BTC-27DEC24', 
    # 'BTC-28MAR25', 'BTC-27JUN25', 'BTC-PERPETUAL'], ['ETH-30AUG24', 'ETH-6SEP24', 
    # 'ETH-27SEP24', 'ETH-27DEC24', 'ETH-28MAR25', 'ETH-27JUN25', 'ETH-PERPETUAL']]
    
    instruments_holder_plc= []
    for instr in instruments_holder_place:
        instruments_holder_plc.append(instr)

    return remove_double_brackets_in_list(instruments_holder_plc)
    
    
def get_futures_instruments(
    active_currencies, 
    settlement_periods
    ) -> list:
    
    active_futures=   get_futures_for_active_currencies(
            active_currencies,
            settlement_periods)
              
    min_expiration_timestamp = min([o["expiration_timestamp"] for o in active_futures]) 
    
    return dict(
        instruments_name = [o["instrument_name"] for o in (active_futures)],
        min_expiration_timestamp = min_expiration_timestamp,
        active_futures = [o for o in active_futures if "future" in o["kind"]],
        active_combo =  [o for o in active_futures if "future_combo" in o["kind"]],
        instruments_name_with_min_expiration_timestamp = [o["instrument_name"] for o in active_futures \
            if o["expiration_timestamp"] == min_expiration_timestamp][0]
        )
    
