
# built ins
import asyncio
from loguru import logger as log
# user defined formula
from db_management.sqlite_management import (
    executing_query_with_return,
    querying_table,
    querying_ohlc_price_vol,
    insert_tables,)

data = {"type": "user", "info": {"name": "Alice", "age": 30}}  

match data:  
    case {"type": "user", "info": {"name": str(name), "age": int(age)}}:  
        print(f"User: {name}, Age: {age}")
        
number = 0

def loop_test():
    for number in range(10):
        if number == 1:
            break    # break here

    return number

print(loop_test())

orders = [{'oto_order_ids': ['OTO-80322590'], 'is_liquidation': False, 'risk_reducing': False, 'order_type': 'limit', 'creation_timestamp': 1733172624209, 'order_state': 'open', 'reject_post_only': False, 'contracts': 1.0, 'average_price': 0.0, 'reduce_only': False, 'trigger_fill_condition': 'incremental', 'last_update_timestamp': 1733172624209, 'filled_amount': 0.0, 'replaced': False, 'post_only': True, 'mmp': False, 'web': True, 'api': False, 'instrument_name': 'BTC-PERPETUAL', 'max_show': 10.0, 'time_in_force': 'good_til_cancelled', 'direction': 'buy', 'amount': 10.0, 'order_id': '81944428472', 'price': 90000.0, 'label': ''}, {'is_liquidation': False, 'risk_reducing': False, 'order_type': 'limit', 'creation_timestamp': 1733172624177, 'order_state': 'untriggered', 'average_price': 0.0, 'reduce_only': False, 'trigger_fill_condition': 'incremental', 'last_update_timestamp': 1733172624177, 'filled_amount': 0.0, 'is_secondary_oto': True, 'replaced': False, 'post_only': False, 'mmp': False, 'web': True, 'api': False, 'instrument_name': 'BTC-PERPETUAL', 'max_show': 10.0, 'time_in_force': 'good_til_cancelled', 'direction': 'sell', 'amount': 10.0, 'order_id': 'OTO-80322590', 'price': 100000.0, 'label': ''}]
print ("oto_order_ids" in (orders[0]))
print (orders)

async def get_price_ohlc(
    price: str, 
    table: str, 
    window: int = 100
) -> list:
    """ """

    # get query for close price
    get_ohlc_query = querying_ohlc_price_vol(price, table, window)

    # executing query above
    ohlc_all = await executing_query_with_return(get_ohlc_query)

    return ohlc_all


async def cleaned_up_ohlc(
    price: str, 
    table: str,
    window: int = 100
) -> list:
    """ """

    # get query for close price
    ohlc_all = await get_price_ohlc(price, 
                                    table, 
                                    window)

    #log.warning(f" ohlc_all {ohlc_all}")

    # pick value only
    ohlc = [o[price] for o in ohlc_all]
    tick = [o["tick"] for o in ohlc_all]

    ohlc.reverse()
    tick.reverse()
    ohlc_window = ohlc[: window - 1]
    ohlc_price = ohlc_window[-1:][0]
    #log.error (f"ohlc_price {ohlc_price}")

    return dict(
        tick=max(tick), ohlc=ohlc_window, ohlc_price=ohlc_price, last_price=ohlc[-1:][0]
    )

import numpy as np
from timeit import timeit

n=10
x_np = np.random.randn(n) # your data
x_list = list(x_np)
ratio: float = 0.9 
print(x_np)
def ema_list(x, ratio):
     y = [x[0]]
     log.warning (y)
     for k in range(1, n):
         log.debug (n)
         y.append(y[-1]*ratio + x[k]*(1-ratio))
     log.error (y)
     return y




log.info(timeit(lambda: ema_list(x_list, ratio), number=1))

def ema_list(x, ratio):
     y = [x[0]]
     log.warning (y)
     for k in range(1, len(x)):
         log.debug (len(x))
         y.append(y[-1]*ratio + x[k]*(1-ratio))
     log.error (y)
     return y
 
table_1 = f"ohlc1_btc_perp_json" 
ohlc_1_high_9 = asyncio. run(cleaned_up_ohlc("high", table_1, 10))

ohlc = ohlc_1_high_9["ohlc"] 

log.info (ohlc)
ema= ema_list(ohlc,ratio)

log.debug (ema)