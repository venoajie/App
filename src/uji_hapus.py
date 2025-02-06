# built ins
import asyncio

from loguru import logger as log

# user defined formula
from db_management.sqlite_management import (
    executing_query_with_return,
    insert_tables,
    querying_ohlc_price_vol,
    querying_table,
)
from utilities.string_modification import get_strings_before_character

data = {"type": "user", "info": {"name": "Alice", "age": 30}}

match data:
    case {"type": "user", "info": {"name": str(name), "age": int(age)}}:
        print(f"User: {name}, Age: {age}")

number = 0


def loop_test():
    for number in range(10):
        if number == 1:
            break  # break here

    return number


print(loop_test())

orders = [
    {
        "oto_order_ids": ["OTO-80322590"],
        "is_liquidation": False,
        "risk_reducing": False,
        "order_type": "limit",
        "creation_timestamp": 1733172624209,
        "order_state": "open",
        "reject_post_only": False,
        "contracts": 1.0,
        "average_price": 0.0,
        "reduce_only": False,
        "trigger_fill_condition": "incremental",
        "last_update_timestamp": 1733172624209,
        "filled_amount": 0.0,
        "replaced": False,
        "post_only": True,
        "mmp": False,
        "web": True,
        "api": False,
        "instrument_name": "BTC-PERPETUAL",
        "max_show": 10.0,
        "time_in_force": "good_til_cancelled",
        "direction": "buy",
        "amount": 10.0,
        "order_id": "81944428472",
        "price": 90000.0,
        "label": "",
    },
    {
        "is_liquidation": False,
        "risk_reducing": False,
        "order_type": "limit",
        "creation_timestamp": 1733172624177,
        "order_state": "untriggered",
        "average_price": 0.0,
        "reduce_only": False,
        "trigger_fill_condition": "incremental",
        "last_update_timestamp": 1733172624177,
        "filled_amount": 0.0,
        "is_secondary_oto": True,
        "replaced": False,
        "post_only": False,
        "mmp": False,
        "web": True,
        "api": False,
        "instrument_name": "BTC-PERPETUAL",
        "max_show": 10.0,
        "time_in_force": "good_til_cancelled",
        "direction": "sell",
        "amount": 10.0,
        "order_id": "OTO-80322590",
        "price": 100000.0,
        "label": "",
    },
]
print("oto_order_ids" in (orders[0]))
print(orders)


async def get_price_ohlc(price: str, table: str, window: int = 100) -> list:
    """ """

    # get query for close price
    get_ohlc_query = querying_ohlc_price_vol(price, table, window)

    # executing query above
    ohlc_all = await executing_query_with_return(get_ohlc_query)

    return ohlc_all


async def cleaned_up_ohlc(price: str, table: str, window: int = 100) -> list:
    """ """

    # get query for close price
    ohlc_all = await get_price_ohlc(price, table, window)

    # log.warning(f" ohlc_all {ohlc_all}")

    # pick value only
    ohlc = [o[price] for o in ohlc_all]
    tick = [o["tick"] for o in ohlc_all]

    ohlc.reverse()
    tick.reverse()
    ohlc_window = ohlc[: window - 1]
    ohlc_price = ohlc_window[-1:][0]
    # log.error (f"ohlc_price {ohlc_price}")

    return dict(
        tick=max(tick),
        ohlc=ohlc_window,
        ohlc_price=ohlc_price,
        last_price=ohlc[-1:][0],
    )


from timeit import timeit

import numpy as np

n = 10
x_np = np.random.randn(n)  # your data
x_list = list(x_np)
ratio: float = 0.9
print(x_np)


def ema_list(x, ratio):
    y = [x[0]]
    log.warning(y)
    for k in range(1, n):
        log.debug(n)
        y.append(y[-1] * ratio + x[k] * (1 - ratio))
    log.error(y)
    return y


log.info(timeit(lambda: ema_list(x_list, ratio), number=1))


def ema_list(ohlc, ratio):

    y = [ohlc[0]]

    for k in range(1, len(ohlc)):

        y.append(y[-1] * ratio + ohlc[k] * (1 - ratio))

    return y


import string
from utilities.string_modification import get_unique_elements

updated_data_all = [
    {'tick': 1738807200000, 'open': 2789.9, 'high': 2795.6, 'low': 2785.75, 'close': 2792.75}, 
    {'tick': 1738807500000, 'open': 2793.65, 'high': 2804.1, 'low': 2790.55, 'close': 2802.5},
    {'tick': 1738807800000, 'open': 2802.3, 'high': 2805.75, 'low': 2797.65, 'close': 2801.8},
    {'tick': 1738808100000, 'open': 2802.7, 'high': 2810.3, 'low': 2797.65, 'close': 2805.5}, 
    {'tick': 1738808400000, 'open': 2806.55, 'high': 2816.65, 'low': 2803.7, 'close': 2806.4}, 
    {'tick': 1738808700000, 'open': 2806.75, 'high': 2813.95, 'low': 2805.9, 'close': 2813.95}
    ] 

ohlc_system = [
    {'tick': 1738806900000, 'open': 2790.9, 'high': 2793.95, 'low': 2788.5, 'close': 2788.95}, 
    {'tick': 1738807200000, 'open': 2789.9, 'high': 2795.6, 'low': 2785.75, 'close': 2792.75},
    {'tick': 1738807500000, 'open': 2793.65, 'high': 2804.1, 'low': 2790.55, 'close': 2802.5}, 
    {'tick': 1738807800000, 'open': 2802.3, 'high': 2805.75, 'low': 2797.65, 'close': 2801.8}, 
    {'tick': 1738808100000, 'open': 2802.7, 'high': 2810.3, 'low': 2797.65, 'close': 2805.5},
    {'tick': 1738808400000, 'open': 2806.55, 'high': 2816.65, 'low': 2803.7, 'close': 2808.05}
                    ] 

elements = get_unique_elements(ohlc_system,updated_data_all)
print (elements)