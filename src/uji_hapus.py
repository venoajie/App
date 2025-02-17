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
import numpy as np

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


new_data = {"tick": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"}


cached_candles_data = [
    {
        "instrument_name": "BTC-PERPETUAL",
        "resolution": 60,
        "ohlc": [
            {
                "tick": 1738810800000,
                "open": 97398.5,
                "high": 97500.0,
                "low": 97115.0,
                "close": 97360.0,
            },
            {
                "tick": 1738814400000,
                "open": 97384.5,
                "high": 97872.0,
                "low": 97306.5,
                "close": 97672.0,
            },
            {
                "tick": 1738818000000,
                "open": 97672.5,
                "high": 97906.5,
                "low": 97635.5,
                "close": 97860.0,
            },
            {
                "tick": 1738821600000,
                "open": 97860.0,
                "high": 98474.0,
                "low": 97753.0,
                "close": 98390.0,
            },
            {
                "tick": 1738825200000,
                "open": 98398.5,
                "high": 98401.0,
                "low": 98100.0,
                "close": 98257.5,
            },
            {
                "tick": 1738828800000,
                "open": 98257.5,
                "high": 98339.0,
                "low": 97996.0,
                "close": 98083.5,
            },
        ],
    },
    {
        "instrument_name": "BTC-PERPETUAL",
        "resolution": 15,
        "ohlc": [
            {
                "tick": 1738826100000,
                "open": 98129.0,
                "high": 98309.0,
                "low": 98129.0,
                "close": 98210.5,
            },
            {
                "tick": 1738827000000,
                "open": 98238.5,
                "high": 98364.0,
                "low": 98185.5,
                "close": 98189.0,
            },
            {
                "tick": 1738827900000,
                "open": 98189.0,
                "high": 98282.0,
                "low": 98100.0,
                "close": 98257.5,
            },
            {
                "tick": 1738828800000,
                "open": 98257.5,
                "high": 98339.0,
                "low": 98160.5,
                "close": 98210.5,
            },
            {
                "tick": 1738829700000,
                "open": 98221.0,
                "high": 98250.0,
                "low": 98110.5,
                "close": 98120.0,
            },
            {
                "tick": 1738830600000,
                "open": 98119.5,
                "high": 98166.0,
                "low": 97996.0,
                "close": 98083.5,
            },
        ],
    },
    {
        "instrument_name": "BTC-PERPETUAL",
        "resolution": 5,
        "ohlc": [
            {
                "tick": 1738829400000,
                "open": 98325.5,
                "high": 98325.5,
                "low": 98190.0,
                "close": 98210.5,
            },
            {
                "tick": 1738829700000,
                "open": 98221.0,
                "high": 98250.0,
                "low": 98198.5,
                "close": 98214.5,
            },
            {
                "tick": 1738830000000,
                "open": 98218.0,
                "high": 98218.0,
                "low": 98117.0,
                "close": 98128.0,
            },
            {
                "tick": 1738830300000,
                "open": 98128.0,
                "high": 98146.5,
                "low": 98110.5,
                "close": 98120.0,
            },
            {
                "tick": 1738830600000,
                "open": 98119.5,
                "high": 98166.0,
                "low": 98064.5,
                "close": 98064.5,
            },
            {
                "tick": 1738830900000,
                "open": 98052.0,
                "high": 98109.0,
                "low": 97996.0,
                "close": 98083.5,
            },
        ],
    },
    {
        "instrument_name": "ETH-PERPETUAL",
        "resolution": 60,
        "ohlc": [
            {
                "tick": 1738810800000,
                "open": 2808.0,
                "high": 2819.55,
                "low": 2790.05,
                "close": 2807.3,
            },
            {
                "tick": 1738814400000,
                "open": 2806.5,
                "high": 2832.2,
                "low": 2806.35,
                "close": 2822.0,
            },
            {
                "tick": 1738818000000,
                "open": 2822.65,
                "high": 2850.3,
                "low": 2821.3,
                "close": 2832.75,
            },
            {
                "tick": 1738821600000,
                "open": 2832.7,
                "high": 2857.1,
                "low": 2823.45,
                "close": 2853.35,
            },
            {
                "tick": 1738825200000,
                "open": 2853.6,
                "high": 2855.3,
                "low": 2827.55,
                "close": 2833.35,
            },
            {
                "tick": 1738828800000,
                "open": 2833.2,
                "high": 2846.35,
                "low": 2825.0,
                "close": 2833.85,
            },
        ],
    },
    {
        "instrument_name": "ETH-PERPETUAL",
        "resolution": 15,
        "ohlc": [
            {
                "tick": 1738826100000,
                "open": 2838.3,
                "high": 2845.2,
                "low": 2838.3,
                "close": 2840.2,
            },
            {
                "tick": 1738827000000,
                "open": 2840.55,
                "high": 2844.2,
                "low": 2834.2,
                "close": 2837.75,
            },
            {
                "tick": 1738827900000,
                "open": 2837.65,
                "high": 2838.9,
                "low": 2827.55,
                "close": 2833.35,
            },
            {
                "tick": 1738828800000,
                "open": 2833.2,
                "high": 2845.3,
                "low": 2825.0,
                "close": 2837.3,
            },
            {
                "tick": 1738829700000,
                "open": 2837.4,
                "high": 2846.35,
                "low": 2835.95,
                "close": 2840.25,
            },
            {
                "tick": 1738830600000,
                "open": 2840.25,
                "high": 2842.1,
                "low": 2829.35,
                "close": 2833.85,
            },
        ],
    },
    {
        "instrument_name": "ETH-PERPETUAL",
        "resolution": 5,
        "ohlc": [
            {
                "tick": 1738829400000,
                "open": 2841.1,
                "high": 2842.0,
                "low": 2835.9,
                "close": 2837.3,
            },
            {
                "tick": 1738829700000,
                "open": 2837.4,
                "high": 2844.35,
                "low": 2835.95,
                "close": 2844.35,
            },
            {
                "tick": 1738830000000,
                "open": 2844.4,
                "high": 2846.25,
                "low": 2840.25,
                "close": 2843.5,
            },
            {
                "tick": 1738830300000,
                "open": 2843.65,
                "high": 2846.35,
                "low": 2839.1,
                "close": 2840.25,
            },
            {
                "tick": 1738830600000,
                "open": 2840.25,
                "high": 2842.1,
                "low": 2833.5,
                "close": 2833.5,
            },
            {
                "tick": 1738830900000,
                "open": 2832.8,
                "high": 2835.45,
                "low": 2829.35,
                "close": 2833.85,
            },
        ],
    },
]
import ast


json_load =  {
    'type': 'message', 
    'pattern': None, 
    'channel': 'market.ticker.data', 
    'data': '{"server_time":1739699263756,"data":{"timestamp":1739699263756,"type":"change","index_price":97297.07,"instrument_name":"BTC-FS-27JUN25_PERP","min_price":2457.0,"max_price":3435.0,"mark_price":2945.99},"currency":"btc","instrument_name":"BTC-FS-27JUN25_PERP","currency_upper":"BTC"}'}

log.warning(json_load["data"])

instrument_name = "BTC-PERPETUAL"
sub_acc = [
    {
        'currency': 'BTC', 
        'result': [
            {'uid': 148510,
             'open_orders': [
                 {
                     'label': 'customShort-open-1739749530910', 'price': 99300.0, 'direction': 'sell', 'time_in_force': 'good_til_cancelled', 'max_show': 20.0, 'instrument_name': 'BTC-27JUN25', 'api': True, 'web': False, 'amount': 20.0, 'order_id': '88345521875', 'mmp': False, 'replaced': False, 'post_only': True, 'filled_amount': 0.0, 'last_update_timestamp': 1739749531929, 'reduce_only': False, 'average_price': 0.0, 'contracts': 2.0, 'reject_post_only': False, 'order_state': 'open', 'creation_timestamp': 1739749531929, 'order_type': 'limit', 'is_liquidation': False, 'risk_reducing': False}, {'label': 'customShort-open-1739749392456', 'price': 99300.0, 'direction': 'sell', 'time_in_force': 'good_til_cancelled', 'max_show': 20.0, 'instrument_name': 'BTC-27JUN25', 'api': True, 'web': False, 'amount': 20.0, 'order_id': '88345306377', 'mmp': False, 'replaced': False, 'post_only': True, 'filled_amount': 0.0, 'last_update_timestamp': 1739749393458, 'reduce_only': False, 'average_price': 0.0, 'contracts': 2.0, 'reject_post_only': False, 'order_state': 'open', 'creation_timestamp': 1739749393458, 'order_type': 'limit', 'is_liquidation': False, 'risk_reducing': False}, {'label': 'customShort-open-1739748723107', 'price': 99097.5, 'direction': 'sell', 'time_in_force': 'good_til_cancelled', 'max_show': 20.0, 'instrument_name': 'BTC-27JUN25', 'api': True, 'web': False, 'amount': 20.0, 'order_id': '88344343079', 'mmp': False, 'replaced': False, 'post_only': True, 'filled_amount': 0.0, 'last_update_timestamp': 1739748724123, 'reduce_only': False, 'average_price': 0.0, 'contracts': 2.0, 'reject_post_only': False, 'order_state': 'open', 'creation_timestamp': 1739748724123, 'order_type': 'limit', 'is_liquidation': False, 'risk_reducing': False}
                 ], 
             'positions': [
                 {'size': -100.0, 'kind': 'future', 'maintenance_margin': 2.0789e-05, 'initial_margin': 4.1578e-05, 'open_orders_margin': 0.0, 'direction': 'sell', 'index_price': 96125.81, 'instrument_name': 'BTC-21FEB25', 'settlement_price': 97352.66, 'mark_price': 96204.93, 'delta': -0.001039448, 'average_price': 97999.59, 'leverage': 25, 'floating_profit_loss': 1.2255e-05, 'realized_profit_loss': 0.0, 'total_profit_loss': 1.9036e-05, 'size_currency': -0.001039448, 'estimated_liquidation_price': None}, {'size': -80.0, 'kind': 'future', 'maintenance_margin': 1.5781e-05, 'initial_margin': 3.1561e-05, 'open_orders_margin': 0.0, 'direction': 'sell', 'index_price': 96125.81, 'instrument_name': 'BTC-26SEP25', 'settlement_price': 102629.46, 'mark_price': 101390.01, 'delta': -0.000789032, 'average_price': 101446.25, 'leverage': 25, 'floating_profit_loss': 4.37e-07, 'realized_profit_loss': 0.0, 'total_profit_loss': 4.37e-07, 'size_currency': -0.000789032, 'estimated_liquidation_price': None}, {'size': -40.0, 'kind': 'future', 'maintenance_margin': 8.08e-06, 'initial_margin': 1.6161e-05, 'open_orders_margin': 1.8683e-05, 'direction': 'sell', 'index_price': 96125.81, 'instrument_name': 'BTC-27JUN25', 'settlement_price': 100235.94, 'mark_price': 99006.59, 'delta': -0.000404014, 'average_price': 99056.25, 'leverage': 25, 'floating_profit_loss': 2.03e-07, 'realized_profit_loss': 0.0, 'total_profit_loss': 2.03e-07, 'size_currency': -0.000404014, 'estimated_liquidation_price': None}, {'size': -20.0, 'kind': 'future', 'maintenance_margin': 2.08e-06, 'initial_margin': 4.161e-06, 'open_orders_margin': 0.0, 'direction': 'sell', 'index_price': 96125.81, 'instrument_name': 'BTC-PERPETUAL', 'settlement_price': 97278.97, 'mark_price': 96141.74, 'interest_value': 0.013659020736206413, 'delta': -0.000208026, 'average_price': 96582.5, 'leverage': 50, 'floating_profit_loss': 2.432e-06, 'realized_profit_loss': 6.05e-07, 'total_profit_loss': 9.49e-07, 'realized_funding': 1e-08, 'size_currency': -0.000208026, 'estimated_liquidation_price': None}]}]},
    {'currency': 'ETH', 
     'result': [
         {
             'uid': 148510, 
             'open_orders': [
                 {'label': 'customShort-open-1739748125825', 'price': 2700.0, 'direction': 'sell', 'time_in_force': 'good_til_cancelled', 'max_show': 1.0, 'instrument_name': 'ETH-PERPETUAL', 'api': True, 'web': False, 'amount': 1.0, 'order_id': 'ETH-58697996606', 'mmp': False, 'replaced': False, 'post_only': True, 'filled_amount': 0.0, 'last_update_timestamp': 1739748126852, 'reduce_only': False, 'average_price': 0.0, 'contracts': 1.0, 'reject_post_only': False, 'order_state': 'open', 'creation_timestamp': 1739748126852, 'order_type': 'limit', 'is_liquidation': False, 'risk_reducing': False}, {'label': 'customShort-open-1739747944107', 'price': 2700.0, 'direction': 'sell', 'time_in_force': 'good_til_cancelled', 'max_show': 1.0, 'instrument_name': 'ETH-PERPETUAL', 'api': True, 'web': False, 'amount': 1.0, 'order_id': 'ETH-58697782839', 'mmp': False, 'replaced': False, 'post_only': True, 'filled_amount': 0.0, 'last_update_timestamp': 1739747946285, 'reduce_only': False, 'average_price': 0.0, 'contracts': 1.0, 'reject_post_only': False, 'order_state': 'open', 'creation_timestamp': 1739747946285, 'order_type': 'limit', 'is_liquidation': False, 'risk_reducing': False}
                 ], 
             'positions': [
                 {'size': -10.0, 'kind': 'future', 'maintenance_margin': 7.0308e-05, 'initial_margin': 0.000140615, 'open_orders_margin': 0.0, 'direction': 'sell', 'index_price': 2660.4, 'instrument_name': 'ETH-26DEC25', 'settlement_price': 2880.29, 'mark_price': 2844.64, 'delta': -0.003515383, 'average_price': 2948.5, 'leverage': 25, 'floating_profit_loss': 4.351e-05, 'realized_profit_loss': 0.0, 'total_profit_loss': 0.000123826, 'size_currency': -0.003515383, 'estimated_liquidation_price': None}, {'size': -10.0, 'kind': 'future', 'maintenance_margin': 7.3325e-05, 'initial_margin': 0.000146651, 'open_orders_margin': 0.0, 'direction': 'sell', 'index_price': 2660.4, 'instrument_name': 'ETH-27JUN25', 'settlement_price': 2765.44, 'mark_price': 2727.57, 'delta': -0.003666267, 'average_price': 2823.75, 'leverage': 25, 'floating_profit_loss': 5.0206e-05, 'realized_profit_loss': 0.0, 'total_profit_loss': 0.000124877, 'size_currency': -0.003666267, 'estimated_liquidation_price': None}, {'size': -3.0, 'kind': 'future', 'maintenance_margin': 1.1279e-05, 'initial_margin': 2.2558e-05, 'open_orders_margin': 2.2889e-05, 'direction': 'sell', 'index_price': 2660.4, 'instrument_name': 'ETH-PERPETUAL', 'settlement_price': 2695.45, 'mark_price': 2659.8, 'interest_value': -0.8817779145084984, 'delta': -0.001127904, 'average_price': 2704.87, 'leverage': 50, 'floating_profit_loss': 1.4917e-05, 'realized_profit_loss': -9.2e-08, 'total_profit_loss': 1.8792e-05, 'realized_funding': 0.0, 'size_currency': -0.001127904, 'estimated_liquidation_price': None}]}]}, [{'size': -3.0, 'kind': 'future', 'maintenance_margin': 1.1279e-05, 'initial_margin': 2.2559e-05, 'direction': 'sell', 'index_price': 2660.23, 'instrument_name': 'ETH-PERPETUAL', 'settlement_price': 2695.45, 'mark_price': 2659.7, 'interest_value': -0.8817779145084984, 'delta': -0.001127947, 'average_price': 2704.87, 'leverage': 50, 'floating_profit_loss': 1.496e-05, 'realized_profit_loss': -9.2e-08, 'total_profit_loss': 1.8835e-05, 'realized_funding': 0.0, 'size_currency': -0.001127947}]]

log.debug([
            o
            for o in sub_acc 
        ])

log.warning(sub_acc[0]["currency"])

log.error([
            o
            for o in sub_acc if "BTC" in o["currency"]
        ])