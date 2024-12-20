import numpy as np
from datetime import datetime, timedelta, timezone
from loguru import logger as log
import pandas as pd

from utilities.string_modification import (transform_nested_dict_to_list_ohlc)

nump= [
    {"liquidity": "M", "risk_reducing": False, "order_type": "limit", "trade_id": "329163428", "fee_currency": "BTC", "contracts": 1.0, "self_trade": False, "reduce_only": False, "post_only": True, "mmp": False, "fee": 0.0, "tick_direction": 1, "matching_id": None, "mark_price": 98292.08, "api": True, "trade_seq": 224809036, "instrument_name": "BTC-PERPETUAL", "profit_loss": -4.1e-07, "index_price": 98229.85, "direction": "sell", "amount": 10.0, "order_id": "81484353138", "price": 98277.0, "state": "filled", "timestamp": 1732429227364, "label": "futureSpread-closed-1732285058841"},
    {"liquidity": "M", "risk_reducing": False, "order_type": "limit", "trade_id": "329163380", "fee_currency": "BTC", "contracts": 1.0, "self_trade": False, "reduce_only": False, "post_only": True, "mmp": False, "fee": 0.0, "tick_direction": 1, "matching_id": None, "mark_price": 98240.81, "api": True, "trade_seq": 224809002, "instrument_name": "BTC-PERPETUAL", "profit_loss": -4.5e-07, "index_price": 98181.77, "direction": "sell", "amount": 10.0, "order_id": "81484332065", "price": 98238.0, "state": "filled", "timestamp": 1732429113025, "label": "futureSpread-closed-1732285058841"},
    {"liquidity": "M", "risk_reducing": False, "order_type": "limit", "trade_id": "328935563", "fee_currency": "BTC", "contracts": 1.0, "self_trade": False, "reduce_only": False, "post_only": True, "mmp": False, "fee": -1e-08, "tick_direction": 3, "matching_id": None, "mark_price": 98433.87, "api": True, "trade_seq": 1256, "instrument_name": "BTC-6DEC24", "profit_loss": 0.0, "index_price": 97705.94, "direction": "sell", "amount": 10.0, "order_id": "81422720796", "price": 98435.0, "state": "filled", "timestamp": 1732284144654, "label": "hedgingSpot-open-1732284137844"}
       ]


users_data = [
    {"user_id": 1, "plan_type": "basic", "data_usage": 300},
    {"user_id": 2, "plan_type": "premium", "data_usage": 500},
    {"user_id": 3, "plan_type": "basic", "data_usage": 100},
    {"user_id": 4, "plan_type": "premium", "data_usage": 800}
]# Converting list of dictionaries to a structured NumPy array
dtype = [("user_id", "i4"), ("plan_type", "U10"), ("data_usage", "i4")]
np_users_data = np.array([tuple(user.values()) for user in users_data], dtype=dtype)
print (np_users_data)
vtr1 = np.array(nump)   

dtype = [
    
    ("liquidity","U1"), 
    ("risk_reducing", "bool"),
    ("order_type","U5"),
    ("trade_id", "U12"), 
    ("fee_currency", "U5"), 
    ("contracts", "f4"), 
    ("self_trade", "bool"),
    ("reduce_only", "bool"),
    ("post_only", "bool"), 
    ("mmp", "bool"),
    ("fee", "f4"),
    ("tick_direction", "i4"), 
    ("matching_id", "bool"),
    ("mark_price", "f4"),
    ("api", "bool"),
    ("trade_seq", "i4"), 
    ("instrument_name", "U20"), 
    ("profit_loss", "f4"), 
    ("index_price", "f4"),
    ("direction", "U5"), 
    ("amount", "f4"), 
    ("order_id", "U12"), 
    ("price", "f4"), 
    ("state", "U10"),
    ("timestamp", "i8"), 
    ("label", "U30")
    ]

np_users_data = np.array([tuple(user.values()) for user in vtr1], dtype=dtype)
print (np_users_data)

arr = np.array([1, 2, 3, 4, 5])
result = np.where(arr > 3, 'Large', 'Small')
print(result)

from functools import reduce

data = [1, 2, 3, 4, 5]
product = reduce(lambda x, y: x * y, data)  # Calculates

print (product)




def cached_ohlc_data(
    currencies: list,
    resolutions: list):
    """_summary_
    https://blog.apify.com/python-cache-complete-guide/]
    data caching
    https://medium.com/@ryan_forrester_/python-return-statement-complete-guide-138c80bcfdc7

    Args:
        instrument_ticker (_type_): _description_

    Returns:
        _type_: _description_
    """

    import httpx
    from utilities.time_modification import convert_time_to_unix
    
    from websocket_management.allocating_ohlc import (
        ohlc_end_point, )   
    
    qty_candles = 5
    
    now_utc = datetime.now()
    now_unix = convert_time_to_unix(now_utc)

    result=[]
    for currency in currencies:
        instrument_name = f"{currency}-PERPETUAL"
        for resolution in resolutions:
            
            start_timestamp = now_unix - (60000 * resolution) * qty_candles
                
            end_point = ohlc_end_point(instrument_name,
                            resolution,
                            start_timestamp,
                            now_unix,
                            )
            
            with httpx.Client() as client:
                ohlc_request = client.get(
                    end_point, 
                    follow_redirects=True
                    ).json()["result"]

                result = transform_nested_dict_to_list_ohlc(ohlc_request)

            items_to_be_removed = ["status", "cost"]
            
            tick_max = max([o for o in ohlc_request["ticks"]])

            ohlc_request_cleaned= {i:ohlc_request [i] for i in ohlc_request if i not in items_to_be_removed}
            
            ohlc_request_cleaned.update({"resolution": resolution})

            ohlc_request_cleaned.update({"instrument_name": instrument_name})

            # remove_dict_elements
            
            ohlc_request_cleaned.update({"tick": tick_max})

            result.append (ohlc_request_cleaned)
    
    return result


def my_generator(data,lookback):
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


    if wicks_up < 0:wicks_up=wicks_up*(-1)
    if wicks_down < 0:wicks_down=wicks_down*(-1)
    if body_size < 0:body_size=body_size*(-1)
    candlestick_data[0]=candle_type
    candlestick_data[1]=round(round(wicks_up,5)*10000,2)
    candlestick_data[2]=round(round(wicks_down,5)*10000,2)
    candlestick_data[3]=round(round(body_size,5)*10000,2)

    return candlestick_data

def my_generator_candle(data,lookback):
    first_row = 0
    arr = np.empty((1,lookback,4), int)
    for a in range(len(data)-lookback):
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

my_dataset = pd.read_csv('dataset.csv')
del my_dataset['Local time']
del my_dataset['Volume']

log.error (f"my_dataset {my_dataset}")
three_dim_sequence = np.asarray(my_generator_candle(my_dataset.values[1:],3))
log.warning (f"three_dim_sequence        {three_dim_sequence}")

currencies = ["BTC", "ETH"]
resolutions = [60, 15]

my_data = cached_ohlc_data(
    currencies,
    resolutions)
log.warning (my_data)

for data in my_data:

    three_dim_sequence = np.asarray(my_generator(data.values[1:],3))
    log.error (f"three_dim_sequence = np.asarray(my_generator(my_dataset.values[1:],3)) {three_dim_sequence}")