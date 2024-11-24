import numpy as np

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

