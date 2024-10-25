sub_account = {'positions': [{'estimated_liquidation_price': None, 'size_currency': -0.000292631, 'total_profit_loss': -7.51e-07, 'realized_profit_loss': 0.0, 'floating_profit_loss': -4.788e-06, 'leverage': 25, 'average_price': 68170.5, 'delta': -0.000292631, 'mark_price': 68345.47, 'settlement_price': 67245.09, 'instrument_name': 'BTC-1NOV24', 'index_price': 68119.71, 'direction': 'sell', 'open_orders_margin': 0.0, 'initial_margin': 1.1705e-05, 'maintenance_margin': 5.853e-06, 'kind': 'future', 'size': -20.0}, {'estimated_liquidation_price': None, 'size_currency': -0.004109001, 'total_profit_loss': -0.000133189, 'realized_profit_loss': 0.0, 'floating_profit_loss': -6.4059e-05, 'leverage': 25, 'average_price': 66003.64, 'delta': -0.004109001, 'mark_price': 68143.09, 'settlement_price': 67097.05, 'instrument_name': 'BTC-25OCT24', 'index_price': 68119.71, 'direction': 'sell', 'open_orders_margin': 0.0, 'initial_margin': 0.000164361, 'maintenance_margin': 8.2181e-05, 'kind': 'future', 'size': -280.0}, {'estimated_liquidation_price': None, 'size_currency': 0.004988649, 'realized_funding': -1.84e-06, 'total_profit_loss': 6.4429e-05, 'realized_profit_loss': -1.835e-06, 'floating_profit_loss': 7.9973e-05, 'leverage': 50, 'average_price': 67285.72, 'delta': 0.004988649, 'interest_value': 0.15543975329023996, 'mark_price': 68154.73, 'settlement_price': 67079.38, 'instrument_name': 'BTC-PERPETUAL', 'index_price': 68119.71, 'direction': 'buy', 'open_orders_margin': 0.0, 'initial_margin': 9.9774e-05, 'maintenance_margin': 4.9888e-05, 'kind': 'future', 'size': 340.0}], 'open_orders': [], 'uid': 148510}

position = sub_account["positions"]
result = [{'instrument_name':o['instrument_name'], 'size_all':(i['size']  for i in position)} for o in position]

size_all = [abs(o["size"]) for o in position]
print (f"size_all {size_all}")
print (f"sub_account {result}")