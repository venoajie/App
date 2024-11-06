transactions = [{'instrument_name': 'BTC-15NOV24', 'label': 'futureSpread-open-1730865794527', 'amount': -200.0, 'price': 74851.0, 'side': 'sell', 'balance': -40}, {'instrument_name': 'BTC-PERPETUAL', 'label': 'futureSpread-open-1730865794527', 'amount': 200.0, 'price': 74586.0, 'side': 'buy', 'balance': -40}]

transactions_premium = sum([(o["price"]*o["amount"])/abs(o["amount"]) for o in transactions])

print (transactions_premium)