
from_lowest_to_highest = [
                            {'instrument_name': 'ETH-PERPETUAL', 'label': 'hedgingSpot-open-1728859651166', 'amount': -5.0, 'price': 2465.65, 'side': 'sell'},
                               {'instrument_name': 'ETH-PERPETUAL', 'label': 'hedgingSpot-closed-1728806470451', 'amount': 3.0, 'price': 2466.25, 'side': 'buy'},
                               {'instrument_name': 'ETH-PERPETUAL', 'label': 'hedgingSpot-closed-1728806470451', 'amount': 3.0, 'price': 2466.4, 'side': 'buy'},
                               {'instrument_name': 'ETH-PERPETUAL', 'label': 'hedgingSpot-closed-1728806470451', 'amount': 3.0, 'price': 2466.45, 'side': 'buy'},
                          {'instrument_name': 'ETH-PERPETUAL', 'label': 'hedgingSpot-open-1728806470451', 'amount': -3.0, 'price': 2466.6, 'side': 'sell'},
                               ]
price = 2466.6
bid_price_future= 2464.3
tp_threshold = .01/100
profited_price = price  - (price*tp_threshold)
print(profited_price)
print(profited_price < bid_price_future  )
unpaired_transactions_futures_with_good_premium = [o for o in from_lowest_to_highest \
                            if  bid_price_future < (o["price"] - (o["price"] * tp_threshold)) ][0]

print (unpaired_transactions_futures_with_good_premium)