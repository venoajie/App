from loguru import logger as log


transactions = [ {'order_allowed': True, 'order_parameters': {'size': 200.0, 'label': 'futureSpread-closed-1730853512814'}, 'cancel_allowed': False, 'cancel_id': None}, 
                    {'order_allowed': True, 'order_parameters': {'size': 200.0, 'label': 'futureSpread-closed-1730853452649'}, 'cancel_allowed': False, 'cancel_id': None}
                ]
will_be_closed = []

for transaction in transactions:
    log.error (transaction)
    will_be_closed.append(transaction.copy())
log.warning (will_be_closed)