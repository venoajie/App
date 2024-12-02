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