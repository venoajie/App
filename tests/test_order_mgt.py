# -*- coding: utf-8 -*-
import pytest

from transaction_management.deribit.orders_management import (
    get_custom_label_oto,
    labelling_unlabelled_order_oto,)

orders = [
        {
            'oto_order_ids': ['OTO-80322590'], 'is_liquidation': False, 'risk_reducing': False,
            'order_type': 'limit', 'creation_timestamp': 1733172624209, 'order_state': 'open',
            'reject_post_only': False, 'contracts': 1.0, 'average_price': 0.0, 'reduce_only': False, 
            'trigger_fill_condition': 'incremental', 'last_update_timestamp': 1733172624209, 
            'filled_amount': 0.0, 'replaced': False, 'post_only': True, 'mmp': False, 'web': True, 
            'api': False, 'instrument_name': 'BTC-PERPETUAL', 'max_show': 10.0, 'time_in_force': 'good_til_cancelled', 
            'direction': 'buy', 'amount': 10.0, 'order_id': '81944428472', 'price': 90000.0, 'label': ''},
        {
            'is_liquidation': False, 'risk_reducing': False, 'order_type': 'limit', 
            'creation_timestamp': 1733172624177, 'order_state': 'untriggered', 'average_price': 0.0, 
            'reduce_only': False, 'trigger_fill_condition': 'incremental', 'last_update_timestamp': 1733172624177, 
            'filled_amount': 0.0, 'is_secondary_oto': True, 'replaced': False, 'post_only': False, 'mmp': False, 
            'web': True, 'api': False, 'instrument_name': 'BTC-PERPETUAL', 'max_show': 10.0, 
            'time_in_force': 'good_til_cancelled', 'direction': 'sell', 'amount': 10.0, 
            'order_id': 'OTO-80322590', 'price': 100000.0, 'label': ''}
        ]

transaction_main = [o for o in orders if "OTO" not in o["order_id"]][0]

transaction_secondary = [o for o in orders if "OTO" in o["order_id"]][0]

    
@pytest.mark.parametrize("transaction_main, expected", [
    (transaction_main,  {'closed': 'customLong-closed-1733172624209', 
               'open': 'customLong-open-1733172624209'}),
    ])
def test_get_custom_label_oto (transaction_main,
                               expected):
    
    result = get_custom_label_oto (transaction_main)

    assert result == expected
    
        
@pytest.mark.parametrize("transaction_main, transaction_secondary, expected", [
    (transaction_main,  transaction_secondary, {'everything_is_consistent': True, 'order_allowed': True, 'type': 'limit', 
         'entry_price': 90000.0, 'size': 10.0, 'label': 'customLong-open-1733172624209', 'side': 'buy', 
         'otoco_config': [{'amount': 10.0, 'direction': 'sell', 'type': 'limit', 
                           'label': 'customLong-closed-1733172624209', 'price': 100000,
                           'time_in_force': 'good_til_cancelled', 'post_only': True}]
         }),
    ])
def test_labelling_unlabelled_order_oto (
    transaction_main,
    transaction_secondary,
    expected):
    
    result = labelling_unlabelled_order_oto (
        transaction_main,
        transaction_secondary)

    assert result == expected
    
    