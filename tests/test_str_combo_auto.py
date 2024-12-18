# -*- coding: utf-8 -*-
import pytest


from strategies.cash_carry.combo_auto import (
    delta_premium_pct,
    determine_opening_size,
    get_basic_opening_size,
    get_transactions_premium,
    is_new_transaction_will_reduce_delta,
    proforma_delta,
    )

transactions1= [{'instrument_name': 'BTC-PERPETUAL', 'label': 'futureSpread-open-1731647294383', 'amount': 40.0, 'price': 87792.0, 'side': 'buy', 'balance': 10}, {'instrument_name': 'BTC-28MAR25', 'label': 'futureSpread-open-1731647294383', 'amount': -40.0, 'price': 91810.0, 'side': 'sell', 'balance': 10}]

@pytest.mark.parametrize("transactions, expected", [
    (transactions1, 4018.0 ),
    ])
def test_get_transactions_premium (transactions, 
                                        expected):
    
    result = get_transactions_premium (transactions)

    assert result == expected
    
    
@pytest.mark.parametrize("transactions, current_premium, expected", [
    (4018, 4018.0, 0 ),
    (4018, 4000.0,  0.004479840716774514   ),
    (4018, 2000.0, 0.5022399203583873  ),
    ])
def test_delta_premium_pct (transactions, 
                            current_premium,
                            expected):
    
    result = delta_premium_pct (transactions,
                                current_premium)

    assert result == expected
    
    
@pytest.mark.parametrize("delta, selected_transaction_size, side, expected", [
    (-100, -20.0, "sell", -120 ),
    (-100, 20.0, "sell", -120 ),
    (-100, 20.0, "buy", -80 ),
    (-100, -20.0, "buy", -80 ),
    (100, -20.0, "buy", 120 ),
    (100, -20.0, "sell", 80 ),
    ])
def test_proforma_delta (delta, 
                         selected_transaction_size,
                         side,
                         expected):
    
    result = proforma_delta (delta,
                             selected_transaction_size,
                             side)

    assert result == expected
    
@pytest.mark.parametrize("delta, selected_transaction_size, side, expected", [
    (-100, -20.0, "sell", False ),
    (-100, 20.0, "buy", True ),
    (100, -20.0, "buy", False ),
    (100, -20.0, "sell", True ),
    ])
def test_is_new_transaction_will_reduce_delta (delta, 
                         selected_transaction_size,
                         side,
                         expected):
    
    result = is_new_transaction_will_reduce_delta (delta,
                             selected_transaction_size,
                             side)

    assert result == expected
    
    
@pytest.mark.parametrize("notional, target_profit, average_movement, basic_ticks_for_average_meovement, expected", [
    ( 400,50/100, .15/100, 15,  46.29629629629629 ),
    ( 400,50/100, .05/100, 5,   46.2962962962963 ),
    ])
def test_get_basic_opening_size (
    notional, 
    target_profit,
    average_movement,
    basic_ticks_for_average_meovement,
    expected
    ):
    
    result = get_basic_opening_size (
        notional, 
        basic_ticks_for_average_meovement,
        average_movement,
        target_profit)
    

    assert result == expected
    
    
@pytest.mark.parametrize("instrument_name, instrument_attributes_futures, notional, target_profit, average_movement, basic_ticks_for_average_meovement, expected", [
    ("BTC-PERPETUAL", 400,50/100, .15/100, 15,  46.29629629629629 ),
    ])
def test_determine_opening_size (
    instrument_name,
    instrument_attributes_futures,
    notional, 
    target_profit,
    average_movement,
    basic_ticks_for_average_meovement,
    expected
    ):
    
    result = determine_opening_size (
        instrument_name,
        instrument_attributes_futures,
        notional, 
        basic_ticks_for_average_meovement,
        average_movement,
        target_profit)
    

    assert result == expected
    