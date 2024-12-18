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


instrument_attributes_futures_all = [{'price_index': 'btc_usd', 'rfq': False, 'kind': 'future', 'instrument_name': 'BTC-20DEC24', 'is_active': True, 'maker_commission': -0.0001, 'taker_commission': 0.0005, 'instrument_type': 'reversed', 'expiration_timestamp': 1734681600000, 'contract_size': 10.0, 'tick_size': 2.5, 'block_trade_tick_size': 0.01, 'base_currency': 'BTC', 'instrument_id': 405856, 'creation_timestamp': 1733472013000, 'settlement_period': 'week', 'future_type': 'reversed', 'max_leverage': 25, 'max_liquidation_commission': 0.0075, 'block_trade_commission': 0.00025, 'block_trade_min_trade_amount': 200000, 'settlement_currency': 'BTC', 'counter_currency': 'USD', 'min_trade_amount': 10.0, 'quote_currency': 'USD', 'tick_size_steps': []}, {'price_index': 'btc_usd', 'rfq': False, 'kind': 'future', 'instrument_name': 'BTC-27DEC24', 'is_active': True, 'maker_commission': 0.0, 'taker_commission': 0.0005, 'instrument_type': 'reversed', 'expiration_timestamp': 1735286400000, 'contract_size': 10.0, 'tick_size': 2.5, 'block_trade_tick_size': 0.01, 'base_currency': 'BTC', 'instrument_id': 285303, 'creation_timestamp': 1703836813000, 'settlement_period': 'month', 'future_type': 'reversed', 'max_leverage': 25, 'max_liquidation_commission': 0.0075, 'block_trade_commission': 0.00025, 'block_trade_min_trade_amount': 200000, 'settlement_currency': 'BTC', 'counter_currency': 'USD', 'min_trade_amount': 10.0, 'quote_currency': 'USD', 'tick_size_steps': []}, {'price_index': 'btc_usd', 'rfq': False, 'kind': 'future', 'instrument_name': 'BTC-31JAN25', 'is_active': True, 'maker_commission': 0.0, 'taker_commission': 0.0005, 'instrument_type': 'reversed', 'expiration_timestamp': 1738310400000, 'contract_size': 10.0, 'tick_size': 2.5, 'block_trade_tick_size': 0.01, 'base_currency': 'BTC', 'instrument_id': 399998, 'creation_timestamp': 1732867218000, 'settlement_period': 'month', 'future_type': 'reversed', 'max_leverage': 25, 'max_liquidation_commission': 0.0075, 'block_trade_commission': 0.00025, 'block_trade_min_trade_amount': 200000, 'settlement_currency': 'BTC', 'counter_currency': 'USD', 'min_trade_amount': 10.0, 'quote_currency': 'USD', 'tick_size_steps': []}, {'price_index': 'btc_usd', 'rfq': False, 'kind': 'future', 'instrument_name': 'BTC-28MAR25', 'is_active': True, 'maker_commission': 0.0, 'taker_commission': 0.0005, 'instrument_type': 'reversed', 'expiration_timestamp': 1743148800000, 'contract_size': 10.0, 'tick_size': 2.5, 'block_trade_tick_size': 0.01, 'base_currency': 'BTC', 'instrument_id': 309709, 'creation_timestamp': 1711699222000, 'settlement_period': 'month', 'future_type': 'reversed', 'max_leverage': 25, 'max_liquidation_commission': 0.0075, 'block_trade_commission': 0.00025, 'block_trade_min_trade_amount': 200000, 'settlement_currency': 'BTC', 'counter_currency': 'USD', 'min_trade_amount': 10.0, 'quote_currency': 'USD', 'tick_size_steps': []}, {'price_index': 'btc_usd', 'rfq': False, 'kind': 'future', 'instrument_name': 'BTC-27JUN25', 'is_active': True, 'maker_commission': 0.0, 'taker_commission': 0.0005, 'instrument_type': 'reversed', 'expiration_timestamp': 1751011200000, 'contract_size': 10.0, 'tick_size': 2.5, 'block_trade_tick_size': 0.01, 'base_currency': 'BTC', 'instrument_id': 341755, 'creation_timestamp': 1719561614000, 'settlement_period': 'month', 'future_type': 'reversed', 'max_leverage': 25, 'max_liquidation_commission': 0.0075, 'block_trade_commission': 0.00025, 'block_trade_min_trade_amount': 200000, 'settlement_currency': 'BTC', 'counter_currency': 'USD', 'min_trade_amount': 10.0, 'quote_currency': 'USD', 'tick_size_steps': []}, {'price_index': 'btc_usd', 'rfq': False, 'kind': 'future', 'instrument_name': 'BTC-26SEP25', 'is_active': True, 'maker_commission': 0.0, 'taker_commission': 0.0005, 'instrument_type': 'reversed', 'expiration_timestamp': 1758873600000, 'contract_size': 10.0, 'tick_size': 2.5, 'block_trade_tick_size': 0.01, 'base_currency': 'BTC', 'instrument_id': 373248, 'creation_timestamp': 1727424018000, 'settlement_period': 'month', 'future_type': 'reversed', 'max_leverage': 25, 'max_liquidation_commission': 0.0075, 'block_trade_commission': 0.00025, 'block_trade_min_trade_amount': 200000, 'settlement_currency': 'BTC', 'counter_currency': 'USD', 'min_trade_amount': 10.0, 'quote_currency': 'USD', 'tick_size_steps': []}, {'price_index': 'btc_usd', 'rfq': False, 'kind': 'future', 'instrument_name': 'BTC-PERPETUAL', 'is_active': True, 'maker_commission': 0.0, 'taker_commission': 0.0005, 'instrument_type': 'reversed', 'expiration_timestamp': 32503708800000, 'contract_size': 10.0, 'tick_size': 0.5, 'block_trade_tick_size': 0.01, 'base_currency': 'BTC', 'instrument_id': 210838, 'creation_timestamp': 1534242287000, 'settlement_period': 'perpetual', 'future_type': 'reversed', 'max_leverage': 50, 'max_liquidation_commission': 0.0075, 'block_trade_commission': 0.00025, 'block_trade_min_trade_amount': 200000, 'settlement_currency': 'BTC', 'counter_currency': 'USD', 'min_trade_amount': 10.0, 'quote_currency': 'USD', 'tick_size_steps': []}, {'price_index': 'btc_usd', 'rfq': False, 'kind': 'future_combo', 'instrument_name': 'BTC-FS-20DEC24_PERP', 'is_active': True, 'maker_commission': 0.0, 'taker_commission': 0.0, 'instrument_type': 'reversed', 'expiration_timestamp': 1734681600000, 'contract_size': 10.0, 'tick_size': 0.5, 'base_currency': 'BTC', 'instrument_id': 406191, 'creation_timestamp': 1733474395000, 'settlement_period': 'week', 'settlement_currency': 'BTC', 'counter_currency': 'USD', 'min_trade_amount': 10.0, 'quote_currency': 'USD', 'tick_size_steps': []}, {'price_index': 'btc_usd', 'rfq': False, 'kind': 'future_combo', 'instrument_name': 'BTC-FS-27DEC24_PERP', 'is_active': True, 'maker_commission': 0.0, 'taker_commission': 0.0, 'instrument_type': 'reversed', 'expiration_timestamp': 1735286400000, 'contract_size': 10.0, 'tick_size': 0.5, 'base_currency': 'BTC', 'instrument_id': 285456, 'creation_timestamp': 1703839739000, 'settlement_period': 'month', 'settlement_currency': 'BTC', 'counter_currency': 'USD', 'min_trade_amount': 10.0, 'quote_currency': 'USD', 'tick_size_steps': []}, {'price_index': 'btc_usd', 'rfq': False, 'kind': 'future_combo', 'instrument_name': 'BTC-FS-31JAN25_PERP', 'is_active': True, 'maker_commission': 0.0, 'taker_commission': 0.0, 'instrument_type': 'reversed', 'expiration_timestamp': 1738310400000, 'contract_size': 10.0, 'tick_size': 0.5, 'base_currency': 'BTC', 'instrument_id': 400420, 'creation_timestamp': 1732869597000, 'settlement_period': 'month', 'settlement_currency': 'BTC', 'counter_currency': 'USD', 'min_trade_amount': 10.0, 'quote_currency': 'USD', 'tick_size_steps': []}, {'price_index': 'btc_usd', 'rfq': False, 'kind': 'future_combo', 'instrument_name': 'BTC-FS-28MAR25_PERP', 'is_active': True, 'maker_commission': 0.0, 'taker_commission': 0.0, 'instrument_type': 'reversed', 'expiration_timestamp': 1743148800000, 'contract_size': 10.0, 'tick_size': 0.5, 'base_currency': 'BTC', 'instrument_id': 310054, 'creation_timestamp': 1711702139000, 'settlement_period': 'month', 'settlement_currency': 'BTC', 'counter_currency': 'USD', 'min_trade_amount': 10.0, 'quote_currency': 'USD', 'tick_size_steps': []}, {'price_index': 'btc_usd', 'rfq': False, 'kind': 'future_combo', 'instrument_name': 'BTC-FS-27JUN25_PERP', 'is_active': True, 'maker_commission': 0.0, 'taker_commission': 0.0, 'instrument_type': 'reversed', 'expiration_timestamp': 1751011200000, 'contract_size': 10.0, 'tick_size': 0.5, 'base_currency': 'BTC', 'instrument_id': 342023, 'creation_timestamp': 1719564001000, 'settlement_period': 'month', 'settlement_currency': 'BTC', 'counter_currency': 'USD', 'min_trade_amount': 10.0, 'quote_currency': 'USD', 'tick_size_steps': []}, {'price_index': 'btc_usd', 'rfq': False, 'kind': 'future_combo', 'instrument_name': 'BTC-FS-26SEP25_PERP', 'is_active': True, 'maker_commission': 0.0, 'taker_commission': 0.0, 'instrument_type': 'reversed', 'expiration_timestamp': 1758873600000, 'contract_size': 10.0, 'tick_size': 0.5, 'base_currency': 'BTC', 'instrument_id': 373281, 'creation_timestamp': 1727426399000, 'settlement_period': 'month', 'settlement_currency': 'BTC', 'counter_currency': 'USD', 'min_trade_amount': 10.0, 'quote_currency': 'USD', 'tick_size_steps': []}, {'price_index': 'eth_usd', 'rfq': False, 'kind': 'future', 'instrument_name': 'ETH-20DEC24', 'is_active': True, 'maker_commission': -0.0001, 'taker_commission': 0.0005, 'instrument_type': 'reversed', 'expiration_timestamp': 1734681600000, 'contract_size': 1, 'tick_size': 0.25, 'block_trade_tick_size': 0.01, 'base_currency': 'ETH', 'instrument_id': 405857, 'creation_timestamp': 1733472014000, 'settlement_period': 'week', 'future_type': 'reversed', 'max_leverage': 25, 'max_liquidation_commission': 0.009, 'block_trade_commission': 0.00025, 'block_trade_min_trade_amount': 100000, 'settlement_currency': 'ETH', 'counter_currency': 'USD', 'min_trade_amount': 1, 'quote_currency': 'USD', 'tick_size_steps': []}, {'price_index': 'eth_usd', 'rfq': False, 'kind': 'future', 'instrument_name': 'ETH-27DEC24', 'is_active': True, 'maker_commission': 0.0, 'taker_commission': 0.0005, 'instrument_type': 'reversed', 'expiration_timestamp': 1735286400000, 'contract_size': 1, 'tick_size': 0.25, 'block_trade_tick_size': 0.01, 'base_currency': 'ETH', 'instrument_id': 285302, 'creation_timestamp': 1703836806000, 'settlement_period': 'month', 'future_type': 'reversed', 'max_leverage': 25, 'max_liquidation_commission': 0.009, 'block_trade_commission': 0.00025, 'block_trade_min_trade_amount': 100000, 'settlement_currency': 'ETH', 'counter_currency': 'USD', 'min_trade_amount': 1, 'quote_currency': 'USD', 'tick_size_steps': []}, {'price_index': 'eth_usd', 'rfq': False, 'kind': 'future', 'instrument_name': 'ETH-31JAN25', 'is_active': True, 'maker_commission': 0.0, 'taker_commission': 0.0005, 'instrument_type': 'reversed', 'expiration_timestamp': 1738310400000, 'contract_size': 1, 'tick_size': 0.25, 'block_trade_tick_size': 0.01, 'base_currency': 'ETH', 'instrument_id': 399996, 'creation_timestamp': 1732867217000, 'settlement_period': 'month', 'future_type': 'reversed', 'max_leverage': 25, 'max_liquidation_commission': 0.009, 'block_trade_commission': 0.00025, 'block_trade_min_trade_amount': 100000, 'settlement_currency': 'ETH', 'counter_currency': 'USD', 'min_trade_amount': 1, 'quote_currency': 'USD', 'tick_size_steps': []}, {'price_index': 'eth_usd', 'rfq': False, 'kind': 'future', 'instrument_name': 'ETH-28MAR25', 'is_active': True, 'maker_commission': 0.0, 'taker_commission': 0.0005, 'instrument_type': 'reversed', 'expiration_timestamp': 1743148800000, 'contract_size': 1, 'tick_size': 0.25, 'block_trade_tick_size': 0.01, 'base_currency': 'ETH', 'instrument_id': 309707, 'creation_timestamp': 1711699210000, 'settlement_period': 'month', 'future_type': 'reversed', 'max_leverage': 25, 'max_liquidation_commission': 0.009, 'block_trade_commission': 0.00025, 'block_trade_min_trade_amount': 100000, 'settlement_currency': 'ETH', 'counter_currency': 'USD', 'min_trade_amount': 1, 'quote_currency': 'USD', 'tick_size_steps': []}, {'price_index': 'eth_usd', 'rfq': False, 'kind': 'future', 'instrument_name': 'ETH-27JUN25', 'is_active': True, 'maker_commission': 0.0, 'taker_commission': 0.0005, 'instrument_type': 'reversed', 'expiration_timestamp': 1751011200000, 'contract_size': 1, 'tick_size': 0.25, 'block_trade_tick_size': 0.01, 'base_currency': 'ETH', 'instrument_id': 341752, 'creation_timestamp': 1719561606000, 'settlement_period': 'month', 'future_type': 'reversed', 'max_leverage': 25, 'max_liquidation_commission': 0.009, 'block_trade_commission': 0.00025, 'block_trade_min_trade_amount': 100000, 'settlement_currency': 'ETH', 'counter_currency': 'USD', 'min_trade_amount': 1, 'quote_currency': 'USD', 'tick_size_steps': []}, {'price_index': 'eth_usd', 'rfq': False, 'kind': 'future', 'instrument_name': 'ETH-26SEP25', 'is_active': True, 'maker_commission': 0.0, 'taker_commission': 0.0005, 'instrument_type': 'reversed', 'expiration_timestamp': 1758873600000, 'contract_size': 1, 'tick_size': 0.25, 'block_trade_tick_size': 0.01, 'base_currency': 'ETH', 'instrument_id': 373133, 'creation_timestamp': 1727424017000, 'settlement_period': 'month', 'future_type': 'reversed', 'max_leverage': 25, 'max_liquidation_commission': 0.009, 'block_trade_commission': 0.00025, 'block_trade_min_trade_amount': 100000, 'settlement_currency': 'ETH', 'counter_currency': 'USD', 'min_trade_amount': 1, 'quote_currency': 'USD', 'tick_size_steps': []}, {'price_index': 'eth_usd', 'rfq': False, 'kind': 'future', 'instrument_name': 'ETH-PERPETUAL', 'is_active': True, 'maker_commission': 0.0, 'taker_commission': 0.0005, 'instrument_type': 'reversed', 'expiration_timestamp': 32503708800000, 'contract_size': 1, 'tick_size': 0.05, 'block_trade_tick_size': 0.01, 'base_currency': 'ETH', 'instrument_id': 210760, 'creation_timestamp': 1552568454000, 'settlement_period': 'perpetual', 'future_type': 'reversed', 'max_leverage': 50, 'max_liquidation_commission': 0.009, 'block_trade_commission': 0.00025, 'block_trade_min_trade_amount': 100000, 'settlement_currency': 'ETH', 'counter_currency': 'USD', 'min_trade_amount': 1, 'quote_currency': 'USD', 'tick_size_steps': []}, {'price_index': 'eth_usd', 'rfq': False, 'kind': 'future_combo', 'instrument_name': 'ETH-FS-20DEC24_PERP', 'is_active': True, 'maker_commission': 0.0, 'taker_commission': 0.0, 'instrument_type': 'reversed', 'expiration_timestamp': 1734681600000, 'contract_size': 1, 'tick_size': 0.05, 'base_currency': 'ETH', 'instrument_id': 406198, 'creation_timestamp': 1733474398000, 'settlement_period': 'week', 'settlement_currency': 'ETH', 'counter_currency': 'USD', 'min_trade_amount': 1, 'quote_currency': 'USD', 'tick_size_steps': []}, {'price_index': 'eth_usd', 'rfq': False, 'kind': 'future_combo', 'instrument_name': 'ETH-FS-27DEC24_PERP', 'is_active': True, 'maker_commission': 0.0, 'taker_commission': 0.0, 'instrument_type': 'reversed', 'expiration_timestamp': 1735286400000, 'contract_size': 1, 'tick_size': 0.05, 'base_currency': 'ETH', 'instrument_id': 285477, 'creation_timestamp': 1703839746000, 'settlement_period': 'month', 'settlement_currency': 'ETH', 'counter_currency': 'USD', 'min_trade_amount': 1, 'quote_currency': 'USD', 'tick_size_steps': []}, {'price_index': 'eth_usd', 'rfq': False, 'kind': 'future_combo', 'instrument_name': 'ETH-FS-31JAN25_PERP', 'is_active': True, 'maker_commission': 0.0, 'taker_commission': 0.0, 'instrument_type': 'reversed', 'expiration_timestamp': 1738310400000, 'contract_size': 1, 'tick_size': 0.05, 'base_currency': 'ETH', 'instrument_id': 400433, 'creation_timestamp': 1732869601000, 'settlement_period': 'month', 'settlement_currency': 'ETH', 'counter_currency': 'USD', 'min_trade_amount': 1, 'quote_currency': 'USD', 'tick_size_steps': []}, {'price_index': 'eth_usd', 'rfq': False, 'kind': 'future_combo', 'instrument_name': 'ETH-FS-28MAR25_PERP', 'is_active': True, 'maker_commission': 0.0, 'taker_commission': 0.0, 'instrument_type': 'reversed', 'expiration_timestamp': 1743148800000, 'contract_size': 1, 'tick_size': 0.05, 'base_currency': 'ETH', 'instrument_id': 310075, 'creation_timestamp': 1711702145000, 'settlement_period': 'month', 'settlement_currency': 'ETH', 'counter_currency': 'USD', 'min_trade_amount': 1, 'quote_currency': 'USD', 'tick_size_steps': []}, {'price_index': 'eth_usd', 'rfq': False, 'kind': 'future_combo', 'instrument_name': 'ETH-FS-27JUN25_PERP', 'is_active': True, 'maker_commission': 0.0, 'taker_commission': 0.0, 'instrument_type': 'reversed', 'expiration_timestamp': 1751011200000, 'contract_size': 1, 'tick_size': 0.05, 'base_currency': 'ETH', 'instrument_id': 342044, 'creation_timestamp': 1719564009000, 'settlement_period': 'month', 'settlement_currency': 'ETH', 'counter_currency': 'USD', 'min_trade_amount': 1, 'quote_currency': 'USD', 'tick_size_steps': []}, {'price_index': 'eth_usd', 'rfq': False, 'kind': 'future_combo', 'instrument_name': 'ETH-FS-26SEP25_PERP', 'is_active': True, 'maker_commission': 0.0, 'taker_commission': 0.0, 'instrument_type': 'reversed', 'expiration_timestamp': 1758873600000, 'contract_size': 1, 'tick_size': 0.05, 'base_currency': 'ETH', 'instrument_id': 373302, 'creation_timestamp': 1727426406000, 'settlement_period': 'month', 'settlement_currency': 'ETH', 'counter_currency': 'USD', 'min_trade_amount': 1, 'quote_currency': 'USD', 'tick_size_steps': []}]

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
    ("BTC-PERPETUAL", instrument_attributes_futures_all, 400,50/100, .15/100, 15,  46.29629629629629 ),
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
    