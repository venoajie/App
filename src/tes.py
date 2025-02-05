from functools import reduce

data = [1, 2, 3, 4, 5]
product = reduce(lambda x, y: x * y, data)  # Calculates

print(product)
dt = []
for dat in data:
    dt.append(str(dat))

    print(dt)

print(dt)

candles_data = [
    {
        'instrument_name': 'BTC-PERPETUAL', 
        'resolution': 60, 
        'ohlc': [
            {
                'tick': 1738692000000, 
                'open': 99270.0, 
                'high': 99649.5, 
                'low': 98868.0, 
                'close': 99480.5
                },
            {
                'tick': 1738695600000, 
                'open': 99480.0,
                'high': 100867.0, 
                'low': 98900.0, 
                'close': 98959.5
                },
            {'tick': 1738699200000, 
             'open': 98942.5, 
             'high': 99500.0, 
             'low': 98017.0, 
             'close': 98675.0
             }, {'tick': 1738702800000, 'open': 98699.5, 'high': 98729.5, 'low': 96494.0, 'close': 96569.0}, {'tick': 1738706400000, 'open': 96568.5, 'high': 98055.5, 'low': 96186.0, 'close': 97767.5}, {'tick': 1738710000000, 'open': 97780.0, 'high': 98049.5, 'low': 97546.0, 'close': 97896.5}]}, {'instrument_name': 'BTC-PERPETUAL', 'resolution': 15, 'ohlc': [{'tick': 1738708200000, 'open': 97007.0, 'high': 97350.0, 'low': 96828.5, 'close': 97301.0}, {'tick': 1738709100000, 'open': 97301.0, 'high': 98055.5, 'low': 97115.5, 'close': 97767.5}, {'tick': 1738710000000, 'open': 97780.0, 'high': 98049.5, 'low': 97638.5, 'close': 97960.5}, {'tick': 1738710900000, 'open': 97955.5, 'high': 97970.0, 'low': 97567.5, 'close': 97599.5}, {'tick': 1738711800000, 'open': 97600.0, 'high': 97881.5, 'low': 97546.0, 'close': 97799.5}, {'tick': 1738712700000, 'open': 97776.0, 'high': 97907.0, 'low': 97776.0, 'close': 97896.5}]}, {'instrument_name': 'BTC-PERPETUAL', 'resolution': 5, 'ohlc': [{'tick': 1738711200000, 'open': 97711.0, 'high': 97970.0, 'low': 97700.0, 'close': 97900.0}, {'tick': 1738711500000, 'open': 97800.0, 'high': 97800.0, 'low': 97567.5, 'close': 97599.5}, {'tick': 1738711800000, 'open': 97600.0, 'high': 97791.5, 'low': 97546.0, 'close': 97755.0}, {'tick': 1738712100000, 'open': 97811.0, 'high': 97881.5, 'low': 97794.5, 'close': 97799.0}, {'tick': 1738712400000, 'open': 97846.5, 'high': 97846.5, 'low': 97731.0, 'close': 97799.5}, {'tick': 1738712700000, 'open': 97776.0, 'high': 97907.0, 'low': 97776.0, 'close': 97896.5}]}, {'instrument_name': 'ETH-PERPETUAL', 'resolution': 60, 'ohlc': [{'tick': 1738692000000, 'open': 2816.05, 'high': 2836.35, 'low': 2795.8, 'close': 2830.8}, {'tick': 1738695600000, 'open': 2830.2, 'high': 2869.95, 'low': 2771.6, 'close': 2771.6}, {'tick': 1738699200000, 'open': 2771.0, 'high': 2794.95, 'low': 2721.05, 'close': 2731.45}, {'tick': 1738702800000, 'open': 2732.85, 'high': 2733.9, 'low': 2636.95, 'close': 2639.5}, {'tick': 1738706400000, 'open': 2639.65, 'high': 2701.15, 'low': 2632.95, 'close': 2685.25}, {'tick': 1738710000000, 'open': 2685.25, 'high': 2735.1, 'low': 2677.55, 'close': 2726.1}]}, {'instrument_name': 'ETH-PERPETUAL', 'resolution': 15, 'ohlc': [{'tick': 1738708200000, 'open': 2668.65, 'high': 2677.9, 'low': 2658.55, 'close': 2674.15}, {'tick': 1738709100000, 'open': 2674.6, 'high': 2701.15, 'low': 2669.45, 'close': 2685.25}, {'tick': 1738710000000, 'open': 2685.25, 'high': 2735.1, 'low': 2677.55, 'close': 2722.45}, {'tick': 1738710900000, 'open': 2722.6, 'high': 2725.45, 'low': 2702.6, 'close': 2707.35}, {'tick': 1738711800000, 'open': 2707.3, 'high': 2725.25, 'low': 2703.5, 'close': 2724.3}, {'tick': 1738712700000, 'open': 2722.4, 'high': 2727.0, 'low': 2721.75, 'close': 2726.1}]}, {'instrument_name': 'ETH-PERPETUAL', 'resolution': 5, 'ohlc': [{'tick': 1738711200000, 'open': 2706.4, 'high': 2713.85, 'low': 2704.8, 'close': 2709.3}, {'tick': 1738711500000, 'open': 2708.35, 'high': 2709.05, 'low': 2702.6, 'close': 2707.35}, {'tick': 1738711800000, 'open': 2707.3, 'high': 2717.1, 'low': 2703.5, 'close': 2714.8}, {'tick': 1738712100000, 'open': 2714.95, 'high': 2724.5, 'low': 2714.95, 'close': 2721.25}, {'tick': 1738712400000, 'open': 2721.0, 'high': 2725.25, 'low': 2719.15, 'close': 2724.3}, {'tick': 1738712700000, 'open': 2722.4, 'high': 2727.0, 'low': 2721.75, 'close': 2726.1}]}]
print ([o for o in candles_data if o["instrument_name"] == "BTC-PERPETUAL"])