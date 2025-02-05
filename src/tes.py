from functools import reduce
from loguru import logger as log

data = [1, 2, 3, 4, 5]
product = reduce(lambda x, y: x * y, data)  # Calculates

print(product)
dt = []
for dat in data:
    dt.append(str(dat))

    print(dt)

print(dt)

candles_data = [
    [
        {
            "tick": 1738695600000,
            "open": 99480.0,
            "high": 100867.0,
            "low": 98900.0,
            "close": 98959.5,
        },
        {
            "tick": 1738699200000,
            "open": 98942.5,
            "high": 99500.0,
            "low": 98017.0,
            "close": 98675.0,
        },
        {
            "tick": 1738702800000,
            "open": 98699.5,
            "high": 98729.5,
            "low": 96494.0,
            "close": 96569.0,
        },
        {
            "tick": 1738706400000,
            "open": 96568.5,
            "high": 98055.5,
            "low": 96186.0,
            "close": 97767.5,
        },
        {
            "tick": 1738710000000,
            "open": 97780.0,
            "high": 98260.5,
            "low": 97546.0,
            "close": 97832.0,
        },
        {
            "tick": 1738713600000,
            "open": 97832.5,
            "high": 98245.5,
            "low": 97781.5,
            "close": 98068.5,
        },
    ]
]
log.info(candles_data[0])
