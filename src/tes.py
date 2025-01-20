from functools import reduce

data = [1, 2, 3, 4, 5]
product = reduce(lambda x, y: x * y, data)  # Calculates

print(product)
result = ['CRV|2|1.45238055|7.7%|0.99811558|3.4%|0.49015232|21:56:38 01/20/25', 627]

rows = [str((i.replace('\n' and '%', ''))).split("|") for i in result[:-1]]
print (rows)
print (result[1])

headers = [
    "coin",
    "pings",
    "net_vol_btc",
    "net_vol_pct",
    "recent_total_vol_btc",
    "recent_vol_pct",
    "recent_net_vol",
    "datetime",#  (UTC)
]
data_all = [dict(zip(headers, l)) for l in rows]
print (data_all)
