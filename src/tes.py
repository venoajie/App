from functools import reduce

data = [1, 2, 3, 4, 5]
product = reduce(lambda x, y: x * y, data)  # Calculates

print(product)
dt = []
for dat in data:
    dt.append(str(dat))

    print(dt)

print(dt)
