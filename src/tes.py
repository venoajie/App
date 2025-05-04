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


result = {}
result.update({"params": {}})
result.update({"method": "subscription"})
log.info(result)

result["params"].update({"data": None})
result["params"].update({"channel": None})

log.info(result)


my_dict = {
    "usdc": {
        "balance": 0.342129,
        "currency": "usdc",
        "locked_balance": 0.0,
        "margin_balance": 6.8557,
        "equity": 0.342129,
        "maintenance_margin": 1.869204,
        "initial_margin": 2.883011,
        "available_funds": 3.972689,
        "available_withdrawal_funds": 0.341465,
        "spot_reserve": 0.0,
        "additional_reserve": 0.0,
    },
    "usdt": {
        "balance": 0.0,
        "currency": "usdt",
        "locked_balance": 0.0,
        "margin_balance": 6.855014,
        "equity": 0.0,
        "maintenance_margin": 1.869017,
        "initial_margin": 2.882723,
        "available_funds": 3.972292,
        "available_withdrawal_funds": 0.0,
        "spot_reserve": 0.0,
        "additional_reserve": 0.0,
    },
    "eurr": {
        "balance": 0.0,
        "currency": "eurr",
        "locked_balance": 0.0,
        "margin_balance": 0.0,
        "equity": 0.0,
        "maintenance_margin": 0.0,
        "initial_margin": 0.0,
        "available_funds": 0.0,
        "available_withdrawal_funds": 0.0,
        "spot_reserve": 0.0,
        "additional_reserve": 0.0,
    },
    "matic": {
        "balance": 0.0,
        "currency": "matic",
        "locked_balance": 0.0,
        "margin_balance": 0.0,
        "equity": 0.0,
        "maintenance_margin": 0.0,
        "initial_margin": 0.0,
        "available_funds": 0.0,
        "available_withdrawal_funds": 0.0,
        "spot_reserve": 0.0,
        "additional_reserve": 0.0,
    },
    "steth": {
        "balance": 0.0,
        "currency": "steth",
        "locked_balance": 0.0,
        "margin_balance": 0.003674,
        "equity": 0.0,
        "maintenance_margin": 0.001002,
        "initial_margin": 0.001545,
        "available_funds": 0.002129,
        "available_withdrawal_funds": 0.0,
        "spot_reserve": 0.0,
        "additional_reserve": 0.0,
    },
    "usyc": {
        "balance": 0.0,
        "currency": "usyc",
        "locked_balance": 0.0,
        "margin_balance": 6.336029,
        "equity": 0.0,
        "maintenance_margin": 1.727516,
        "initial_margin": 2.664475,
        "available_funds": 3.671554,
        "available_withdrawal_funds": 0.0,
        "spot_reserve": 0.0,
        "additional_reserve": 0.0,
    },
    "paxg": {
        "balance": 0.0,
        "currency": "paxg",
        "locked_balance": 0.0,
        "margin_balance": 0.002176,
        "equity": 0.0,
        "maintenance_margin": 0.000593,
        "initial_margin": 0.000915,
        "available_funds": 0.001261,
        "available_withdrawal_funds": 0.0,
        "spot_reserve": 0.0,
        "additional_reserve": 0.0,
    },
    "usde": {
        "balance": 0.0,
        "currency": "usde",
        "locked_balance": 0.0,
        "margin_balance": 6.8557,
        "equity": 0.0,
        "maintenance_margin": 1.869204,
        "initial_margin": 2.883011,
        "available_funds": 3.972689,
        "available_withdrawal_funds": 0.0,
        "spot_reserve": 0.0,
        "additional_reserve": 0.0,
    },
    "buidl": {
        "balance": 0.0,
        "currency": "buidl",
        "locked_balance": 0.0,
        "margin_balance": 6.854329,
        "equity": 0.0,
        "maintenance_margin": 1.86883,
        "initial_margin": 2.882434,
        "available_funds": 3.971895,
        "available_withdrawal_funds": 0.0,
        "spot_reserve": 0.0,
        "additional_reserve": 0.0,
    },
    "bnb": {
        "balance": 0.0,
        "currency": "bnb",
        "locked_balance": 0.0,
        "margin_balance": 0.0,
        "equity": 0.0,
        "maintenance_margin": 0.0,
        "initial_margin": 0.0,
        "available_funds": 0.0,
        "available_withdrawal_funds": 0.0,
        "spot_reserve": 0.0,
        "additional_reserve": 0.0,
    },
    "btc": {
        "balance": 6.694e-05,
        "currency": "btc",
        "locked_balance": 0.0,
        "margin_balance": 8.165e-05,
        "equity": 6.529e-05,
        "maintenance_margin": 2.226e-05,
        "initial_margin": 3.434e-05,
        "available_funds": 4.732e-05,
        "available_withdrawal_funds": 0.0,
        "spot_reserve": 0.0,
        "additional_reserve": 0.0,
    },
    "eth": {
        "balance": 0.00055,
        "currency": "eth",
        "locked_balance": 0.0,
        "margin_balance": 0.003674,
        "equity": 0.000553,
        "maintenance_margin": 0.001002,
        "initial_margin": 0.001545,
        "available_funds": 0.002129,
        "available_withdrawal_funds": 0.000422,
        "spot_reserve": 0.0,
        "additional_reserve": 0.0,
    },
    "sol": {
        "balance": 0.0,
        "currency": "sol",
        "locked_balance": 0.0,
        "margin_balance": 0.05367016,
        "equity": 0.0,
        "maintenance_margin": 0.01463315,
        "initial_margin": 0.02256978,
        "available_funds": 0.03110038,
        "available_withdrawal_funds": 0.0,
        "spot_reserve": 0.0,
        "additional_reserve": 0.0,
    },
    "xrp": {
        "balance": 0.0,
        "currency": "xrp",
        "locked_balance": 0.0,
        "margin_balance": 0.0,
        "equity": 0.0,
        "maintenance_margin": 0.0,
        "initial_margin": 0.0,
        "available_funds": 0.0,
        "available_withdrawal_funds": 0.0,
        "spot_reserve": 0.0,
        "additional_reserve": 0.0,
    },
    "ethw": {
        "balance": 0.0,
        "currency": "ethw",
        "locked_balance": 0.0,
        "margin_balance": 0.0,
        "equity": 0.0,
        "maintenance_margin": 0.0,
        "initial_margin": 0.0,
        "available_funds": 0.0,
        "available_withdrawal_funds": 0.0,
        "spot_reserve": 0.0,
        "additional_reserve": 0.0,
    },
}

log.error(my_dict.values())

def printTable(table,align="",hasHeader=False,pad=2,isGrid=False):
    table = [row[:] for row in table] # copy table
    numRows,numCols = len(table),len(table[0]) # table size
    align = align.ljust(numCols,"L") # align left by default
    align = ["RC".find(c)+1 for c in align] # convert to index (?RC=012)
    widths = [max(len(row[col]) for row in table) for col in range(numCols)] # column widths

    # --- apply column widths with alignments ---
    if hasHeader: # header is centered
        for x in range(numCols): table[0][x] = table[0][x].center(widths[x])
    for y in range(hasHeader,numRows): # apply column alignments
        for x in range(numCols): c = table[y][x]; table[y][x] = [c.ljust,c.rjust,c.center][align[x]](widths[x])

    # --- data for printing
    P = " "*pad; LSEP,SEP,RSEP = "│"+P, P+"│"+P, P+"│"
    lines = ["─"*(widths[col]+pad*2) for col in range(numCols)]

    drawLine = [isGrid]*numRows; drawLine[0]|=hasHeader; drawLine[-1] = False
    if hasHeader or isGrid: gridLine = "├"+"┼".join(lines)+"┤" # if any(drawLine)

    # --- print rows ---
    print("┌"+"┬".join(lines)+"┐")
    for y in range(numRows):
        print(LSEP+SEP.join(table[y])+RSEP)
        if drawLine[y]: print(gridLine)
    print("└"+"┴".join(lines)+"┘")
    
table = [
    ['Fruit',      'Animals',      'Numbers',    'Characters'],
    ['Apple',      'Cat',          '123,456',    'Pac-Man'],
    ['Banana',     'Dog',          '69',         'Donkey Kong'],
    ['Mango',      'Horse',        '210',        'Bomberman'],
    ['Pear',       'Lion',         '84,763,596', 'Pikachu'],
    ['Watermelon', 'Elephant',     '1,000',      'Mario'],
    ['Orange',     'Hippopotamus', '263',        'Ezio Auditore da Firenze'],
    ['Cranberry',  'Hedgehog',     '2,495',      'Sonic']
]

printTable(table,align="RLRC",hasHeader=True)