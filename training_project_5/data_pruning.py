
# This script helps prune the datasets of multiple non-data rows that occur during the gathering script
# (essentially each year/coin repeats the headers when it's pulled, and these rows need to be removed).

bds = open("binance_multicoin_dataset.csv", 'r')
lines = bds.readlines()
bds.close()

output = ""
for line in lines:
    if len(line) > 72:
        output += line

bds = open("binance_multicoin_dataset.csv", "w")
bds.write(output)
