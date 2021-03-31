import requests

# This script builds the analysis dataset from cryptodatadownload.com's historical Binance and Gemini Exchange data.

coin_list = (
    "BTC", "ETH", "LTC", "NEO", "BNB", "XRP", "LINK", "EOS", "TRX",
    "ETC", "XLM", "ZEC", "ADA", "QTUM", "DASH", "XMR", "BTT"
)

# Create the binance_multicoin_dataset.csv file.
bds = open("binance_multicoin_dataset.csv", 'wb')

# Writing the column headers for the dataset as the first line of the CSV
bds.write(bytes("unix,date,symbol,open,high,low,close,Volume BTC,Volume USDT,tradecount", 'utf-8'))

for coin in coin_list:
    # Building the request string to pull in each coin's csv data file.
    request_str = f"http://www.cryptodatadownload.com/cdd/Binance_{coin}USDT_minute.csv"
    r = requests.get(request_str)
    coin_csv = r.content
    bds.write(coin_csv)

bds.close()


# Create the gemini_long_history_dataset.csv file.
gds = open("gemini_long_history_dataset.csv", "wb")

# Writing the column headers as the first line of the CSV
gds.write(bytes("Unix Timestamp,Date,Symbol,Open,High,Low,Close,Volume", 'utf-8'))

# BTC data is available for 2015-2020
for year in range(2015, 2021):
    request_str = f"http://www.cryptodatadownload.com/cdd/gemini_BTCUSD_{year}_1min.csv"
    r = requests.get(request_str)
    coin_csv = r.content
    gds.write(coin_csv)

# ETH data is available for 2016-2020
for year in range(2016, 2021):
    request_str = f"http://www.cryptodatadownload.com/cdd/gemini_ETHUSD_{year}_1min.csv"
    r = requests.get(request_str)
    coin_csv = r.content
    gds.write(coin_csv)

# LTC and ZEC data is available for 2018-2020
for year in range(2018, 2021):
    request_str = f"http://www.cryptodatadownload.com/cdd/gemini_LTCUSD_{year}_1min.csv"
    r = requests.get(request_str)
    coin_csv = r.content
    gds.write(coin_csv)

for year in range(2018, 2021):
    request_str = f"http://www.cryptodatadownload.com/cdd/gemini_ZECUSD_{year}_1min.csv"
    r = requests.get(request_str)
    coin_csv = r.content
    gds.write(coin_csv)

gds.close()
