from rfnmarket import Market
from rfnmarket import log

tickers = Market(log_level=log.DEBUG)

ts_data = tickers.get_timeseries(['AAPL'], update=True)
symbols = tickers.get_us_symbols()

print(ts_data)
print(len(symbols))
