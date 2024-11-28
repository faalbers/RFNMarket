from rfnmarket import Tickers
from rfnmarket import log

tickers = Tickers(log_level=log.DEBUG)

ts_data = tickers.get_timeseries(['AAPL'], update=True)
symbols = tickers.get_us_symbols()

print(ts_data)
print(len(symbols))
