import rfnmarket as rfnm
from pprint import pp
from datetime import datetime
import pandas as pd

if __name__ == '__main__':
    tickers = rfnm.Tickers()
    data = tickers.getQuickenInvestments()
    symbols = list((data.keys()))
    symbols.sort()
    # data = tickers.getData(symbols, ['price'], forceUpdate=True)
    data = tickers.getData(['price'], symbols, forceUpdate=True)
    dftn = data['price']['price']['price']['price']
    
    # dftn['timestamp'] = pd.to_datetime(dftn['timestamp'], unit='s').dt.tz_localize('US/Pacific')
    # dftn['marketTime'] = pd.to_datetime(dftn['marketTime'], unit='s').dt.tz_localize('US/Pacific')

    dftn.dropna().to_csv('Z:\\Quicken\\QuickenImport.csv', sep=',', encoding='utf-8')


