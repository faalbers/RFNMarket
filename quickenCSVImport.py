import rfnmarket as rfnm
from pprint import pp
from datetime import datetime
import pandas as pd

if __name__ == '__main__':
    tickers = rfnm.Tickers()
    data = tickers.getQuickenInvestments()
    symbols = list((data.keys()))
    symbols.sort()
    data = tickers.getData(['price'], symbols, forceUpdate=True)
    dftn = pd.DataFrame(data['price']).T['price'].dropna()
    dftn.to_csv('Z:\\Quicken\\QuickenImport.csv', header=False, sep=',', encoding='utf-8')


