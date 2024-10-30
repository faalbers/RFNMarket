from rfnmarket import Tickers, QPortfolio
from pprint import pp
from datetime import datetime
import pandas as pd

if __name__ == '__main__':
    tickers = Tickers()
    qpf = QPortfolio('database/2020.QIF')
    symbols = qpf.getSecurityShares()
    data = tickers.vdata.getData(['price'], symbols, forceUpdate=True)
    dftn = pd.DataFrame(data['price']).T['price'].dropna()
    dftn.to_csv('Z:\\Quicken\\QuickenImport.csv', header=False, sep=',', encoding='utf-8')


