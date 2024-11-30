from rfnmarket import Market, QPortfolio, Data
from pprint import pp
from datetime import datetime
import pandas as pd

if __name__ == '__main__':
    tickers = Market()
    qpf = QPortfolio('database/2020.QIF')
    symbols = qpf.getSecurityShares()
    data = Data().getData(['price'], symbols, forceUpdate=True)
    dftn = pd.DataFrame(data['price']).T['price'].dropna()
    dftn.to_csv('Z:\\Quicken\\QuickenImport.csv', header=False, sep=',', encoding='utf-8')


