from .ticker import Ticker, Data
from .utils import log
import pandas as pd
import numpy as np

class Portfolio():
    def __init__(self, symbols=[], update=False, log_level=log.WARNING):
        log.initLogger(logLevel=log_level)
        self.__tickers = []
        symbols.sort()
        if update:
           Data().update(Ticker.get_catalogs(), symbols)
        for symbol in symbols:
            self.__tickers.append(Ticker(symbol))
        self.__allocations = [1.0/len(self.__tickers)] * len(self.__tickers)

    def get_tickers_symbols(self):
        return [ticker.symbol for ticker in self.__tickers]
    
    def get_metrics(self):
        symbols = self.get_tickers_symbols()
        df = pd.DataFrame(index=symbols)
        df['allocation'] = np.array(self.__allocations) * 100
        df['beta'] = [ticker.get_beta_5y_monthly() for ticker in self.__tickers]
        df['eps'] = [ticker.get_eps_ttm() for ticker in self.__tickers]
        return df

    def __str__(self):
        info = 'Portfolio:'
        info += '\n%s' % self.get_metrics().round(2)
        # for ticker, allocation in zip(self.__tickers, self.__allocations):
        #     info += '\n%s:\t%.2f %%' % (ticker.symbol, allocation*100)
        return info