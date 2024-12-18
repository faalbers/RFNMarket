from .utils import log, utils
from .vault import Data
from .portfolio import Portfolio
import pandas as pd
import numpy as np
from datetime import datetime

class Market():
    def __init__(self, log_level=None):
        if log_level != None:
            log.initLogger(logLevel=log_level)
        self.data = Data()

    def update_data_us(self):
        us_symbols = self.get_us_symbols()
        self.data.update(Portfolio.get_catalogs(), us_symbols)

    def get_us_symbols(self, update=False):
        symbols = self.data.getData(['ussymbols'], update=update)['ussymbols']
        symbols.sort()
        return symbols
        
    
    def get_timeseries(self, symbols, start_date=None, end_date=None, update=False):
        timeseries_data = self.data.getData(['timeSeries'], keyValues=symbols, update=update)['timeSeries']
        if not 'chart' in timeseries_data: return {}
        timeseries_data = timeseries_data['chart']
        if not start_date: start_date = pd.to_datetime(start_date)
        if not end_date: end_date = pd.to_datetime(end_date)
        data = {}
        for symbol, ts_data in timeseries_data.items():
            df_ts_data = pd.DataFrame(ts_data).T
            df_ts_data.sort_index(inplace=True)
            df_ts_data.index = pd.to_datetime(df_ts_data.index, unit='s')
            if start_date and end_date:
                df_ts_data = df_ts_data.loc[start_date:end_date]
            elif start_date:
                df_ts_data = df_ts_data.loc[start_date:]
            elif end_date:
                df_ts_data = df_ts_data.loc[:end_date]
            df = df_ts_data[['open', 'high', 'low', 'close', 'adjclose', 'volume']].astype(np.float64)
            if 'dividend' in df_ts_data.columns:
                df['dividend'] = df_ts_data['dividend'].astype(np.float64)
            if 'splitRatio' in df_ts_data.columns:
                df['splitRatio'] = df_ts_data['splitRatio']
                df[['numerator', 'denominator']] = df_ts_data[['numerator', 'denominator']].astype(np.float64)
            data[symbol] = df
        return data

    def get_news(self, symbols, start_date=None, end_date=None, update=False):
        news_data = self.data.getData(['ticker_news'], keyValues=symbols, update=update)['ticker_news']
        if 'news' not in news_data: return {}
        news_data = news_data['news']
        if not start_date: start_date = pd.to_datetime(start_date)
        if not end_date: end_date = pd.to_datetime(end_date)
        data = {}
        for symbol, ts_data in news_data.items():
            df_news_data = pd.DataFrame(ts_data).T
            df_news_data.sort_index(inplace=True)
            df_news_data.index = pd.to_datetime(df_news_data.index, unit='s')
            if start_date and end_date:
                df_news_data = df_news_data.loc[start_date:end_date]
            elif start_date:
                df_news_data = df_news_data.loc[start_date:]
            elif end_date:
                df_news_data = df_news_data.loc[:end_date]
            data[symbol] = df_news_data
        return data
    
    def data_report(self, symbols):
        data = self.data.getData(['all'], keyValues=symbols)
        allData = {}
        utils.dataStructure(data, allData, set(symbols))
        utils.printHierachy(allData, 'data_report.txt')
