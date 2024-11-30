from .vault import Data
from .utils import GICS
import pandas as pd
import pandas_ta as ta
import numpy as np
from pprint import pp

class Ticker():
    def __init__(self, symbol):
        self.symbol = symbol
        self.__data = Data()
    
    def get_timeseries(self, start_date=None, end_date=None, update=False):
        
        timeseries_data = self.__data.getData(['timeSeries'], keyValues=[self.symbol], update=update)['timeSeries']['chart']
        if not self.symbol in timeseries_data: pd.DataFrame()
        
        if not start_date: start_date = pd.to_datetime(start_date)
        if not end_date: end_date = pd.to_datetime(end_date)
        
        df_ts_data = pd.DataFrame(timeseries_data[self.symbol]).T
        df_ts_data.sort_index(inplace=True)
        df_ts_data.index = pd.to_datetime(df_ts_data.index, unit='s')
        if start_date and end_date:
            df_ts_data = df_ts_data.loc[start_date:end_date]
        elif start_date:
            df_ts_data = df_ts_data.loc[start_date:]
        elif end_date:
            df_ts_data = df_ts_data.loc[:end_date]
        df = df_ts_data[['open', 'high', 'low', 'close', 'adjclose','volume']].astype(np.float64)
        if 'dividend' in df_ts_data.columns:
            df['dividend'] = df_ts_data['dividend'].astype(np.float64)
        if 'splitRatio' in df_ts_data.columns:
            df['splitRatio'] = df_ts_data['splitRatio']
            df[['numerator', 'denominator']] = df_ts_data[['numerator', 'denominator']].astype(np.float64)
        
        return df
    
    def get_sma(self, start_date=None, end_date=None, update=False):

        df_ts = self.get_timeseries(start_date, end_date, update=update)
        if df_ts.shape[0] == 0: return df_ts

        df = df_ts[['adjclose']].copy()
        df['SMA_50'] = ta.sma(df_ts['adjclose'], length=50)
        df['SMA_100'] = ta.sma(df_ts['adjclose'], length=100)
        df['SMA_200'] = ta.sma(df_ts['adjclose'], length=200)

        return df
    
    def get_ema(self, start_date=None, end_date=None, update=False):

        df_ts = self.get_timeseries(start_date, end_date, update=update)
        if df_ts.shape[0] == 0: return df_ts

        df = df_ts[['adjclose']].copy()
        df['EMA_50'] = ta.ema(df_ts['adjclose'], length=50)
        df['EMA_100'] = ta.ema(df_ts['adjclose'], length=100)
        df['EMA_200'] = ta.ema(df_ts['adjclose'], length=200)

        return df
    def get_rsi(self, start_date=None, end_date=None, update=False):

        df_ts = self.get_timeseries(start_date, end_date, update=update)
        if df_ts.shape[0] == 0: return df_ts

        df = df_ts[['adjclose']].copy()
        df['RSI'] = ta.rsi(df_ts['adjclose'], length=14)

        return df
    
    def get_bollinger_bands(self, start_date=None, end_date=None, update=False):

        df_ts = self.get_timeseries(start_date, end_date, update=update)
        if df_ts.shape[0] == 0: return df_ts

        df = df_ts[['adjclose']].copy()
        
        bollinger = ta.bbands(df_ts['adjclose'], length=20, std=2)
        df['BB_Upper'] = bollinger['BBU_20_2.0']  # Upper Bollinger Band
        df['BB_Middle'] = bollinger['BBM_20_2.0'] # Middle Band (20-period SMA)
        df['BB_Lower'] = bollinger['BBL_20_2.0']  # Lower Bollinger Band

        return df
    
    def get_stoch(self, start_date=None, end_date=None, update=False):

        df_ts = self.get_timeseries(start_date, end_date, update=update)
        if df_ts.shape[0] == 0: return df_ts

        df = df_ts[['adjclose']].copy()
        
        stoch = ta.stoch(df_ts['high'], df_ts['low'], df_ts['close'], k=14, d=3)
        df['%K'] = stoch['STOCHk_14_3_3'] # %K line (main line)
        df['%D'] = stoch['STOCHd_14_3_3'] # %D line (3-period moving average of %K)

        return df
    
    def get_macd(self, start_date=None, end_date=None, update=False):

        df_ts = self.get_timeseries(start_date, end_date, update=update)
        if df_ts.shape[0] == 0: return df_ts

        df = df_ts[['adjclose']].copy()
        
        macd = ta.macd(df_ts['adjclose'], fast=12, slow=26, signal=9)
        df['MACD'] = macd['MACD_12_26_9']        # MACD line
        df['Signal_Line'] = macd['MACDs_12_26_9'] # Signal line

        return df
    

    def get_atr(self, start_date=None, end_date=None, update=False):

        df_ts = self.get_timeseries(start_date, end_date, update=update)
        if df_ts.shape[0] == 0: return df_ts

        df = df_ts[['adjclose']].copy()
        
        df['ATR'] = ta.atr(df_ts['high'], df_ts['low'], df_ts['close'], length=14)

        return df
    

    def get_technical_indicators(self, start_date=None, end_date=None, update=False):

        df_ts = self.get_timeseries(start_date, end_date, update=update)
        if df_ts.shape[0] == 0: return df_ts

        df = df_ts[['adjclose']].copy()
        
        df['SMA_50'] = ta.sma(df_ts['adjclose'], length=50)
        df['SMA_100'] = ta.sma(df_ts['adjclose'], length=100)
        df['SMA_200'] = ta.sma(df_ts['adjclose'], length=200)
        
        df['EMA_50'] = ta.ema(df_ts['adjclose'], length=50)
        
        df['RSI_14'] = ta.rsi(df_ts['adjclose'], length=14)
        
        macd = ta.macd(df_ts['adjclose'], fast=12, slow=26, signal=9)
        df['MACD'] = macd['MACD_12_26_9']        # MACD line
        df['Signal_Line'] = macd['MACDs_12_26_9'] # Signal line
        
        bollinger = ta.bbands(df_ts['adjclose'], length=20, std=2)
        df['BB_Upper'] = bollinger['BBU_20_2.0']  # Upper Bollinger Band
        df['BB_Middle'] = bollinger['BBM_20_2.0'] # Middle Band (20-period SMA)
        df['BB_Lower'] = bollinger['BBL_20_2.0']  # Lower Bollinger Band
        
        stoch = ta.stoch(df_ts['high'], df_ts['low'], df_ts['close'], k=14, d=3)
        df['%K'] = stoch['STOCHk_14_3_3'] # %K line (main line)
        df['%D'] = stoch['STOCHd_14_3_3'] # %D line (3-period moving average of %K)

        df['ATR'] = ta.atr(df_ts['high'], df_ts['low'], df_ts['close'], length=14)

        return df
    
    def get_statistics(self, update=False):
        
        statistics_data = self.__data.getData(['statistics'], keyValues=[self.symbol], update=update)['statistics']
        if not self.symbol in statistics_data: return None
        statistics_data = statistics_data[self.symbol]

        statistics = pd.Series(statistics_data).dropna().astype(np.float64)

        if 'forwardPE_A' in statistics and 'forwardPE_B' in statistics:
            statistics['forwardPE'] = statistics.loc[['forwardPE_A', 'forwardPE_B']].mean()
            statistics.drop(['forwardPE_A', 'forwardPE_B'], inplace=True)
        elif 'forwardPE_A' in statistics:
            statistics.rename(index={'forwardPE_A': 'forwardPE'}, inplace=True)
        elif 'forwardPE_B' in statistics:
            statistics.rename(index={'forwardPE_B': 'forwardPE'}, inplace=True)

        
        statistics.sort_index(inplace=True)
        
        print(statistics)

