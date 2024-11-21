from .ticker import Ticker, Data
from .utils import log
import pandas as pd
import numpy as np
from multiprocessing import Pool, cpu_count
import queue, math
from pprint import pp

class Portfolio():
    __update_catalogs = ['timeSeries', 'earnings', 'statistics', 'profile']

    @staticmethod
    def get_catalogs():
        return Portfolio.__update_catalogs

    @staticmethod
    def get_timeseries_proc(params):
        symbols = params[0]
        data = params[1]
        chart = data.getData(['timeSeries'], keyValues=symbols)['timeSeries']['chart']
        return chart
    
    @staticmethod
    def get_beta_5y_monthly_proc(params):
        symbols = params[0]
        data = params[1]
        ts_symbols = symbols.copy()
        if 'SPY' not in ts_symbols:
            ts_symbols.append('SPY')
        timeseries_data = data.getData(['timeSeries'], keyValues=ts_symbols)['timeSeries']['chart']
        adj_close = pd.DataFrame(timeseries_data['SPY']).T['adjclose']
        adj_close.sort_index(inplace=True)
        adj_close.index = pd.to_datetime(adj_close.index, unit='s')
        adj_close = adj_close.resample('ME').last().loc[:adj_close.index[-1]]
        monthly_returns_spy = (adj_close / adj_close.shift(1)) - 1.0
        monthly_returns_spy = monthly_returns_spy.iloc[(-12*5):]

        result = pd.Series(np.nan, index=symbols, name='beta_5y_monthly')
        for symbol in symbols:
            if not symbol in timeseries_data: continue
            adj_close = pd.DataFrame(timeseries_data[symbol]).T['adjclose']
            adj_close.sort_index(inplace=True)
            adj_close.index = pd.to_datetime(adj_close.index, unit='s')
            adj_close = adj_close.resample('ME').last().loc[:adj_close.index[-1]]
            monthly_returns = ((adj_close / adj_close.shift(1)) - 1.0).dropna()
            monthly_returns = monthly_returns.iloc[(-12*5):]
            mrs_in = monthly_returns_spy[monthly_returns_spy.index.isin(monthly_returns.index)]
            if len(mrs_in) < 2: continue
            mr_in = monthly_returns[monthly_returns.index.isin(mrs_in.index)]
            variance = mrs_in.var()
            covariance = mr_in.cov(mrs_in)
            result[symbol] = covariance / variance

        return result
    
    @staticmethod
    def __multi_proc(symbols, proc):
        symbols_per_proc = int(max(min(math.ceil(len(symbols) / cpu_count()), 100), 10))
        print('symbols per proc: %s' % symbols_per_proc)
        task_queue = queue.Queue()
        for symbol in symbols: task_queue.put(symbol)
        datas = []
        for _ in range(cpu_count()):
            datas.append(Data())
        data = pd.Series()
        with Pool() as pool:
            while not task_queue.empty():
                chunk = []
                for proc_idx in range(cpu_count()):
                    chunk_symbols = []
                    for _ in range(symbols_per_proc):
                        if task_queue.empty(): break
                        chunk_symbols.append(task_queue.get())
                    if len(chunk_symbols) == 0: break
                    chunk.append((chunk_symbols, datas[proc_idx]))
                results = pool.map(proc, chunk)
                print('chunk len: %s' % len(chunk))
                for result in results:
                    if len(data) == 0:
                        data = result.copy()
                    else:
                        data = pd.concat([data, result])
                del(results)
        return data
    
    def __init__(self, symbols=[], update=False, log_level=None):
        if log_level != None:
            log.initLogger(logLevel=log_level)
        self.__data = Data()
        symbols.sort()
        self.__symbols = symbols
        self.__allocations = [1.0/len(self.__symbols)] * len(self.__symbols)
        if update:
           self.__data.update(self.__update_catalogs, self.__symbols)
    
    def get_timeseries(self, start_date=None, end_date=None, update=False):
        timeseries_data = self.__multi_proc(self.__symbols, self.get_timeseries_proc)

    def get_beta_5y_monthly(self, update=False):
        beta_5y_monthly = self.__multi_proc(self.__symbols, self.get_beta_5y_monthly_proc)
        return beta_5y_monthly

    def get_beta(self, update=False):
        statistics = self.__data.getData(['statistics'], self.__symbols, update=update)['statistics']
        beta = pd.Series(np.nan, index=self.__symbols, name='beta')
        for symbol in self.__symbols:
            if not symbol in statistics: continue
            if not 'beta' in statistics[symbol]: continue
            beta[symbol] = statistics[symbol]['beta']
        return beta

    def get_tickers_symbol(self):
        return [ticker.symbol for ticker in self.__tickers]
    
    def get_metrics(self):
        symbols = self.get_tickers_symbol()
        df = pd.DataFrame(index=symbols)
        df['allocation'] = np.array(self.__allocations) * 100
        names = []
        for ticker in self.__tickers:
            profile = ticker.get_profile()
            if 'name' in profile:
                names.append(profile['name'])
            else:
                names.append(None)
        df['name'] = names
        df['beta'] = [ticker.get_beta_5y_monthly() for ticker in self.__tickers]
        df['eps'] = [ticker.get_eps_ttm() for ticker in self.__tickers]
        df['pe'] = [ticker.get_pe_ttm() for ticker in self.__tickers]
        df['cap'] = [ticker.get_profile()['marketCapCategory'] for ticker in self.__tickers]
        return df

    def __str__(self):
        info = 'Portfolio:'
        info += '\n%s' % self.get_metrics().round(2)
        # for ticker, allocation in zip(self.__tickers, self.__allocations):
        #     info += '\n%s:\t%.2f %%' % (ticker.symbol, allocation*100)
        return info