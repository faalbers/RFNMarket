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

    def get_beta_5y_monthly(self, update=False):
        ts_symbols = self.__symbols.copy()
        if 'SPY' not in ts_symbols:
            ts_symbols.append('SPY')
        timeseries_data = self.__data.getData(['timeSeries'], keyValues=ts_symbols)['timeSeries']['chart']
    
    @staticmethod
    def get_beta_5y_monthly_proc(params):
        # get_beta_5y_monthly_proc: get the beta for each symbol in the list
        # beta is calculated as the slope of the linear regression between the
        # monthly returns of the symbol and the monthly returns of SPY
        # over the last 5 years
        symbols = params[0]
        data = params[1]
        ts_symbols = symbols.copy()
        if 'SPY' not in ts_symbols:
            ts_symbols.append('SPY')
        timeseries_data = data.getData(['timeSeries'], keyValues=ts_symbols)['timeSeries']['chart']

        # select the adjclose from the timeseries data
        adj_close_spy = pd.DataFrame(timeseries_data['SPY']).T['adjclose']
        adj_close_spy.sort_index(inplace=True)
        adj_close_spy.index = pd.to_datetime(adj_close_spy.index, unit='s')
        adj_close_spy = adj_close_spy.resample('ME').last().loc[:adj_close_spy.index[-1]]
        monthly_returns_spy = (adj_close_spy / adj_close_spy.shift(1)) - 1.0
        monthly_returns_spy = monthly_returns_spy.iloc[(-12*5):]  # last 5 years

        result = pd.Series(np.nan, index=symbols, name='beta_5y_monthly')
        for symbol in symbols:
            # check if the symbol is in the timeseries data
            # if not, skip to the next symbol
            if not symbol in timeseries_data: continue

            # select the adjclose from the timeseries data
            adj_close = pd.DataFrame(timeseries_data[symbol]).T['adjclose']

            # sort the index of the adjclose
            adj_close.sort_index(inplace=True)

            # convert the index to datetime
            adj_close.index = pd.to_datetime(adj_close.index, unit='s')

            # resample the adjclose to the end of each month
            adj_close = adj_close.resample('ME').last().loc[:adj_close.index[-1]]

            # calculate the monthly returns
            monthly_returns = ((adj_close / adj_close.shift(1)) - 1.0).dropna()

            # select the last 5 years of data
            monthly_returns = monthly_returns.iloc[(-12*5):]
            # select the overlap of the dates
            mrs_in = monthly_returns_spy[monthly_returns_spy.index.isin(monthly_returns.index)]
            if len(mrs_in) < 2: continue  # need at least two points to calculate variance
            mr_in = monthly_returns[monthly_returns.index.isin(mrs_in.index)]
            
            # calculate the variance and covariance
            variance = mrs_in.var()
            covariance = mr_in.cov(mrs_in)

            # calculate the beta
            result[symbol] = covariance / variance

        return result
    
    @staticmethod
    def __multi_proc(symbols, proc):
        # calculate the number of symbols to process in each chunk
        # this is a compromise between using all the cpu's and not
        # overwhelming the database with too many requests at once
        symbols_per_proc = int(max(min(math.ceil(len(symbols) / cpu_count()), 100), 10))
        print('symbols per proc: %s' % symbols_per_proc)

        # create a queue to hold the symbols to process
        task_queue = queue.Queue()
        for symbol in symbols: task_queue.put(symbol)

        # create a list to hold the data objects, one for each proc
        datas = []
        for _ in range(cpu_count()):
            datas.append(Data())

        # create a pandas series to hold the results
        data = pd.Series()

        # create a pool of processes and hand them the work
        with Pool() as pool:
            while not task_queue.empty():
                # build a list of chunks to process in parallel
                chunk = []
                for proc_idx in range(cpu_count()):
                    # build a list of symbols to process in this chunk
                    chunk_symbols = []
                    for _ in range(symbols_per_proc):
                        if task_queue.empty(): break
                        chunk_symbols.append(task_queue.get())
                    if len(chunk_symbols) == 0: break
                    # add a tuple of (symbols, data) to the chunk
                    chunk.append((chunk_symbols, datas[proc_idx]))
                # process the chunk in parallel
                results = pool.map(proc, chunk)
                # print the length of the chunk (for debugging)
                print('chunk len: %s' % len(chunk))

                # concatenate the results into the data series
                for result in results:
                    if len(data) == 0:
                        data = result.copy()
                    else:
                        data = pd.concat([data, result])
                # delete the results to free up memory
                del(results)
        return data

    def __init__(self, symbols=[], update=False, log_level=None):
        # if log_level is not None, set the log level to that value
        if log_level != None:
            log.initLogger(logLevel=log_level)

        # create a Data object to find all the database data
        self.__data = Data()

        # sort the symbols just to be sure they are in order
        symbols.sort()
        self.__symbols = symbols

        # create a list of allocations, one for each symbol
        # each allocation is equal to 1 / number of symbols
        if len(self.__symbols) == 0:
            self.__allocations = []
        else:
            self.__allocations = [1.0/len(self.__symbols)] * len(self.__symbols)

        # if update is True, call update on the data object
        # this will update all the data in the database
        if update:
            self.__data.update(self.__update_catalogs, self.__symbols)
    
    def get_timeseries(self, start_date=None, end_date=None, update=False):
        timeseries_data = self.__multi_proc(self.__symbols, self.get_timeseries_proc)


    def get_beta(self, update=False):
        statistics = self.__data.getData(['statistics'], self.__symbols, update=update)['statistics']
        beta = pd.Series(np.nan, index=self.__symbols, name='beta')
        for symbol in self.__symbols:
            if not symbol in statistics: continue
            if not 'beta' in statistics[symbol]: continue
            beta[symbol] = statistics[symbol]['beta']
        return beta

    def get_eps_ttm(self, update=False):
        data = self.__data.getData(['earnings','statistics'], self.__symbols, update=update)
        eps_ttm = pd.Series(np.nan, index=self.__symbols, name='beta')
        for symbol in self.__symbols:
            if not symbol in data['earnings'] or not symbol in data['statistics']: continue
            if not 'sharesOutstanding' in data['statistics'][symbol]: continue
            
            qData = data['earnings'][symbol]['financialsChart']['quarterly']
            earnings_ttm = pd.DataFrame(qData)
            if earnings_ttm.shape[0] == 0: continue
            earnings_ttm = earnings_ttm['earnings'].dropna().sum()
            
            shares_outstanding = data['statistics'][symbol]['sharesOutstanding']
            if shares_outstanding == 0: continue
            if symbol == 'GOOG':
                print(earnings_ttm, shares_outstanding)
                print(earnings_ttm / shares_outstanding)
                print(earnings_ttm / 12264000000)
            eps_ttm[symbol] = earnings_ttm / shares_outstanding

        return eps_ttm

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