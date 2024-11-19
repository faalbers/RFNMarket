from .utils import log, utils
from .vault import Data
from .ticker import Ticker
import pandas as pd

class Tickers():
    def __init__(self, logLevel=log.WARNING):
        log.initLogger(logLevel=logLevel)
        self.data = Data()

    def update_data_us(self):
        us_symbols = self.get_us_symbols()
        catalogs = Ticker.get_catalogs()
        self.data.update(catalogs, us_symbols)

    def get_us_symbols(self):
        return self.data.getData(['ussymbols'])['ussymbols']
    
    def get_timeseries(self, symbols, start_date=None, end_date=None, update=False):
        timeseries_data = self.data.getData(['timeSeries'], keyValues=symbols, update=update)['timeSeries']['chart']
        if start_date == None:
            start_date = df.index[0]
        else:
            start_date = pd.to_datetime(start_date)
        if end_date == None:
            end_date = df.index[-1]
        else:
            end_date = pd.to_datetime(end_date)
        data = {}
        for symbol, ts_data in timeseries_data.items():
            df = pd.DataFrame(ts_data).T
            df.sort_index(inplace=True)
            df.index = pd.to_datetime(df.index, unit='s')
            df = df.loc[start_date:end_date]
            data[symbol] = df
        return data

    def data_report(self, symbols):
        data = self.data.getData(['all'], keyValues=symbols)
        allData = {}
        utils.dataStructure(data, allData, set(symbols))
        utils.printHierachy(allData, 'data_report.txt')
