from .vault import Data
from .utils import GICS
import pandas as pd
from pprint import pp

class Ticker():
    @staticmethod
    def get_catalogs():
        return ['timeSeries', 'earnings', 'statistics', 'profile']

    def __init__(self, symbol):
        self.symbol = symbol
        self.__data = Data()
    
    def get_timeseries(self, start_date=None, end_date=None, update=False):
        timeseries_data = self.__data.getData(['timeSeries'], keyValues=[self.symbol], update=update)['timeSeries']['chart']
        if not self.symbol in timeseries_data: return None
        timeseries_data = timeseries_data[self.symbol]
        df = pd.DataFrame(timeseries_data).T
        df.sort_index(inplace=True)
        df.index = pd.to_datetime(df.index, unit='s')
        if start_date == None:
            start_date = df.index[0]
        else:
            start_date = pd.to_datetime(start_date)
        if end_date == None:
            end_date = df.index[-1]
        else:
            end_date = pd.to_datetime(end_date)
        df = df.loc[start_date:end_date]
        return df
    
    def get_adjusted_close(self, start_date=None, end_date=None, update=False):
        adj_close = self.get_timeseries(start_date, end_date, update=update)['adjclose']
        return adj_close
    
    def get_adjusted_close_monthly(self, start_date=None, end_date=None, update=False):
        adj_close = self.get_timeseries(start_date, end_date, update=update)['adjclose']
        return adj_close.resample('ME').last().loc[:adj_close.index[-1]]
    
    def get_monthly_returns(self, start_date=None, end_date=None, update=False):
        adj_close = self.get_adjusted_close_monthly(start_date, end_date, update=update)
        return ((adj_close / adj_close.shift(1)) - 1.0)
    
    def get_adjusted_close_yearly(self, start_date=None, end_date=None, update=False):
        adj_close = self.get_timeseries(start_date, end_date, update=update)['adjclose']
        # print(adj_close)
        print('blah')
        return adj_close.resample('YE').last().loc[:adj_close.index[-1]]

    def get_yearly_returns(self, start_date=None, end_date=None, update=False):
        adj_close = self.get_adjusted_close_yearly(start_date, end_date, update=update)
        return ((adj_close / adj_close.shift(1)) - 1.0)
    
    def get_daily_returns(self, start_date=None, end_date=None, update=False):
        adj_close = self.get_adjusted_close(start_date, end_date, update=update)
        return ((adj_close / adj_close.shift(1)) - 1.0)

    def get_cumulative_returns(self, start_date=None, end_date=None, update=False):
        adj_close = self.get_timeseries(start_date, end_date, update=update)['adjclose']
        return (adj_close / adj_close.iloc[0]) - 1.0
    
    def get_beta_5y_monthly(self, update=False):
        monthly_returns = self.get_monthly_returns(update=update).iloc[(-12*5):]
        monthly_returns_spy = Ticker('SPY').get_monthly_returns(update=update).iloc[(-12*5):]
        covariance = monthly_returns.cov(monthly_returns_spy)
        variance = monthly_returns_spy.var()
        return covariance / variance
    
    def get_earnings_quarterly(self, update=False):
        data = self.__data.getData(['earnings'], [self.symbol], update=update)['earnings']
        if not self.symbol in data: return None
        data = data[self.symbol]['financialsChart']['quarterly']
        data = pd.DataFrame(data)
        data.set_index('date', inplace=True)

        return data
    
    def get_eps_ttm(self, update=False):
        earnings_ttm = self.get_earnings_quarterly(update=update)
        if earnings_ttm is None: return None
        earnings_ttm = earnings_ttm['earnings'].dropna().values.sum()
        shares_outstanding = self.__data.getData(['statistics'], [self.symbol], update=update)['statistics'][self.symbol]
        shares_outstanding = shares_outstanding['sharesOutstanding']
        return earnings_ttm / shares_outstanding

    def get_eps_quarterly(self, update=False):
        # data = self.__data.getData(['earnings'], [self.symbol], update=update)['earnings'][self.symbol]['earningsChart']['quarterly']
        data = self.__data.getData(['earnings'], [self.symbol], update=update)['earnings'][self.symbol]
        pp(data)
        data = data['earningsChart']['quarterly']
        data = pd.DataFrame(data)
        data.set_index('date', inplace=True)

        return data

    def get_profile(self, update = False):
        data = self.__data.getData(['profile'], [self.symbol], update=update)['profile']
        # get all mics and acronyms that have US country code and update profile
        mics = data['mic']['ISO10383_MIC']
        usacronyms = set()
        usmics = set()
        for micRow in mics:
            if 'cc' in micRow and micRow['cc'] == 'US':
                usmics.add(micRow['mic'])
                if 'acronym' in micRow:
                    usacronyms.add(micRow['acronym'])

        # for keyValue, keyData in data['profile'].items():
        data = data['profile'][self.symbol]
        if 'mic' in data and data['mic'] in usmics:
            data['exchangeCountry'] = 'United States'
        if 'acronym' in data and data['acronym'] in usacronyms:
            data['exchangeCountry'] = 'United States'

        # GICS names
        gics = GICS()
        sectors = gics.getNames('sector')
        industryGroups = gics.getNames('industryGroup')
        industries = gics.getNames('industry')
        subIndustries = gics.getNames('subIndustry')

        # for symbol, sData in data.items():
        # set GICS data
        industry = None
        if 'industry' in data:
            industry = data.pop('industry')
        industryNASDAQ = None
        if 'industryNASDAQ' in data:
            industryNASDAQ = data.pop('industryNASDAQ')
        sectorSP500 = None
        if 'sectorSP500' in data:
            sectorSP500 = data.pop('sectorSP500')
        
        if industry == None: industry = industryNASDAQ

        symbolGICS = {}
        if industry != None and industry != '':
            if gics.renameSubIndustry(industry) in subIndustries:
                industry = gics.renameSubIndustry(industry)
                symbolGICS['sector'] = subIndustries[industry][0]
                symbolGICS['industryGroup'] = subIndustries[industry][1]
                symbolGICS['industry'] = subIndustries[industry][2]
                symbolGICS['subIndustry'] = industry
            elif gics.renameIndustry(industry) in industries:
                industry = gics.renameIndustry(industry)
                symbolGICS['sector'] = industries[industry][0]
                symbolGICS['industryGroup'] = industries[industry][1]
                symbolGICS['industry'] = industry
            elif gics.renameIndustryGroup(industry) in industryGroups:
                industry = gics.renameIndustryGroup(industry)
                symbolGICS['sector'] = industryGroups[industry][0]
                symbolGICS['industryGroup'] = industry
            elif gics.renameSector(industry) in sectors:
                industry = gics.renameSector(industry)
                symbolGICS['sector'] = industry
        elif sectorSP500 != None:
            symbolGICS['sector'] = sectorSP500
        
        # set market cap category
        if 'marketCap' in data:
            if data['marketCap'] < 50000000:
                data['marketCapCategory'] = 'Nano Cap'
            elif data['marketCap'] < 300000000:
                data['marketCapCategory'] = 'Micro Cap'
            elif data['marketCap'] < 2000000000:
                data['marketCapCategory'] = 'Small Cap'
            elif data['marketCap'] < 10000000000:
                data['marketCapCategory'] = 'Mid Cap'
            elif data['marketCap'] < 200000000000:
                data['marketCapCategory'] = 'Large Cap'
            else:
                data['marketCapCategory'] = 'Mega Cap'
        
        if len(symbolGICS) > 0:
            data = {**data,**symbolGICS}

        return data
    
    def __str__(self):
        info = '%s:' % self.symbol
        profile = self.get_profile()
        info += '\nname: %s' % profile['name']
        info += '\ninfo: %s' % profile['info']
        return info