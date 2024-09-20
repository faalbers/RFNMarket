import pandas as pd
from pprint import pp
from ..utils import database

class Data():
    @staticmethod
    def __addExchangeData(data):
        db = database.Database('saved')
        
        # create mic to country code dict
        micsSaved = db.getTableDataFrame('ISO10383_MIC')
        micsSaved.rename(columns={'ISO COUNTRY CODE (ISO 3166)': 'cc'}, inplace=True)
        micCC = {}
        for row in micsSaved[['MIC', 'cc']].itertuples():
            micCC[row.MIC] = row.cc
        micCC[None] = None

        # create country code to country name dict
        countriesSaved = db.getTableDataFrame('ISO3166-1')
        ccCountries = {None: None}
        for row in countriesSaved[['Code', 'Name']].itertuples():
            ccCountries[row.Code] = row.Name
        ccCountries[None] = None
        
        # create new country name row and add it to DataFrame
        countryList = [ccCountries[micCC[mic]] for mic in data['mic']]
        data.insert(data.columns.get_loc('mic')+1, 'micCountry', countryList)

    @staticmethod
    def __addExchangeDataOld(data):
        # get country codes for mics
        db = database.Database('saved')
        micsSaved = db.getTableDataFrame('ISO10383_MIC')
        micsSaved.set_index('MIC', inplace=True)
        mics = data.loc[:,'mic']
        countryCodes = []
        for mic in mics:
            if mic != None:
                countryCode = micsSaved.loc[micsSaved.index == 'XNAS'].loc['XNAS','ISO COUNTRY CODE (ISO 3166)']
                countryCodes.append(countryCode)
            else:
                countryCodes.append(None)
        
        # add micCountry column
        data.insert(data.columns.get_loc('mic')+1, 'micCountry', countryCodes)
    
    __catalog = {
        'statistics': {
            'info': 'key ticker statistics',
            'data': {
                # param_name [db_name, table_name, column_name, make upper]
                'trailingPE': ['yahoo_quotesummary', 'summaryDetail', 'trailingPE', False],
                'trailingEps': ['yahoo_quotesummary', 'defaultKeyStatistics', 'trailingEps', False],
                'forwardEps': ['yahoo_quotesummary', 'defaultKeyStatistics', 'forwardEps', False],
                'pegRatio': ['yahoo_quotesummary', 'defaultKeyStatistics', 'pegRatio', False],
                'ttmDividendRate': ['yahoo_quotesummary', 'summaryDetail', 'trailingAnnualDividendRate', False],
                'earningsGrowth': ['yahoo_quotesummary', 'financialData', 'earningsGrowth', False],
                'revenueGrowth': ['yahoo_quotesummary', 'financialData', 'revenueGrowth', False],
                'revenuePerShare': ['yahoo_quotesummary', 'financialData', 'revenuePerShare', False],
            },
            'post': [],
        },
        'profile': {
            'info': 'ticker company profile information',
            'data': {
                # param_name [db_name, table_name, column_name, make upper]
                'name': ['yahoo_quotesummary', 'quoteType', 'longName', False],
                'exchange': ['fmp_stocklist', 'stocklist', 'exchangeShortName', False],
                'mic': ['polygon_tickers', 'tickers', 'primary_exchange', False],
                'market': ['polygon_tickers', 'tickers', 'market', False],
                'type': ['fmp_stocklist', 'stocklist', 'type', True],
                'typeCode': ['polygon_tickers', 'tickers', 'type', False],
                'typeQuote': ['yahoo_quotesummary', 'quoteType', 'quoteType', False],
                'currency': ['yahoo_quotesummary', 'summaryDetail', 'currency', False],
                'sector': ['yahoo_quotesummary', 'assetProfile', 'sectorKey', False],
                'industry': ['yahoo_quotesummary', 'assetProfile', 'industryKey', False],
                'country': ['yahoo_quotesummary', 'assetProfile', 'country', False],
                'city': ['yahoo_quotesummary', 'assetProfile', 'city', False],
                'state': ['yahoo_quotesummary', 'assetProfile', 'state', False],
            },
            'post': [__addExchangeData],
        },
    }
 
    def __getTableColumns(self, params, symbols, dbName):
        symbolsSet = set(symbols)
        modules = {}
        for pitems in params:
            if not pitems[0] in modules:
                modules[pitems[0]] = [[]]
            modules[pitems[0]][0].append(pitems[1:])
        db = database.Database(dbName)
        for module, mparams in modules.items():
            columns = ['keySymbol']+[x[0] for x in mparams[0]]
            values, dbparams = db.getRows(module, ['keySymbol']+[x[0] for x in mparams[0]] )
            fvalues = {}
            for value in values:
                if not value[0] in symbolsSet: continue
                fvalues[value[0]] = {}
                index = 1
                for param in mparams[0]:
                    fvalue = value[index]
                    if param[1]:
                        fvalue = fvalue.upper()
                    fvalues[value[0]][param[-1]] = fvalue
                    index += 1

            mparams.append(fvalues)

        data = {}
        for symbol in symbols:
            values = {}
            for param in params:
                module = modules[param[0]]
                if symbol in module[1]:
                    values[param[-1]] = module[1][symbol][param[-1]]
            data[symbol] = values

        return data
    
    def __getDatabaseData(self, symbols, catalog):
        if not catalog in self.__catalog: return pd.DataFrame()

        # find all database queries and fill DataFrame columns list
        dbQuery = {}
        dfColumns = ['symbol']
        catData =  self.__catalog[catalog]['data']
        for param , dbData in catData.items():
            dfColumns.append(param)
            if not dbData[0] in dbQuery:
                dbQuery[dbData[0]] = []
            dbQuery[dbData[0]].append(dbData[1:]+[param])
        
        # prepare empty symbols data
        sData = {}
        for symbol in symbols:
            sData[symbol] = [None]*len(dfColumns[1:])
        
        # get data database query
        for dbName, params in dbQuery.items():
            if dbName == 'yahoo_quotesummary' \
                or dbName == 'fmp_stocklist' \
                or dbName == 'polygon_tickers':
                dbData = self.__getTableColumns(params, symbols, dbName)
                for symbol in symbols:
                    index = 0
                    for column in dfColumns[1:]:
                        if column in dbData[symbol]:
                            sData[symbol][index] = dbData[symbol][column]
                        index += 1

        # create DataFrame
        rows = []
        for symbol in symbols:
            row = [symbol]+sData[symbol]
            rows.append(row)
        df = pd.DataFrame(rows, columns=dfColumns)
        df.set_index('symbol', inplace=True)

        for proc in self.__catalog[catalog]['post']:
            proc(df)
        # print(df.columns)
        # print(df)

        return df

    def __init__(self):
        pass
    
    def getData(self, symbols, catalogs):
        data = {}
        for catalog in catalogs:
            dbdata = self.__getDatabaseData(symbols, catalog)
            if not dbdata.empty:
                data[catalog] = dbdata
        return data

    def getCatalog(self):
        catalog = {}
        for cat, data in self.__catalog.items():
            catalog[cat] = data['info']
        return catalog
    
    def getSymbols(self):
        db = database.Database('yahoo_quotesummary')
        values, params = db.getRows('status_db', ['keySymbol'])
        return [x[0] for x in values] 
