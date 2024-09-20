import pandas as pd
from pprint import pp
from ..utils import database
from .. import scrape

class Data():
    @staticmethod
    def __addExchangeData(data):
        db = database.Database(scrape.saved.Saved.dbName)
        
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
    
    __catalog = {
        'statistics': {
            'info': 'key ticker statistics',
            'data': {
                # param_name [db_name, table_name, column_name, make upper]
                'trailingPE': [scrape.yahoo.QuoteSummary, 'summaryDetail', 'trailingPE', False],
                'trailingEps': [scrape.yahoo.QuoteSummary, 'defaultKeyStatistics', 'trailingEps', False],
                'forwardEps': [scrape.yahoo.QuoteSummary, 'defaultKeyStatistics', 'forwardEps', False],
                'pegRatio': [scrape.yahoo.QuoteSummary, 'defaultKeyStatistics', 'pegRatio', False],
                'ttmDividendRate': [scrape.yahoo.QuoteSummary, 'summaryDetail', 'trailingAnnualDividendRate', False],
                'earningsGrowth': [scrape.yahoo.QuoteSummary, 'financialData', 'earningsGrowth', False],
                'revenueGrowth': [scrape.yahoo.QuoteSummary, 'financialData', 'revenueGrowth', False],
                'revenuePerShare': [scrape.yahoo.QuoteSummary, 'financialData', 'revenuePerShare', False],
            },
            'post': [],
        },
        'profile': {
            'info': 'ticker company profile information',
            'data': {
                # param_name [db_name, table_name, column_name, make upper]
                'name': [scrape.yahoo.QuoteSummary, 'quoteType', 'longName', False],
                'exchange': [scrape.fmp.StockList, 'stocklist', 'exchangeShortName', False],
                'mic': [scrape.polygon.Tickers, 'tickers', 'primary_exchange', False],
                'market': [scrape.polygon.Tickers, 'tickers', 'market', False],
                'type': [scrape.fmp.StockList, 'stocklist', 'type', True],
                'typeCode': [scrape.polygon.Tickers, 'tickers', 'type', False],
                'typeQuote': [scrape.yahoo.QuoteSummary, 'quoteType', 'quoteType', False],
                'currency': [scrape.yahoo.QuoteSummary, 'summaryDetail', 'currency', False],
                'sector': [scrape.yahoo.QuoteSummary, 'assetProfile', 'sectorKey', False],
                'industry': [scrape.yahoo.QuoteSummary, 'assetProfile', 'industryKey', False],
                'country': [scrape.yahoo.QuoteSummary, 'assetProfile', 'country', False],
                'city': [scrape.yahoo.QuoteSummary, 'assetProfile', 'city', False],
                'state': [scrape.yahoo.QuoteSummary, 'assetProfile', 'state', False],
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
        for scrapeClass, params in dbQuery.items():
            if scrapeClass == scrape.yahoo.QuoteSummary \
                or scrapeClass == scrape.fmp.StockList \
                or scrapeClass == scrape.polygon.Tickers:
                dbData = self.__getTableColumns(params, symbols, scrapeClass.dbName)
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

        return df

    def __init__(self):
        pass
    
    def update(self, symbols, catalogs):
        print('update: %s' % catalogs)
        # gather scrape classes and needed tables
        scrapeClasses = {}
        for catalog in catalogs:
            if catalog in self.__catalog:
                for param, config in self.__catalog[catalog]['data'].items():
                    if not config[0] in scrapeClasses:
                        scrapeClasses[config[0]] = set()
                    scrapeClasses[config[0]].add(config[1])
        
        # create scrapers and pass tables to update
        for scraperClass, tables in scrapeClasses.items():
            scraperClass(symbols, tables=tables)

    def getData(self, symbols, catalogs, update=False):
        if update: self.update(symbols, catalogs)
        
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
        db = database.Database(scrape.yahoo.QuoteSummary.dbName)
        values, params = db.getRows('status_db', ['keySymbol'])
        return [x[0] for x in values] 
