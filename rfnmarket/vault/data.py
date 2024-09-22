import pandas as pd
from pprint import pp
from ..utils import database
from .. import scrape
from datetime import datetime

class Data():
    @staticmethod
    def __mergeDataFrames(data):
        newData = {}
        for symbol, dataFrames in data.items():
            names = list(dataFrames.keys())
            if len(names) == 1:
                newData[symbol] = dataFrames[names[0]]
            elif len(names) > 1:
                dfMerged = dataFrames[names[0]]
                for name in names[1:]:
                    dfMerged = pd.merge(dfMerged, dataFrames[name], left_index=True, right_index=True, how='outer')
                newData[symbol] = dfMerged
        return newData

    @staticmethod
    def __addExchangeData(data):
        mics = data['countries']['ISO10383_MIC']
        mics.loc['*'] = None
        countries = data['countries']['ISO3166-1']
        countries.loc['*'] = None
        profile = data['profile']['merged']

        # find countrie names from mics
        pmics = profile.loc[:,'mic'].copy()
        pmics.fillna('*', inplace=True)
        ccs = mics.loc[pmics,'cc']
        ccs.fillna('*', inplace=True)
        cnames = countries.loc[ccs,'country']

        # add Country names after mic column
        profile.insert(profile.columns.get_loc('mic')+1, 'micCountry', list(cnames))

        # remove unneeded dataframes
        data.pop('countries')

    # sub_table_name: sub table name to be searched
    __catalog = {
        # columnSets: example: ['keySymbol', 'symbol', True, True, True]
        # [column_search, column_name, make_index, check_symbols, make upper, make_datetime]
        # column_search: column name of the querried column
        #              if value is '*' take all columns and capitalise name if column_name is not empty
        #              then add column_name as suffix
        # column_name: final name of the column
        # make_index: make parameter the index and make it unique
        # check_symbols: cross check with symbols
        # make_upper_case: make this data upper case
        # make_datetime: turn collumn timestamps into Datetime
        'statistics': {
            'info': 'ticker company profile information',
            'dataFrames': {
                'statistics': {
                    'postFunctions': ['merge'],
                    'scrapes': {
                        scrape.yahoo.QuoteSummary: {    # scrape class to retrieve data from
                            'defaultKeyStatistics': {   # table name to be searched
                                'columnSets': [
                                    ['keySymbol', 'symbol', True, True, True, False],
                                    ['trailingEps', 'trailingEps', False, False, False, False],
                                    ['forwardEps', 'forwardEps', False, False, False, False],
                                    ['pegRatio', 'pegRatio', False, False, False, False],
                                ],
                            },
                            'summaryDetail': {
                                'columnSets': [
                                    ['keySymbol', 'symbol', True, True, True, False],
                                    ['trailingPE', 'trailingPE', False, False, False, False],
                                    ['trailingAnnualDividendRate', 'ttmDividendRate', False, False, False, False],
                                ],
                            },
                            'financialData': {
                                'columnSets': [
                                    ['keySymbol', 'symbol', True, True, True, False],
                                    ['earningsGrowth', 'earningsGrowth', False, False, False, False],
                                    ['revenueGrowth', 'revenueGrowth', False, False, False, False],
                                    ['revenuePerShare', 'revenuePerShare', False, False, False, False],
                                ],
                            },
                        },
                    },
                },
            },
        },
        'price': {
            'info': 'ticker company profile information',
            'dataFrames': {
                'price': {
                    'scrapes': {
                        scrape.yahoo.QuoteSummary: {
                            'price': {
                                'columnSets': [
                                    ['keySymbol', 'symbol', True, True, True, False],
                                    ['timestamp', 'timestamp', False, False, False, True],
                                    ['regularMarketPrice', 'price', False, False, False, False],
                                    ['regularMarketOpen', 'open', False, False, False, False],
                                    ['regularMarketDayHigh', 'dayHigh', False, False, False, False],
                                    ['regularMarketDayLow', 'dayLow', False, False, False, False],
                                    ['regularMarketPreviousClose', 'previousClose', False, False, False, False],
                                    ['regularMarketTime', 'marketTime', False, False, False, True],
                                    ['regularMarketVolume', 'volume', False, False, False, False],
                                ],
                            },
                        },
                    },
                },
            },
        },
        'profile': {
            'info': 'ticker company profile information',
            'postProcs': [__addExchangeData],
            'dataFrames': {
                'profile': {
                    'postFunctions': ['merge'],
                    'scrapes': {
                        scrape.yahoo.QuoteSummary: {
                            'quoteType': {
                                'columnSets': [
                                    ['keySymbol', 'symbol', True, True, True, False],
                                    ['longName', 'name', False, False, False, False],
                                    ['quoteType', 'typeQuote', False, False, True, False],
                                ],
                            },
                            'summaryDetail': {
                                'columnSets': [
                                    ['keySymbol', 'symbol', True, True, True, False],
                                    ['currency', 'currency', False, False, False, False],
                                ],
                            },
                            'assetProfile': {
                                'columnSets': [
                                    ['keySymbol', 'symbol', True, True, True, False],
                                    ['sectorKey', 'sector', False, False, False, False],
                                    ['industryKey', 'industry', False, False, False, False],
                                    ['country', 'country', False, False, False, False],
                                    ['city', 'city', False, False, False, False],
                                    ['state', 'state', False, False, False, False],
                                ],
                            },
                        },
                        scrape.fmp.StockList: {
                            'stocklist': {
                                'columnSets': [
                                    ['keySymbol', 'symbol', True, True, True, False],
                                    ['exchangeShortName', 'exchange', False, False, False, False],
                                    ['type', 'type', False, False, True, False],
                                ],
                                'subTable': None,
                            },
                        },
                        scrape.polygon.Tickers: {
                            'tickers': {
                                'columnSets': [
                                    ['keySymbol', 'symbol', True, True, True, False],
                                    ['primary_exchange', 'mic', False, False, False, False],
                                    ['market', 'market', False, False, False, False],
                                    ['type', 'typeCode', False, False, True, False],
                                ],
                            },
                        },
                    },
                },
                'countries': {
                    'scrapes': {
                        scrape.saved.Saved: {
                            'ISO10383_MIC': {
                                'columnSets': [
                                    ['MIC', 'mic', True, False, False, False],
                                    ['ISO COUNTRY CODE (ISO 3166)', 'cc', False, False, False, False],
                                ],
                            },
                            'ISO3166-1': {
                                'columnSets': [
                                    ['Name', 'country', False, False, False, False],
                                    ['Code', 'cc', True, False, False, False],
                                ],
                            },
                        },
                    },
                },
            },
        },
        'quicken': {
            'info': 'quicken data',
            'dataFrames': {
                'transactions': {
                    'scrapes': {
                        scrape.saved.Saved: {
                            'QUICKEN_2020': {
                                'columnSets': [
                                    ['timestamp', 'timestamp', False, False, False, True],
                                    ['symbol', 'symbol', False, False, True, False],
                                    ['transaction', 'transaction', False, False, False, False],
                                    ['shares', 'shares', False, False, False, False],
                                    # ['*', 'ex', False, False, False, False],
                                ],
                                'subTable': None,
                            },
                        },
                    },
                },
            },
        },
        'history': {
            'info': 'history data for charts and history analysis',
            'data': {
                # param_name [scrape_class, table_name, column_name(if *, take all columns and make param_name the suffix), make upper]
                'price': [scrape.yahoo.Chart, 'indicators', 'close', False],
                'div': [scrape.yahoo.Chart, 'dividends', 'amount', False],
                'ind': [scrape.yahoo.Chart, 'indicators', '*', False],
            },
            'post': [__mergeDataFrames],
        },
    }

    def __getDatabaseData(self, catalog, symbols):
        catData =  self.__catalog[catalog]
        data = {}
        
        # get dataframes
        for dfName , dfData in catData['dataFrames'].items():
            # DataFrame creation
            dfTables = {}
            for scrapeClass, scrapeData in dfData['scrapes'].items():
                # access scrape database
                db = database.Database(scrapeClass.dbName)
                for tableName, tableData in scrapeData.items():
                    # access database table and create table DataFrame

                    # create columnsSet
                    # make_index, check_symbols, make upper
                    columns = {}
                    for columnSet in tableData['columnSets']:
                        searchColumn = columnSet[0]
                        makeColumn = columnSet[1]
                        if searchColumn == '*':
                            for columnName in db.getColumnNames(tableName):
                                if makeColumn != '':
                                    newColumnName = makeColumn + columnName.capitalize()
                                else:
                                    newColumnName = makeColumn
                                if not columnName in columns:
                                    columns[columnName] = []
                                columns[columnName].append([newColumnName] + columnSet[2:])
                        else:
                            if not searchColumn in columns:
                                columns[searchColumn] = []
                            columns[searchColumn].append(columnSet[1:])
                    dfSearch = db.getTableDataFrame(tableName, columns=list(columns.keys()))
                    
                    # build table DataFrame
                    dfTables[tableName] = pd.DataFrame(index=dfSearch.index)
                    indexColumns = set()
                    symbolsColumns = set()
                    for searchColumn, makeColumnSets in columns.items():
                        for makeColumnSet in makeColumnSets:
                            makeColumn = makeColumnSet[0]
                            makeUpper = makeColumnSet[3]
                            makeDatetime = makeColumnSet[4]
                            dfTables[tableName][makeColumn] = dfSearch[searchColumn]
                            if makeUpper:
                                dfTables[tableName][makeColumn] = dfTables[tableName][makeColumn].str.upper()
                            if makeDatetime:
                                dfTables[tableName][makeColumn] = pd.to_datetime(dfTables[tableName][makeColumn], unit='s').dt.tz_localize('US/Pacific')
                            makeIndex = makeColumnSet[1]
                            checkSymbols = makeColumnSet[2]
                            if makeIndex: indexColumns.add(makeColumn)
                            if checkSymbols: symbolsColumns.add(makeColumn)

                    
                    if len(symbolsColumns) == 1:
                        symbolsColumn = symbolsColumns.pop()
                        dfTables[tableName] = dfTables[tableName][dfTables[tableName][symbolsColumn].isin(symbols)]

                    if len(indexColumns) == 1:
                        indexColumn = indexColumns.pop()
                        dfTables[tableName].set_index(indexColumn, inplace=True,verify_integrity = True)
            
            data[dfName] = {}
            
            #handle post functions
            if 'postFunctions' in dfData:
                if 'merge' in dfData['postFunctions']:
                    tableNames = list(dfTables)
                    dfMerged = pd.DataFrame()
                    for tableName in tableNames:
                        dfTable = dfTables.pop(tableName)
                        dfMerged = pd.merge(dfMerged, dfTable, left_index=True, right_index=True, how='outer')
                    data[dfName]['merged'] = dfMerged
                else:
                    data[dfName] = dfTables
                
                if 'dropDuplicates' in dfData['postFunctions']:
                    for tableName , df in data[dfName].items():
                        df.drop_duplicates(inplace=True)
            else:
                data[dfName] = dfTables
        
        # run post procs
        if 'postProcs' in catData:
            for proc in catData['postProcs']:
                proc(data)

        return data
        
    def __init__(self):
        pass

    def update(self, symbols, catalogs, forceUpdate):
        # gather scrape classes and needed tables
        scrapeClasses = {}
        for catalog in catalogs:
            if catalog in self.__catalog:

                def recursedict(dictData):
                    if isinstance(dictData, dict):
                        for key, nextData in dictData.items():
                            if key == 'scrapes':
                                for scrape, scrapeData in nextData.items():
                                    if not scrape in scrapeClasses:
                                        scrapeClasses[scrape] = []
                                    scrapeClasses[scrape] += list(scrapeData.keys())
                                return
                            recursedict(nextData)
                recursedict(self.__catalog[catalog])
        
        # create scrapers and pass tables to update
        for scraperClass, tables in scrapeClasses.items():
            scraperClass(symbols, tables=tables, forceUpdate=forceUpdate)

    def getData(self, catalogs=[], symbols=[], update=False, forceUpdate=False):
        if update or forceUpdate: self.update(symbols, catalogs, forceUpdate=forceUpdate)
        data = {}
        for catalog in catalogs:
            dbdata = self.__getDatabaseData(catalog, symbols)
            # get list of DataFrames
            dfs = []
            def recursedict(dictData):
                if isinstance(dictData, dict):
                    for key, nextData in dictData.items():
                        recursedict(nextData)
                        return
                dfs.append(dictData)
            recursedict(dbdata)
            data[catalog] = dfs
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

    def getQuickenInvestments(self, withShares=True, update=False):
        investments = {}
        db = database.Database(scrape.saved.Saved.dbName)

        # find QUICKEN table
        db.getTableNames()
        tableNames = list(filter(lambda item: item.startswith('QUICKEN'), db.getTableNames()))
        if len(tableNames) == 0: return investments

        tableName = tableNames[0]

        # find all symbols
        values, params = db.getRows(tableName, columns=['symbol'])
        symbols = list(set([x[0] for x in values]))
        symbols = list(filter(lambda item: not ' ' in item, symbols))
        symbols = set(symbols)

        # find ingoung params and outgoing params
        values, params = db.getRows(tableName, columns=['transaction'])
        transactionParams = list(set([x[0] for x in values]))
        sharesInParams = set(list(filter(lambda item: item.startswith('Reinv'), transactionParams))+['Buy', 'ShrsIn'])
        sharesOutParams = set(['Sell', 'ShrsOut'])

        if withShares:
            # only get symbols that still are invested
            symbolsWithShares = set()
            for symbol in symbols:
                # add incoming shares and substract outgoing shares
                shares = 0
                for tParam in sharesInParams:
                    values, params = db.getRows(tableName, columns=['shares'], whereColumns=['symbol', 'transaction'], areValues=[symbol, tParam])
                    paramValues = [x[0] for x in values]
                    paramValues = list(filter(lambda item: item is not None, paramValues))
                    if len(paramValues) == 0: continue
                    shares += sum(paramValues)
                for tParam in sharesOutParams:
                    values, params = db.getRows(tableName, columns=['shares'], whereColumns=['symbol', 'transaction'], areValues=[symbol, tParam])
                    paramValues = [x[0] for x in values]
                    paramValues = list(filter(lambda item: item is not None, paramValues))
                    if len(paramValues) == 0: continue
                    shares -= sum(paramValues)
                
                # if we still have shares, we keep them
                if shares > 0.001:
                    symbolsWithShares.add(symbol)

            symbols = symbolsWithShares

        # get investment data of symbols
        for symbol in symbols:
            investments[symbol] = {}
            values, params = db.getRows(tableName, ['timestamp', 'transaction', 'shares', 'price', 'costBasis'],
                whereColumns=['symbol'], areValues=[symbol])
            
            for value in values:
                # skip the ones with no shares transaction
                if value[2] == None: continue
                data = investments[symbol][value[0]] = {}
                index = 1
                for param in params[1:]:
                    data[param] = value[index]
                    index += 1
        
        return investments
