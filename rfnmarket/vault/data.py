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

        return data
    
    __catalog = {
        'statistics': {
            'info': 'key ticker statistics',
            'data': {
                # param_name [scrape_class, table_name, column_name, make upper]
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
        # 'profile': {
        #     'info': 'ticker company profile information',
        #     'data': {
        #         # param_name [scrape_class, table_name, column_name, make upper]
        #         'name': [scrape.yahoo.QuoteSummary, 'quoteType', 'longName', False],
        #         'exchange': [scrape.fmp.StockList, 'stocklist', 'exchangeShortName', False],
        #         'mic': [scrape.polygon.Tickers, 'tickers', 'primary_exchange', False],
        #         'market': [scrape.polygon.Tickers, 'tickers', 'market', False],
        #         'type': [scrape.fmp.StockList, 'stocklist', 'type', True],
        #         'typeCode': [scrape.polygon.Tickers, 'tickers', 'type', False],
        #         'typeQuote': [scrape.yahoo.QuoteSummary, 'quoteType', 'quoteType', False],
        #         'currency': [scrape.yahoo.QuoteSummary, 'summaryDetail', 'currency', False],
        #         'sector': [scrape.yahoo.QuoteSummary, 'assetProfile', 'sectorKey', False],
        #         'industry': [scrape.yahoo.QuoteSummary, 'assetProfile', 'industryKey', False],
        #         'country': [scrape.yahoo.QuoteSummary, 'assetProfile', 'country', False],
        #         'city': [scrape.yahoo.QuoteSummary, 'assetProfile', 'city', False],
        #         'state': [scrape.yahoo.QuoteSummary, 'assetProfile', 'state', False],
        #     },
        #     'post': [__addExchangeData],
        # },
        'price': {
            'info': 'just the latest price',
            'data': {
                # param_name [scrape_class, table_name, column_name, make upper]
                'timestamp': [scrape.yahoo.QuoteSummary, 'price', 'timestamp', False],
                'marketTime': [scrape.yahoo.QuoteSummary, 'price', 'regularMarketTime', False],
                'price': [scrape.yahoo.QuoteSummary, 'price', 'regularMarketPrice', False],
                'open': [scrape.yahoo.QuoteSummary, 'price', 'regularMarketOpen', False],
                'dayHigh': [scrape.yahoo.QuoteSummary, 'price', 'regularMarketDayHigh', False],
                'dayLow': [scrape.yahoo.QuoteSummary, 'price', 'regularMarketDayLow', False],
                'previousClose': [scrape.yahoo.QuoteSummary, 'price', 'regularMarketPreviousClose', False],
                'volume': [scrape.yahoo.QuoteSummary, 'price', 'regularMarketVolume', False],
            },
            'post': [],
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
        'profile': {
            'info': 'ticker company profile information',
            'dataFrames': {
                'myData': {
                    'postFunctions': ['merge'],
                    'scrapes': {
                        scrape.yahoo.QuoteSummary: {
                            'quoteType': {
                                'columnSets': [
                                    ['keySymbol', 'symbol', True, True, True],
                                    ['longName', 'name', False, False, False],
                                    ['quoteType', 'typeQuote', False, False, True],
                                ],
                                'subTable': None,
                            },
                            'summaryDetail': {
                                'columnSets': [
                                    ['keySymbol', 'symbol', True, True, True],
                                    ['currency', 'currency', False, False, False],
                                ],
                                'subTable': None,
                            },
                            'assetProfile': {
                                'columnSets': [
                                    ['keySymbol', 'symbol', True, True, True],
                                    ['sectorKey', 'sector', False, False, False],
                                    ['industryKey', 'industry', False, False, False],
                                    ['country', 'country', False, False, False],
                                    ['city', 'city', False, False, False],
                                    ['state', 'state', False, False, False],
                                ],
                                'subTable': None,
                            },
                        },
                        scrape.fmp.StockList: {
                            'stocklist': {
                                'columnSets': [
                                    ['keySymbol', 'symbol', True, True, True],
                                    ['exchangeShortName', 'exchange', False, False, False],
                                    ['type', 'type', False, False, True],
                                ],
                                'subTable': None,
                            },
                        },
                        scrape.polygon.Tickers: {
                            'tickers': {
                                'columnSets': [
                                    ['keySymbol', 'symbol', True, True, True],
                                    ['primary_exchange', 'mic', False, False, False],
                                    ['market', 'market', False, False, False],
                                    ['type', 'typeCode', False, False, True],
                                ],
                                'subTable': None,
                            },
                        },
                    },
                },
            },
            'post': [],
        },
        'quicken': {
            'info': 'quicken data',
            'dataFrames': {
                'transactions': {
                    'scrapes': {
                        scrape.saved.Saved: {
                            'QUICKEN_2020': {
                                'columnSets': [
                                    ['symbol', 'symbol', False, False, True],
                                    ['transaction', 'transaction', False, False, False],
                                    ['shares', 'shares', False, False, False],
                                    # ['*', 'ex', False, False, False],
                                ],
                                'subTable': None,
                            },
                        },
                    },
                },
                'stocks': {
                    'postFunctions': ['dropDuplicates'],
                    'scrapes': {
                        scrape.saved.Saved: {
                            'QUICKEN_2020': {
                                'columnSets': [
                                    ['symbol', 'symbol', False, False, True],
                                    ['security', 'security', False, False, False],
                                ],
                                'subTable': None,
                            },
                        },
                    },
                },
            },
            'post': [],
        },
    }
    # # column_name: [column_search, make_index, check_symbols, make upper, scrape_class, table_name, sub_table_name]
    # # column_name: final name of the column
    # # column_search: column name of the querried parameter
    # #              if value is '*' take all columns and capitalise name if param_name id not empty
    # #              then add param_name as suffix
    # # scrape_class: scrape class to retrieve data from
    # # table_name: table name to be searched
    # # sub_table_name: sub table name to be searched
    # # make_index: make parameter the index and make it unique
    # # check_symbols: cross check with symbols
    # # make_upper_case: make this data upper case
    # 'symbols': ['symbol', False, False, True, None, 'QUICKEN_2020', scrape.saved.Saved],
    # 'trs': ['transaction', False, False, False, None, 'QUICKEN_2020', scrape.saved.Saved],
    # 'shrs': ['shares', False, False, False, None, 'QUICKEN_2020', scrape.saved.Saved],
 
    def __getTimeTables(self, params, symbols, dbName, catalog):
        # print(params)
        # print(symbols)
        # print(dbName)

        # create params sets for each dataTable
        dataTables = {}
        for paramsSet in params:
            tableName = paramsSet[0]
            searchParam = paramsSet[1]
            targetParam = paramsSet[-1]
            if not tableName in dataTables:
                dataTables[tableName] = []
            dataTables[tableName].append([searchParam, targetParam])

        db = database.Database(dbName)
        data = {}
        for dataTable, paramSets in dataTables.items():
            # get dataframe of tables with symbols we need
            dfdt = db.getTableDataFrame(dataTable)
            dfdt = dfdt[dfdt['keySymbol'].isin(symbols)]
            # iterate through symbols and table
            for row in dfdt.iterrows():
                symbol = row[1].keySymbol
                if not symbol in data:
                    data[symbol] = {}
                tableName = row[1].tableName
                dftn = db.getTableDataFrame(tableName)
                dftn['timestamp'] = pd.to_datetime(dftn['timestamp'], unit='s').dt.tz_localize('US/Pacific')
                dftn.set_index('timestamp', inplace=True)
                dfcreate = pd.DataFrame()
                dfcreate.index = dftn.index
                for paramSet in paramSets:
                    findColumn = paramSet[0]
                    makeColumn = paramSet[1]
                    if findColumn == '*':
                        for columnName in dftn.columns:
                            newColumnName = makeColumn+columnName.capitalize()
                            dfcreate[newColumnName] = dftn[columnName]
                    else:
                        if findColumn in dftn.columns:
                            dfcreate[makeColumn] = dftn[findColumn]
                data[symbol][dataTable] = dfcreate
            
        for proc in self.__catalog[catalog]['post']:
            data = proc(data)

        return data

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

    # 'test': {
    #     'info': 'for testing only',
    #     'dataFrames': {
    #         'myData': {
    #             'merge': False,
    #             'scrapes': {
    #                 scrape.saved.Saved: {
    #                     'QUICKEN_2020': {
    #                         'columnSets': [
    #                             ['symbol', 'symbols', False, False, True],
    #                             ['transaction', 'trs', False, False, False],
    #                             ['shares', 'shrs', False, False, False],
    #                             # ['*', 'ex', False, False, False],
    #                         ],
    #                         'subTable': None,
    #                     },
    #                 },
    #             },
    #         },
    #     },
    #     'post': [],
    # },

    def __getDatabaseDataNew(self, catalog, symbols):
        catData =  self.__catalog[catalog]['dataFrames']
        
        data = {}
        for dfName , dfData in catData.items():
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
                            dfTables[tableName][makeColumn] = dfSearch[searchColumn]
                            if makeUpper:
                                dfTables[tableName][makeColumn] = dfTables[tableName][makeColumn].str.upper()
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

                        
                    # dftn['timestamp'] = pd.to_datetime(dftn['timestamp'], unit='s').dt.tz_localize('US/Pacific')
            
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
            elif scrapeClass == scrape.yahoo.Chart:
                return self.__getTimeTables(params, symbols, scrapeClass.dbName, catalog)

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
    
    def update(self, symbols, catalogs, forceUpdate):
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
            scraperClass(symbols, tables=tables, forceUpdate=forceUpdate)

    def getDataNew(self, catalogs, symbols):
        data = {}
        for catalog in catalogs:
            dbdata = self.__getDatabaseDataNew(catalog, symbols)
            data[catalog] = dbdata
        print()
        print('Result:')
        pp(data)

    def getData(self, symbols, catalogs, update=False, forceUpdate=False):
        if update or forceUpdate: self.update(symbols, catalogs, forceUpdate=forceUpdate)
        
        data = {}
        for catalog in catalogs:
            dbdata = self.__getDatabaseData(symbols, catalog)
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
