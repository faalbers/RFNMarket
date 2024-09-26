import pandas as pd
from pprint import pp
from ..utils import database, log
from .. import scrape
from datetime import datetime
import copy

class Data():
    def __init__(self):
        self.databases = {}
    
    def getScrapeDB(self, scrapeClass):
        if not scrapeClass in self.databases:
            self.databases[scrapeClass] = database.Database(scrapeClass.dbName)
        return self.databases[scrapeClass]

    def updateData(self, catalogs=[], symbols=[], forceUpdate=False):
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

    def getData(self, catalogs=[], symbols=[], update=False, forceUpdate=False, catalogDB=None):
        if update or forceUpdate: self.updateData(catalogs, symbols, forceUpdate=forceUpdate)
        data = {}
        
        for catalog in catalogs:
            if catalogDB != None:
                catData =  catalogDB[catalog]
            else:
                catData =  self.__catalog[catalog]
            dbdata = {}
            
            # get dataframes
            for dfName , dfData in catData['dataFrames'].items():
                # DataFrame creation
                dfTables = {}
                for scrapeClass, scrapeData in dfData['scrapes'].items():
                    # access scrape database
                    db = self.getScrapeDB(scrapeClass)
                    for tableName, tableData in scrapeData.items():
                        # if tableName is * we get all available table names from that scrape class
                        scrapeTableNames = scrapeClass.getTableNames(tableName)
                        for tableName in scrapeTableNames:
                            if not db.tableExists(tableName): continue
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
                                            newColumnName = columnName
                                        if not columnName in columns:
                                            columns[columnName] = []
                                        columns[columnName].append([newColumnName] + columnSet[2:])
                                else:
                                    if not searchColumn in columns:
                                        columns[searchColumn] = []
                                    columns[searchColumn].append(columnSet[1:])
                            dfSearch = db.getTable(tableName, columns=list(columns.keys()))
                            
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
                dbdata[dfName] = {}
                
                # run tables post procs
                if 'postProcs' in dfData:
                    for proc in dfData['postProcs']:
                        dbdata[dfName] = proc(self, dfTables)
                    # if 'merge' in dfData['postFunctions']:
                    #     tableNames = list(dfTables)
                    #     dfMerged = pd.DataFrame()
                    #     for tableName in tableNames:
                    #         dfTable = dfTables.pop(tableName)
                    #         dfMerged = pd.merge(dfMerged, dfTable, left_index=True, right_index=True, how='outer')
                    #     dbdata[dfName]['merged'] = dfMerged
                    # else:
                    #     dbdata[dfName] = dfTables
                    
                    # if 'dropDuplicates' in dfData['postFunctions']:
                    #     for tableName , df in dbdata[dfName].items():
                    #         df.drop_duplicates(inplace=True)
                else:
                    dbdata[dfName] = dfTables
            
            # run dataframes post procs
            if 'postProcs' in catData:
                for proc in catData['postProcs']:
                    data[catalog] = proc(self, dbdata)
            else:
                data[catalog] = dbdata
            
        return data

    def getCatalog(self):
        catalog = {}
        for cat, data in self.__catalog.items():
            catalog[cat] = data['info']
        return catalog
    
    def getQuickenInvestments(self, withShares=True, update=False):
        investments = {}
        db = self.getScrapeDB(scrape.saved.Saved)

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

    # post procs
    @staticmethod
    def __findUSExchangeSymbols(self, data):
        # get all mics and acronyms that have US country code
        mics = data['mic']['ISO10383_MIC']
        usacronyms = list(set(mics.loc[mics['cc'] == 'US']['acronym'].dropna()))
        usmics = list(set(mics.loc[mics['cc'] == 'US']['mic'].dropna()))
        
        # add country united States to a new column if:
        # mic is in usmics
        # acronym is in usacronyms
        profile = data['profile']['merged']
        profile.loc[ profile['mic'].isin(usmics) , 'country' ] = 'United States'
        profile.loc[ profile['acronym'].isin(usacronyms) , 'country' ] = 'United States'
        
        # select all symbols that have country set and get the symbols index
        return list(profile['country'].dropna().index)

    @staticmethod
    def __addExchangeData(self, data):
        # get all mics and acronyms that have US country code
        mics = data.pop('mic')['ISO10383_MIC']
        usexchange = list(set(mics.loc[mics['cc'] == 'US']['exchange'].dropna()))
        usmics = list(set(mics.loc[mics['cc'] == 'US']['mic'].dropna()))

        # add country united States to a new column if:
        # mic is in usmics
        # exchange is in usexchange
        profile = data['profile']['merged']
        profile.loc[ profile['mic'].isin(usmics) , 'exchangeCountry' ] = 'United States'
        profile.loc[ profile['exchange'].isin(usexchange) , 'exchangeCountry' ] = 'United States'

        return data

    @staticmethod
    def __getChartData(self, data):
        catalogDB = {
            'mychart': {
                'dataFrames': {
                },
            },
        }
        dataframe = {
            'postFunctions': ['merge'],
            'scrapes': {
                scrape.yahoo.Chart: {
                },
            },
        }
        tableTypes = {
            'indicators': {
                'columnSets': [
                    ['timestamp', 'timestamp', True, False, False, True],
                    ['open', 'open', False, False, False, False],
                    ['open', 'close', False, False, False, False],
                    ['adjclose', 'adjclose', False, False, False, False],
                    ['high', 'high', False, False, False, False],
                    ['low', 'low', False, False, False, False],
                    ['volume', 'volume', False, False, False, False],
                ],
            },
            'dividends': {
                'columnSets': [
                    ['timestamp', 'timestamp', True, False, False, True],
                    ['amount', 'dividend', False, False, False, False],
                ],
            },
            'capitalGains': {
                'columnSets': [
                    ['timestamp', 'timestamp', True, False, False, True],
                    ['amount', 'capitalGain', False, False, False, False],
                ],
            },
            'splits': {
                'columnSets': [
                    ['timestamp', 'timestamp', True, False, False, True],
                    ['denominator', 'denominator', False, False, False, False],
                    ['numerator', 'numerator', False, False, False, False],
                    ['splitRatio', 'splitRatio', False, False, False, False],
                ],
            },
        }
        dfTables = data['chart']['merged']

        # create catalogDB
        catalogDB = {
            'mychart': {
                'dataFrames': {
                },
            },
        }
        for symbol, row in dfTables.iterrows():
            sdataframe = copy.deepcopy(dataframe)
            for tableType in row.index:
                tableName = row[tableType]
                if tableName == tableName:
                    sdataframe['scrapes'][scrape.yahoo.Chart][tableName] = tableTypes[tableType].copy()
            catalogDB['mychart']['dataFrames'][symbol] = sdataframe

        mychartData = self.getData(['mychart'], catalogDB=catalogDB)

        return mychartData
    
    @staticmethod
    def __mergeDataFrames(self, data):
        tableNames = list(data.keys())
        dfMerged = pd.DataFrame()
        for tableName in tableNames:
            dfTable = data[tableName]
            dfMerged = pd.merge(dfMerged, dfTable, left_index=True, right_index=True, how='outer')
        return {'merged': dfMerged}
        
        # if 'dropDuplicates' in dfData['postFunctions']:
        #     for tableName , df in dbdata[dfName].items():
        #         df.drop_duplicates(inplace=True)
    
    __catalog = {
        # columnSets: example: ['keySymbol', 'symbol', True, True, True]
        # [column_search, column_name, make_index, check_symbols, make_upper, make_datetime]
        # column_search: column name of the querried column
        #              if value is '*' take all columns and capitalise name if column_name is not empty
        #              then add column_name as suffix
        # column_name: final name of the column
        # make_index: make parameter the index and make it unique
        # check_symbols: cross check with symbols
        # make_upper: make this data upper case
        # make_datetime: turn collumn timestamps into Datetime
        'test': {
            'info': 'ticker traded in us markets',
            'dataFrames': {
                'profile': {
                    'scrapes': {
                        scrape.yahoo.TimeSeries: {
                            # 'trailingBasicEPS': {
                            #     'columnSets': [
                            #         ['keySymbol', 'symbol', True, False, True, False],
                            #     ],
                            # },
                            # 'annualBasicEPS': {
                            #     'columnSets': [
                            #         ['keySymbol', 'symbol', True, False, True, False],
                            #     ],
                            # },
                            # 'quarterlyBasicEPS': {
                            #     'columnSets': [
                            #         ['keySymbol', 'symbol', True, False, True, False],
                            #     ],
                            # },
                            'quarterlyStockholdersEquity': {
                                'columnSets': [
                                    ['keySymbol', 'symbol', True, False, True, False],
                                ],
                            },
                        },
                    },
                },
            },
        },
        'ussymbols': {
            'info': 'ticker traded in us markets',
            'postProcs': [__findUSExchangeSymbols],
            'dataFrames': {
                'profile': {
                    'postProcs': [__mergeDataFrames],
                    'scrapes': {
                        scrape.fmp.StockList: {
                            'stocklist': {
                                'columnSets': [
                                    ['keySymbol', 'symbol', True, False, True, False],
                                    ['exchangeShortName', 'acronym', False, False, False, False],
                                    # ['type', 'type', False, False, True, False],
                                ],
                            },
                        },
                        scrape.polygon.Tickers: {
                            'tickers': {
                                'columnSets': [
                                    ['keySymbol', 'symbol', True, False, True, False],
                                    ['primary_exchange', 'mic', False, False, False, False],
                                ],
                            },
                        },
                    },
                },
                'mic': {
                    'scrapes': {
                        scrape.saved.Saved: {
                            'ISO10383_MIC': {
                                'columnSets': [
                                    ['MIC', 'mic', False, False, False, False],
                                    ['ACRONYM', 'acronym', False, False, False, False],
                                    ['ISO COUNTRY CODE (ISO 3166)', 'cc', False, False, False, False],
                                ],
                            },
                        },
                    },
                },
            },
        },
        'statistics': {
            'info': 'ticker company profile information',
            'dataFrames': {
                'statistics': {
                    'postProcs': [__mergeDataFrames],
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
                    'postProcs': [__mergeDataFrames],
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
                'mic': {
                    'scrapes': {
                        scrape.saved.Saved: {
                            'ISO10383_MIC': {
                                'columnSets': [
                                    ['MIC', 'mic', False, False, False, False],
                                    ['ACRONYM', 'exchange', False, False, False, False],
                                    ['ISO COUNTRY CODE (ISO 3166)', 'cc', False, False, False, False],
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
                            },
                        },
                    },
                },
            },
        },
        'chart': {
            'info': 'chart data',
            'postProcs': [__getChartData],
            'dataFrames': {
                'chart': {
                    'postProcs': [__mergeDataFrames],
                    'scrapes': {
                        scrape.yahoo.Chart: {
                            'indicators': {
                                'columnSets': [
                                    ['keySymbol', 'symbol', True, True, True, False],
                                    ['tableName', 'indicators', False, False, False, False],
                                ],
                            },
                            'dividends': {
                                'columnSets': [
                                    ['keySymbol', 'symbol', True, True, True, False],
                                    ['tableName', 'dividends', False, False, False, False],
                                ],
                            },
                            'splits': {
                                'columnSets': [
                                    ['keySymbol', 'symbol', True, True, True, False],
                                    ['tableName', 'splits', False, False, False, False],
                                ],
                            },
                            'capitalGains': {
                                'columnSets': [
                                    ['keySymbol', 'symbol', True, True, True, False],
                                    ['tableName', 'capitalGains', False, False, False, False],
                                ],
                            },
                        },
                    },
                },
            },
        },
        'all': {
            'info': 'all avalable database data',
            'dataFrames': {
                'QuoteSummary': {
                    'scrapes': {
                        scrape.yahoo.QuoteSummary: {
                            'all': {
                                'columnSets': [
                                    ['*', '', False, False, False, False],
                                ],
                            },
                        },
                    },
                },
                'Chart': {
                    'scrapes': {
                        scrape.yahoo.Chart: {
                            'all': {
                                'columnSets': [
                                    ['*', '', False, False, False, False],
                                ],
                            },
                        },
                    },
                },
                'StockList': {
                    'scrapes': {
                        scrape.fmp.StockList: {
                            'all': {
                                'columnSets': [
                                    ['*', '', False, False, False, False],
                                ],
                            },
                        },
                    },
                },
                'Tickers': {
                    'scrapes': {
                        scrape.polygon.Tickers: {
                            'all': {
                                'columnSets': [
                                    ['*', '', False, False, False, False],
                                ],
                            },
                        },
                    },
                },
                'Saved': {
                    'scrapes': {
                        scrape.saved.Saved: {
                            'all': {
                                'columnSets': [
                                    ['*', '', False, False, False, False],
                                ],
                            },
                        },
                    },
                },
            },
        },
    }
