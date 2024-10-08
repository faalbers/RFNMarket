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

    def closeScrapeDB(self, scrapeClass):
        if scrapeClass in self.databases:
            self.databases.pop(scrapeClass)
    
    def closeAllScrapeDB(self):
        scrapeClasses = list(self.databases.keys())
        for scrapeClass in scrapeClasses:
            self.closeScrapeDB(scrapeClass)

    def updateData(self, catalogs=[], keyValues=[], forceUpdate=False):
        # gather scrape classes and needed tables
        scraperClasses = []
        for catalog in catalogs:
            if catalog in self.__catalog:
                for dataFrame, dfData in self.__catalog[catalog]['dataFrames'].items():
                    for scraperClass, scraperData in dfData['scrapes'].items():
                        scraperClasses.append((scraperClass, list(scraperData.keys())))

        for ssData in scraperClasses:
            scraperClass = ssData[0]
            tableNames = []
            for tableName in ssData[1]:
                tableNames += scraperClass.getTableNames(tableName)
            tableNames = list(set(tableNames))
            scraperClass(keyValues, tables=tableNames, forceUpdate=forceUpdate)
    
    def getData(self, catalogs=[], keyValues=[], update=False, forceUpdate=False, catalogDB=None):
        if update or forceUpdate: self.updateData(catalogs, keyValues, forceUpdate=forceUpdate)
        mainData = {}
        for catalog in catalogs:
            if catalogDB != None:
                catData =  catalogDB[catalog]
            else:
                catData =  self.__catalog[catalog]
            
            # get data sets
            setsData = {}
            for setName , setData in catData['sets'].items():
                tablesData = {}
                for scrapeClass, scrapeData in setData['scrapes'].items():
                    # access scrape database
                    db = self.getScrapeDB(scrapeClass)
                    for tableName, tableData in scrapeData.items():
                        scrapeTableNames = scrapeClass.getTableNames(tableName)
                        handleKeyValues = tableData['keyValues']
                        for tableName in scrapeTableNames:
                            columns = {}
                            for columnSet in tableData['columnSettings']:
                                searchColumn = columnSet[0]
                                makeColumn = columnSet[1]
                                if searchColumn == 'all':
                                    for columnName in db.getTableColumnNames(tableName):
                                        if makeColumn != '':
                                            newColumnName = makeColumn + columnName.capitalize()
                                        else:
                                            newColumnName = columnName
                                        if not columnName in columns:
                                            columns[columnName] = {}
                                        columns[columnName]['newName'] = newColumnName
                                        columns[columnName]['settings'] = columnSet[2:]
                                else:
                                    if not searchColumn in columns:
                                        columns[searchColumn] = {}
                                    columns[searchColumn]['newName'] = makeColumn
                                    columns[searchColumn]['settings'] = columnSet[2:]
                            
                            # get table data
                            foundData = db.tableRead(tableName, keyValues, list(columns.keys()), handleKeyValues=handleKeyValues)
                            # skip if no data found
                            if len(foundData) == 0: continue

                            # make data
                            if handleKeyValues:
                                makeData = {}
                                for keyValue, keyData in foundData.items():
                                    newKeyData = {}
                                    for searchColumn, columnSettings in columns.items():
                                        if not searchColumn in keyData: continue
                                        newKeyData[columnSettings['newName']] = keyData[searchColumn]
                                    if len(newKeyData) > 0:
                                        makeData[keyValue] = newKeyData
                            else:
                                makeData = []
                                for rowData in foundData:
                                    newRowData = {}
                                    for searchColumn, columnSettings in columns.items():
                                        if not searchColumn in rowData: continue
                                        newRowData[columnSettings['newName']] = rowData[searchColumn]
                                    if len(newRowData) > 0:
                                        makeData.append(newRowData)
                            
                            if len(makeData) > 0:
                                tablesData[tableName] = makeData
                # run sets post procs
                if 'postProcs' in setData:
                    for procEntry in setData['postProcs']:
                        proc = procEntry[0]
                        procParams = procEntry[1]
                        setsData[setName] = proc(self, tablesData, **procParams)
                else:
                    setsData[setName] = tablesData
            
            # run setcatalogs post procs
            if 'postProcs' in catData:
                for procEntry in catData['postProcs']:
                    proc = procEntry[0]
                    procParams = procEntry[1]
                    mainData[catalog] = proc(self, setsData, **procParams)
            else:
                mainData[catalog] = setsData
                    
        self.closeAllScrapeDB()
        return mainData
    
    def getCatalog(self):
        catalog = {}
        for cat, data in self.__catalog.items():
            catalog[cat] = data['info']
        return catalog
    
    def getQuickenInvestments(self, withShares=True, update=False):
        investments = {}
        db = self.getScrapeDB(scrape.saved.Saved)

        # get Quicken data
        dfQuicken = db.getTableDF('QUICKEN_2020')
        if len(dfQuicken) == 0: return investments


        # find all symbols
        symbols = list(dfQuicken['symbol'])
        symbols = list(filter(lambda item: not ' ' in item, symbols))
        symbols = set(symbols)

        # find ingoung params and outgoing params
        transactionParams = list(dfQuicken['transaction'])
        sharesInParams = set(list(filter(lambda item: item.startswith('Reinv'), transactionParams))+['Buy', 'ShrsIn'])
        sharesOutParams = set(['Sell', 'ShrsOut'])

        if withShares:
            # only get symbols that still are invested
            symbolsWithShares = set()
            for symbol in symbols:
                # add incoming shares and substract outgoing shares
                shares = 0
                for tParam in sharesInParams:
                    # values, params = db.getRows(tableName, columns=['shares'], whereColumns=['symbol', 'transaction'], areValues=[symbol, tParam])
                    foundData = dfQuicken[dfQuicken['symbol'] == symbol]
                    foundData = foundData[foundData['transaction'] == tParam]
                    shares += foundData['shares'].sum()
                for tParam in sharesOutParams:
                    foundData = dfQuicken[dfQuicken['symbol'] == symbol]
                    foundData = foundData[foundData['transaction'] == tParam]
                    shares -= foundData['shares'].sum()
                
                # if we still have shares, we keep them
                if shares > 0.001:
                    symbolsWithShares.add(symbol)

            symbols = symbolsWithShares

        # get investment data of symbols
        for symbol in symbols:
            foundData = dfQuicken[dfQuicken['symbol'] == symbol][['timestamp', 'transaction', 'shares', 'price', 'costBasis']]
            foundData.set_index('timestamp', inplace=True)
            investments[symbol] = foundData
        
        return investments

    # post procs
    @staticmethod
    def __findUSExchangeSymbols(self, data):
        # get all mics and acronyms that have US country code
        mics = data['mic']['ISO10383_MIC']
        usacronyms = set()
        usmics = set()
        for micRow in mics:
            if 'cc' in micRow and micRow['cc'] == 'US':
                usmics.add(micRow['mic'])
                if 'acronym' in micRow:
                    usacronyms.add(micRow['acronym'])
        
        keyValues = set()
        for keyValue, keyData in data['profile'].items():
            if 'mic' in keyData and keyData['mic'] in usmics:
                keyValues.add(keyValue)
            if 'acronym' in keyData and keyData['acronym'] in usacronyms:
                keyValues.add(keyValue)
        
        return list(keyValues)

    @staticmethod
    def __addExchangeData(self, data):
        # get all mics and acronyms that have US country code
        mics = data.pop('mic')['ISO10383_MIC']
        usexchange = list(set(mics.loc[mics['cc'] == 'US']['exchange'].dropna()))
        usmics = list(set(mics.loc[mics['cc'] == 'US']['mic'].dropna()))

        # add country united States to a new column if:
        # mic is in usmics
        # exchange is in usexchange
        
        # profile = data['profile']['merged']
        # profile.loc[ profile['mic'].isin(usmics) , 'exchangeCountry' ] = 'United States'
        # profile.loc[ profile['exchange'].isin(usexchange) , 'exchangeCountry' ] = 'United States'

        return data

    @staticmethod
    def __getTimeSeries(self, data):
        dfsTimeTables = {}
        for tableName, tableData in data.items():
            scrapeClass = tableData['scrapeClass']
            dfTableNames = tableData['df']
            db = self.getScrapeDB(scrapeClass)
            for symbol, row in dfTableNames.iterrows():
                symbolTimeTables = {}
                for tableName in row.index:
                    tableReference = row[tableName]
                    dfTable = pd.read_sql("SELECT * FROM '%s'" % tableReference, db.getConnection())
                    # dfTable['timestamp'] = pd.to_datetime(dfTable['timestamp'], unit='s').dt.tz_localize('US/Pacific')
                    dfTable.set_index('timestamp', inplace=True)
                    symbolTimeTables[tableName] = dfTable
                if len(symbolTimeTables) > 0:
                    dfsTimeTables[symbol] = symbolTimeTables

        return dfsTimeTables

    @staticmethod
    def __mergeTables(self, data, mergeName=None):
        merged = {}
        for tableName, tableData in data.items():
            for keyValue, keyData in tableData.items():
                if not keyValue in merged:
                    merged[keyValue] = {}
                merged[keyValue] = {**merged[keyValue], **keyData}
        if mergeName == None:
            return merged
        else:
            return {mergeName: merged}
    
    @staticmethod
    def __mergeDataFrames(self, data):
        dfMerged = pd.DataFrame()
        for tableName, tableData in data.items():
            dfTable = tableData['df']
            dfMerged = pd.merge(dfMerged, dfTable, left_index=True, right_index=True, how='outer')
        return {'merged': dfMerged}
        
        # if 'dropDuplicates' in dfData['postFunctions']:
        #     for tableName , df in dbdata[dfName].items():
        #         df.drop_duplicates(inplace=True)
    
    __catalog = {
        # columnSets: example: ['keySymbol', 'symbol', False, False, False, False]
        # [column_search, column_name, make_index, check_symbols, make_upper, make_datetime]
        # columnSets: example: ['trailingEps', 'teps']
        # [column_search, column_name]
        # column_search: column name of the querried column
        #              if value is '*' take all columns and capitalise name if column_name is not empty
        #              then add column_name as suffix
        # column_name: final name of the column
        # make_index: make parameter the index and make it unique
        # check_symbols: cross check with symbols
        # make_upper: make this data upper case
        # make_datetime: turn collumn timestamps into Datetime
        'test': {
            'info': 'ticker company profile information',
            'sets': {
                'statistics': {
                    'postProcs': [[__mergeTables, {'mergeName': 'quoteSummary'}]],
                    'scrapes': {
                        scrape.yahoo.QuoteSummary: {    # scrape class to retrieve data from
                            'defaultKeyStatistics': {   # table name to be searched
                                'keyValues': True,
                                'columnSettings': [
                                    ['trailingEps', 'eps', {}],
                                    ['forwardEps', 'feps', {}],
                                    ['pegRatio', 'pratio', {}],
                                ],
                            },
                            'summaryDetail': {
                                'keyValues': True,
                                'columnSettings': [
                                    ['trailingPE', 'trailingPE', {}],
                                    ['trailingAnnualDividendRate', 'ttmDividendRate', {}],
                                ],
                            },
                            'financialData': {
                                'keyValues': True,
                                'columnSettings': [
                                    ['earningsGrowth', 'earningsGrowth', {}],
                                    ['revenueGrowth', 'revenueGrowth', {}],
                                    ['revenuePerShare', 'revenuePerShare', {}],
                                ],
                            },
                        },
                    },
                },
            },
        },
        'ussymbols': {
            'info': 'ticker traded in us markets',
            'postProcs': [[__findUSExchangeSymbols, {}]],
            'sets': {
                'profile': {
                    'postProcs': [[__mergeTables, {}]],
                    'scrapes': {
                        scrape.fmp.StockList: {
                            'stocklist': {
                                'keyValues': True,
                                'columnSettings': [
                                    ['exchangeShortName', 'acronym', {}],
                                    # ['type', 'type', False, False, True, False],
                                ],
                            },
                        },
                        scrape.polygon.Tickers: {
                            'tickers': {
                                'keyValues': True,
                                'columnSettings': [
                                    ['primary_exchange', 'mic'],
                                ],
                            },
                        },
                    },
                },
                'mic': {
                    'scrapes': {
                        scrape.saved.Saved: {
                            'ISO10383_MIC': {
                                'keyValues': False,
                                'columnSettings': [
                                    ['MIC', 'mic', {}],
                                    ['ACRONYM', 'acronym', {}],
                                    ['ISO COUNTRY CODE (ISO 3166)', 'cc', {}],
                                ],
                            },
                        },
                    },
                },
            },
        },
        'qtest': {
            'info': 'just a test, remove latern',
            'dataFrames': {
                'statistics': {
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
            # 'postProcs': [__getTimeSeries],
            'dataFrames': {
                'chart': {
                    'postProcs': [__getTimeSeries],
                    'scrapes': {
                        scrape.yahoo.Chart: {
                            'table_reference': {
                                'columnSets': [
                                    ['keySymbol', 'symbol', True, True, True, False],
                                    ['chart', 'chart', False, False, False, False],
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
                # 'all_quarterly_financials': {
                #     'scrapes': {
                #         scrape.yahoo.TimeSeries: {
                #             'all_quarterly_financials': {
                #                 'columnSets': [
                #                     ['*', '', False, False, False, False],
                #                 ],
                #             },
                #         },
                #     },
                # },
                # 'all_annual_financials': {
                #     'scrapes': {
                #         scrape.yahoo.TimeSeries: {
                #             'all_annual_financials': {
                #                 'columnSets': [
                #                     ['*', '', False, False, False, False],
                #                 ],
                #             },
                #         },
                #     },
                # },
                # 'all_trailing_financials': {
                #     'scrapes': {
                #         scrape.yahoo.TimeSeries: {
                #             'all_trailing_financials': {
                #                 'columnSets': [
                #                     ['*', '', False, False, False, False],
                #                 ],
                #             },
                #         },
                #     },
                # },
                # 'all_quarterly_balanceSheet': {
                #     'scrapes': {
                #         scrape.yahoo.TimeSeries: {
                #             'all_quarterly_balanceSheet': {
                #                 'columnSets': [
                #                     ['*', '', False, False, False, False],
                #                 ],
                #             },
                #         },
                #     },
                # },
                # 'all_annual_balanceSheet': {
                #     'scrapes': {
                #         scrape.yahoo.TimeSeries: {
                #             'all_annual_balanceSheet': {
                #                 'columnSets': [
                #                     ['*', '', False, False, False, False],
                #                 ],
                #             },
                #         },
                #     },
                # },
                # 'all_quarterly_cashFlow': {
                #     'scrapes': {
                #         scrape.yahoo.TimeSeries: {
                #             'all_quarterly_cashFlow': {
                #                 'columnSets': [
                #                     ['*', '', False, False, False, False],
                #                 ],
                #             },
                #         },
                #     },
                # },
                # 'all_annual_cashFlow': {
                #     'scrapes': {
                #         scrape.yahoo.TimeSeries: {
                #             'all_annual_cashFlow': {
                #                 'columnSets': [
                #                     ['*', '', False, False, False, False],
                #                 ],
                #             },
                #         },
                #     },
                # },
                # 'all_trailing_cashFlow': {
                #     'scrapes': {
                #         scrape.yahoo.TimeSeries: {
                #             'all_trailing_cashFlow': {
                #                 'columnSets': [
                #                     ['*', '', False, False, False, False],
                #                 ],
                #             },
                #         },
                #     },
                # },
                
                # 'QuoteSummary': {
                #     'scrapes': {
                #         scrape.yahoo.QuoteSummary: {
                #             'all': {
                #                 'columnSets': [
                #                     ['*', '', False, False, False, False],
                #                 ],
                #             },
                #         },
                #     },
                # },
                # 'Chart': {
                #     'postProcs': [__getTimeSeries],
                #     'scrapes': {
                #         scrape.yahoo.Chart: {
                #             'all': {
                #                 'columnSets': [
                #                     ['*', '', False, False, False, False],
                #                 ],
                #             },
                #             # 'table_reference': {
                #             #     'columnSets': [
                #             #         ['keySymbol', 'symbol', True, True, True, False],
                #             #         ['chart', 'chart', False, False, False, False],
                #             #     ],
                #             # },
                #         },
                #     },
                # },
                # 'StockList': {
                #     'scrapes': {
                #         scrape.fmp.StockList: {
                #             'all': {
                #                 'columnSets': [
                #                     ['*', '', False, False, False, False],
                #                 ],
                #             },
                #         },
                #     },
                # },
                # 'Tickers': {
                #     'scrapes': {
                #         scrape.polygon.Tickers: {
                #             'all': {
                #                 'columnSets': [
                #                     ['*', '', False, False, False, False],
                #                 ],
                #             },
                #         },
                #     },
                # },
                # 'Saved': {
                #     'scrapes': {
                #         scrape.saved.Saved: {
                #             'all': {
                #                 'columnSets': [
                #                     ['*', '', False, False, False, False],
                #                 ],
                #             },
                #         },
                #     },
                # },
            },
        },
    }
