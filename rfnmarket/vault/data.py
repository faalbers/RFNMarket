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
                for dataFrame, dfData in self.__catalog[catalog]['sets'].items():
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
        mics = data['mic']['ISO10383_MIC']
        usacronyms = set()
        usmics = set()
        for micRow in mics:
            if 'cc' in micRow and micRow['cc'] == 'US':
                usmics.add(micRow['mic'])
                if 'acronym' in micRow:
                    usacronyms.add(micRow['acronym'])

        for keyValue, keyData in data['profile'].items():
            if 'mic' in keyData and keyData['mic'] in usmics:
                keyData['exchangeCountry'] = 'United States'
            if 'acronym' in keyData and keyData['acronym'] in usacronyms:
                keyData['exchangeCountry'] = 'United States'

        data.pop('mic')

        return data['profile']

    @staticmethod
    def __getTimeSeries(self, data, tableNames=[], scrapeClass=None):
        if 'table_reference' not in data: return {}
        db = self.getScrapeDB(scrapeClass)
        timeTables = {}
        for keyValue, keyData in data['table_reference'].items():
            for tableName in keyData.keys():
                if not tableName in timeTables:
                    timeTables[tableName] = {}
                timeTables[tableName][keyValue] = db.tableRead(keyData[tableName])
        return timeTables

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
    def __dropParent(self, data):
        for parent, pData in data.items(): return pData

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
        'statistics': {
            'info': 'ticker company profile information',
            'postProcs': [[__dropParent, {}]],
            'sets': {
                'statistics': {
                    'postProcs': [[__mergeTables, {}]],
                    'scrapes': {
                        scrape.yahoo.QuoteSummary: {    # scrape class to retrieve data from
                            'defaultKeyStatistics': {   # table name to be searched
                                'keyValues': True,
                                'columnSettings': [
                                    ['trailingEps', 'trailingEps', {}],
                                    ['forwardEps', 'forwardEps', {}],
                                    ['pegRatio', 'pegRatio', {}],
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
        'price': {
            'info': 'ticker price',
            'postProcs': [[__dropParent, {}]],
            'sets': {
                'price': {
                    'postProcs': [[__dropParent, {}]],
                    'scrapes': {
                        scrape.yahoo.QuoteSummary: {
                            'price': {
                                'keyValues': True,
                                'columnSettings': [
                                    ['regularMarketPrice', 'price', {}],
                                    ['regularMarketOpen', 'open', {}],
                                    ['regularMarketDayHigh', 'dayHigh', {}],
                                    ['regularMarketDayLow', 'dayLow', {}],
                                    ['regularMarketPreviousClose', 'previousClose', {}],
                                    ['regularMarketTime', 'marketTime', {}],
                                    ['regularMarketVolume', 'volume', {}],
                                ],
                            },
                        },
                    },
                },
            },
        },
        'profile': {
            'info': 'ticker company profile information',
            'postProcs': [[__addExchangeData, {}]],
            'sets': {
                'profile': {
                    'postProcs': [[__mergeTables, {}]],
                    'scrapes': {
                        scrape.yahoo.QuoteSummary: {
                            'quoteType': {
                                'keyValues': True,
                                'columnSettings': [
                                    ['longName', 'name', {}],
                                    ['quoteType', 'typeQuote', {}],
                                ],
                            },
                            'summaryDetail': {
                                'keyValues': True,
                                'columnSettings': [
                                    ['currency', 'currency', {}],
                                ],
                            },
                            'assetProfile': {
                                'keyValues': True,
                                'columnSettings': [
                                    ['sectorKey', 'sector', {}],
                                    ['industryKey', 'industry', {}],
                                    ['country', 'country', {}],
                                    ['city', 'city', {}],
                                    ['state', 'state', {}],
                                ],
                            },
                        },
                        scrape.fmp.StockList: {
                            'stocklist': {
                                'keyValues': True,
                                'columnSettings': [
                                    ['exchangeShortName', 'exchange', {}],
                                    ['type', 'type', {}],
                                ],
                                'subTable': None,
                            },
                        },
                        scrape.polygon.Tickers: {
                            'tickers': {
                                'keyValues': True,
                                'columnSettings': [
                                    ['primary_exchange', 'mic', {}],
                                    ['market', 'market', {}],
                                    ['type', 'typeCode', {}],
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
        'quicken': {
            'info': 'quicken data',
            'postProcs': [[__dropParent, {}]],
            'sets': {
                'transactions': {
                    'postProcs': [[__dropParent, {}]],
                    'scrapes': {
                        scrape.saved.Saved: {
                            'QUICKEN_2020': {
                                'keyValues': False,
                                'columnSettings': [
                                    ['timestamp', 'timestamp', {}],
                                    ['symbol', 'symbol', {}],
                                    ['transaction', 'transaction', {}],
                                    ['shares', 'shares', {}],
                                ],
                            },
                        },
                    },
                },
            },
        },
        'chart': {
            'info': 'chart data',
            'postProcs': [[__dropParent, {}]],
            'sets': {
                'chart': {
                    'postProcs': [[__getTimeSeries, {'scrapeClass': scrape.yahoo.Chart}]],
                    'scrapes': {
                        scrape.yahoo.Chart: {
                            'table_reference': {
                                'keyValues': True,
                                'columnSettings': [
                                    ['chart', 'chart', {}],
                                ],
                            },
                        },
                    },
                },
            },
        },
        'all': {
            'info': 'all avalable database data',
            'sets': {
                'all_quarterly_financials': {
                    'scrapes': {
                        scrape.yahoo.TimeSeries: {
                            'all_quarterly_financials': {
                                'keyValues': True,
                                'columnSettings': [
                                    ['all', '', {}],
                                ],
                            },
                        },
                    },
                },
                'all_annual_financials': {
                    'scrapes': {
                        scrape.yahoo.TimeSeries: {
                            'all_annual_financials': {
                                'keyValues': True,
                                'columnSettings': [
                                    ['all', '', {}],
                                ],
                            },
                        },
                    },
                },
                'all_trailing_financials': {
                    'scrapes': {
                        scrape.yahoo.TimeSeries: {
                            'all_trailing_financials': {
                                'keyValues': True,
                                'columnSettings': [
                                    ['all', '', {}],
                                ],
                            },
                        },
                    },
                },
                'all_quarterly_balanceSheet': {
                    'scrapes': {
                        scrape.yahoo.TimeSeries: {
                            'all_quarterly_balanceSheet': {
                                'keyValues': True,
                                'columnSettings': [
                                    ['all', '', {}],
                                ],
                            },
                        },
                    },
                },
                'all_annual_balanceSheet': {
                    'scrapes': {
                        scrape.yahoo.TimeSeries: {
                            'all_annual_balanceSheet': {
                                'keyValues': True,
                                'columnSettings': [
                                    ['all', '', {}],
                                ],
                            },
                        },
                    },
                },
                'all_quarterly_cashFlow': {
                    'scrapes': {
                        scrape.yahoo.TimeSeries: {
                            'all_quarterly_cashFlow': {
                                'keyValues': True,
                                'columnSettings': [
                                    ['all', '', {}],
                                ],
                            },
                        },
                    },
                },
                'all_annual_cashFlow': {
                    'scrapes': {
                        scrape.yahoo.TimeSeries: {
                            'all_annual_cashFlow': {
                                'keyValues': True,
                                'columnSettings': [
                                    ['all', '', {}],
                                ],
                            },
                        },
                    },
                },
                'all_trailing_cashFlow': {
                    'scrapes': {
                        scrape.yahoo.TimeSeries: {
                            'all_trailing_cashFlow': {
                                'keyValues': True,
                                'columnSettings': [
                                    ['all', '', {}],
                                ],
                            },
                        },
                    },
                },
                
                'quotesummary': {
                    'scrapes': {
                        scrape.yahoo.QuoteSummary: {
                            'all': {
                                'keyValues': True,
                                'columnSettings': [
                                    ['all', '', {}],
                                ],
                            },
                        },
                    },
                },
                'charts': {
                    'postProcs': [[__getTimeSeries, {'scrapeClass': scrape.yahoo.Chart}]],
                    'scrapes': {
                        scrape.yahoo.Chart: {
                            'table_reference': {
                                'keyValues': True,
                                'columnSettings': [
                                    ['all', '', {}],
                                ],
                            },
                        },
                    },
                },
                'stocklist': {
                    'postProcs': [[__dropParent, {}]],
                    'scrapes': {
                        scrape.fmp.StockList: {
                            'all': {
                                'keyValues': True,
                                'columnSettings': [
                                    ['all', '', {}],
                                ],
                            },
                        },
                    },
                },
                'tickers': {
                    'postProcs': [[__dropParent, {}]],
                    'scrapes': {
                        scrape.polygon.Tickers: {
                            'all': {
                                'keyValues': True,
                                'columnSettings': [
                                    ['all', '', {}],
                                ],
                            },
                        },
                    },
                },
                'saved': {
                    'scrapes': {
                        scrape.saved.Saved: {
                            'all': {
                                'keyValues': False,
                                'columnSettings': [
                                    ['all', '', {}],
                                ],
                            },
                        },
                    },
                },
            },
        },
    }
