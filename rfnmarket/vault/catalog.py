from .. import scrape
from pprint import pp

class Catalog():
    def __init__(self):
        pass

    def getCatalog(self, catName):
        if catName in self.__catalog:
            return self.__catalog[catName]
        return {}
    
    # post processes to handle retrieved data
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
        
        keyValues = keyValues.union(data['other']['NASDAQ'].keys())
        keyValues = keyValues.union(data['other']['SPDRS'].keys())

        return list(keyValues)

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

    # system to retrieve data from databases
    # <catalog name>: {
    #   'info': information about this catalog
    #   'postProcs': [[postProc, {}]], post processes to manipulate sets data
    #   'sets': { set or group of specific data
    #       <set name>: {
    #           'postProcs': [[postProc, {}]], post processes to manipulate scraped tables data
    #           'scrapes': # collection of scrape classes where to retrieve data from
    #               <scrape class>: { database scrape class
    #                   <table name>: { table name to retrieve data from, table name set to 'all' retrieves all tables
    #                       'keyValues': True, this will handle table columns with PRIMARY KEY
    #                       'columnSettings': [ setting on how to retrieve column data
    #                           ['all', '', {}], # search name, new name, future setting for data manipulation, seach name 'all' retrieves all columns
    __catalog = {
        'GICS': {
            'info': 'sector and industry info',
            'postProcs': [[__dropParent, {}]],
            'sets': {
                'GICS': {
                    'postProcs': [[__mergeTables, {}]],
                    'scrapes': {
                        scrape.yahoo.QuoteSummary: {
                            'assetProfile': {
                                'keyValues': True,
                                'columnSettings': [
                                    ['industry', 'industry', {}],
                                    ['sector', 'sector', {}],
                                ],
                            },
                        },
                        scrape.saved.Saved: {
                            'SPDRS': {
                                'keyValues': True,
                                'columnSettings': [
                                    ['SP500sector', 'SP500sector', {}],
                                ],
                            },
                            'NASDAQ': {
                                'keyValues': True,
                                'columnSettings': [
                                    ['Industry', 'NASDAQindustry', {}],
                                    ['Sector', 'NASDAQsector', {}],
                                ],
                            },
                        },
                    },
                },
            },
        },
        'epstest': {
            'info': 'ticker company profile information',
            'postProcs': [[__dropParent, {}]],
            'sets': {
                'eps': {
                    'postProcs': [[__mergeTables, {}]],
                    'scrapes': {
                        scrape.etrade.Quote: {
                            'equity': {
                                'keyValues': True,
                                'columnSettings': [
                                    ['eps', 'eps', {}],
                                ],
                            },
                        },
                        scrape.yahoo.QuoteSummary: {
                            'defaultKeyStatistics': {
                                'keyValues': True,
                                'columnSettings': [
                                    ['trailingEps', 'trailingEps', {}],
                                    ['forwardEps', 'forwardEps', {}],
                                ],
                            },
                            'earningsHistory': {
                                'keyValues': True,
                                'columnSettings': [
                                    ['history', 'history', {}],
                                ],
                            },
                            'earningsTrend': {
                                'keyValues': True,
                                'columnSettings': [
                                    ['trend', 'trend', {}],
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
                'other': {
                    'scrapes': {
                        scrape.saved.Saved: {
                            'NASDAQ': {
                                'keyValues': True,
                                'columnSettings': [
                                    ['Name', 'Name', {}],
                                ],
                            },
                            'SPDRS': {
                                'keyValues': True,
                                'columnSettings': [
                                    ['Name', 'Name', {}],
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
        'ep': {
            'info': 'ep',
            'sets': {
                'ep': {
                    'postProcs': [[__mergeTables, {}]],
                    'scrapes': {
                        scrape.yahoo.QuoteSummary: {
                            'defaultKeyStatistics': {
                                'keyValues': True,
                                'columnSettings': [
                                    ['forwardPE', 'forwardPE', {}],
                                ],
                            },
                            'summaryDetail': {
                                'keyValues': True,
                                'columnSettings': [
                                    ['forwardPE', 'forwardPE2', {}],
                                    ['trailingPE', 'trailingPE', {}],
                                ],
                            },
                        },
                    },
                },
            },
        },
        'profile': {
            'info': 'ticker company profile information',
            'sets': {
                'profile': {
                    'postProcs': [[__mergeTables, {}]],
                    'scrapes': {
                        scrape.yahoo.QuoteSummary: {
                            'quoteType': {
                                'keyValues': True,
                                'columnSettings': [
                                    ['longName', 'name', {}],
                                    ['quoteType', 'type', {}],
                                ],
                            },
                            'summaryDetail': {
                                'keyValues': True,
                                'columnSettings': [
                                    ['currency', 'currency', {}],
                                    ['marketCap', 'marketCap', {}],
                                    ['city', 'dcity', {}],
                                    ['state', 'dstate', {}],
                                ],
                            },
                            'summaryProfile': {
                                'keyValues': True,
                                'columnSettings': [
                                    ['longBusinessSummary', 'info', {}],
                                ],
                            },
                            'assetProfile': {
                                'keyValues': True,
                                'columnSettings': [
                                    ['industry', 'industry', {}],
                                    ['country', 'country', {}],
                                    ['city', 'city', {}],
                                    ['state', 'state', {}],
                                ],
                            },
                            'fundProfile': {
                                'keyValues': True,
                                'columnSettings': [
                                    ['family', 'fundFamily', {}],
                                    ['categoryName', 'categoryName', {}],
                                    ['legalType', 'legalType', {}],
                                ],
                            },
                        },
                        scrape.fmp.StockList: {
                            'stocklist': {
                                'keyValues': True,
                                'columnSettings': [
                                    ['exchangeShortName', 'acronym', {}],
                                    ['type', 'stockType', {}],
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
                        scrape.saved.Saved: {
                            'SPDRS': {
                                'keyValues': True,
                                'columnSettings': [
                                    ['SP500sector', 'sectorSP500', {}],
                                ],
                            },
                            'NASDAQ': {
                                'keyValues': True,
                                'columnSettings': [
                                    ['Industry', 'industryNASDAQ', {}],
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
                                    ['forwardPE', 'forwardPE_A', {}],
                                    ['beta', 'beta', {}],
                                    ['pegRatio', 'pegRatio', {}],
                                    ['yield', 'yield', {}],
                                ],
                            },
                            'summaryDetail': {
                                'keyValues': True,
                                'columnSettings': [
                                    ['forwardPE', 'forwardPE_B', {}],
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
                            'earningsHistory': {
                                'keyValues': True,
                                'columnSettings': [
                                    ['epsActual', 'epsActual', {}],
                                    ['epsEstimate', 'epsEstimate', {}],
                                    ['epsDifference', 'epsDifference', {}],
                                    ['quarter', 'quarter', {}],
                                ],
                            },
                        },
                    },
                },
            },
        },
        'timeSeries': {
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
        'saved': {
            'info': 'saved database',
            'sets': {
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
        'all': {
            'info': 'all avalable database data',
            'sets': {
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
                'chart': {
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
                'etrade': {
                    'scrapes': {
                        scrape.etrade.Quote: {
                            'all': {
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
            },
        },
        'update': {
            'info': 'all avalable database data',
            'sets': {
                # 'quotesummary': {
                #     'scrapes': {
                #         scrape.yahoo.QuoteSummary: {
                #             'all': {
                #                 'keyValues': True,
                #                 'columnSettings': [
                #                     ['all', '', {}],
                #                 ],
                #             },
                #         },
                #     },
                # },
                # 'chart': {
                #     'postProcs': [[__getTimeSeries, {'scrapeClass': scrape.yahoo.Chart}]],
                #     'scrapes': {
                #         scrape.yahoo.Chart: {
                #             'table_reference': {
                #                 'keyValues': True,
                #                 'columnSettings': [
                #                     ['all', '', {}],
                #                 ],
                #             },
                #         },
                #     },
                # },
                'etrade': {
                    'scrapes': {
                        scrape.etrade.Quote: {
                            'all': {
                                'keyValues': True,
                                'columnSettings': [
                                    ['all', '', {}],
                                ],
                            },
                        },
                    },
                },
                # 'stocklist': {
                #     'postProcs': [[__dropParent, {}]],
                #     'scrapes': {
                #         scrape.fmp.StockList: {
                #             'all': {
                #                 'keyValues': True,
                #                 'columnSettings': [
                #                     ['all', '', {}],
                #                 ],
                #             },
                #         },
                #     },
                # },
                # 'tickers': {
                #     'postProcs': [[__dropParent, {}]],
                #     'scrapes': {
                #         scrape.polygon.Tickers: {
                #             'all': {
                #                 'keyValues': True,
                #                 'columnSettings': [
                #                     ['all', '', {}],
                #                 ],
                #             },
                #         },
                #     },
                # },
                # 'saved': {
                #     'scrapes': {
                #         scrape.saved.Saved: {
                #             'all': {
                #                 'keyValues': False,
                #                 'columnSettings': [
                #                     ['all', '', {}],
                #                 ],
                #             },
                #         },
                #     },
                # },
            },
        },
    }
