from .. import scrape
from ..utils import database
from pprint import pp
import queue, math
from multiprocessing import Pool, cpu_count

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
    def getTimeSeries_proc(params):
        scrape_class, key_references = params
        db = database.Database(scrape_class.dbName)
        timeTables = {}
        for key_reference in key_references:
            timeTables[key_reference[0]] = db.tableRead(key_reference[1])
        return timeTables
    
    @staticmethod
    def __getTimeSeries(self, data, tableNames=[], scrapeClass=None):
        if 'table_reference' not in data: return {}
        db = self.getScrapeDB(scrapeClass)
        time_tables = {}
        if len(data['table_reference']) < 105:
            for keyValue, keyData in data['table_reference'].items():
                for tableName in keyData.keys():
                    if not tableName in time_tables:
                        time_tables[tableName] = {}
                    time_tables[tableName][keyValue] = db.tableRead(keyData[tableName])
        else:
            task_queue = queue.Queue()
            tables = {}
            cpus = 8
            for key_value, key_data in data['table_reference'].items():
                for table_name in key_data.keys():
                    if not table_name in tables:
                        tables[table_name] = []
                    tables[table_name].append((key_value, key_data[table_name]))
            for table, key_references in tables.items():
                if not table in time_tables:
                    time_tables[table] = {}
                entries_per_proc = math.ceil(len(key_references) / cpus)
                print(len(key_references))
                print('entries per proc: %s' % entries_per_proc)
                for key_reference in key_references: task_queue.put(key_reference)
                with Pool(processes=cpus) as pool:
                    while not task_queue.empty():
                        # build a list of chunks to process in parallel
                        chunk = []
                        for proc_idx in range(cpus):
                            chunk_key_values = []
                            for _ in range(entries_per_proc):
                                if task_queue.empty(): break
                                chunk_key_values.append(task_queue.get())
                            if len(chunk_key_values) == 0: break
                            # add a params tuple to the chunk
                            chunk.append((scrapeClass, chunk_key_values))
                        # process the chunk in parallel
                        results = pool.map(Catalog.getTimeSeries_proc, chunk)
                
                        # concatenate the results into the data series
                        for result in results:
                            time_tables[table] = {**time_tables[table], **result}
                        
                        # delete the results to free up memory
                        del(results)
        return time_tables

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
        'recommendation': {
            # 'info': 'data to analyze recommendation',
            'postProcs': [[__dropParent, {}]],
            'sets': {
                'earnings': {
                    'postProcs': [[__mergeTables, {}]],
                    'scrapes': {
                        scrape.yahoo.QuoteSummary: {
                            'financialData': {
                                'keyValues': True,
                                'columnSettings': [
                                    ['recommendationKey', 'recommendationKey', {}],
                                    ['numberOfAnalystOpinions', 'numberOfAnalystOpinions', {}],
                                    ['currentPrice', 'currentPrice', {}],
                                ],
                            },
                            'summaryDetail': {
                                'keyValues': True,
                                'columnSettings': [
                                    ['forwardPE', 'forwardPE_sd', {}],
                                    ['trailingPE', 'trailingPE', {}],
                                ],
                            },
                            'defaultKeyStatistics': {
                                'keyValues': True,
                                'columnSettings': [
                                    ['forwardEps', 'forwardEps', {}],
                                    ['trailingEps', 'trailingEps', {}],
                                    ['forwardPE', 'forwardPE_dks', {}],
                                ],
                            },
                        },
                    },
                },
            },
        },
        'earnings': {
            'info': 'ticker earnings',
            'postProcs': [[__dropParent, {}]],
            'sets': {
                'earnings': {
                    'postProcs': [[__mergeTables, {}]],
                    'scrapes': {
                        scrape.yahoo.QuoteSummary: {
                            'earnings': {
                                'keyValues': True,
                                'columnSettings': [
                                    ['earningsChart', 'earningsChart', {}],
                                    ['financialsChart', 'financialsChart', {}],
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
                                    ['price', 'price', {}],
                                    ['costBasis', 'costBasis', {}],
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
                                    ['beta3Year', 'beta3Year', {}],
                                    ['pegRatio', 'pegRatio', {}],
                                    ['yield', 'yield', {}],
                                    ['sharesOutstanding', 'sharesOutstanding', {}],
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
                # 'etrade': {
                #     'scrapes': {
                #         scrape.etrade.Quote: {
                #             'all': {
                #                 'keyValues': True,
                #                 'columnSettings': [
                #                     ['all', '', {}],
                #                 ],
                #             },
                #         },
                #     },
                # },
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
                # 'etrade': {
                #     'scrapes': {
                #         scrape.etrade.Quote: {
                #             'all': {
                #                 'keyValues': True,
                #                 'columnSettings': [
                #                     ['all', '', {}],
                #                 ],
                #             },
                #         },
                #     },
                # },
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
