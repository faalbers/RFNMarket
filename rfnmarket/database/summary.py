from ..api import yahoo
from ..utils import log, Database
from datetime import datetime
from pprint import pp
import sqlite3


class Summary():
    def __init__(self):
        db = Database('summary')
        db.addTable('profiles', [
            'symbol TEXT PRIMARY KEY',
            'timestamp INTEGER',
            'name TEXT',
            'type TEXT',
            'exchange TEXT',
            'industry TEXT',
            'sector TEXT',
            'country TEXT',
            'city TEXT',
            'state TEXT',
            'timezone TEXT',
            'employees INTEGER',
            'info TEXT',
        ])
        db.addTable('statistics', [
            "'symbol' TEXT PRIMARY KEY",
            "'timestamp' INTEGER",
            
            "'category' TEXT",
            "'legalType' TEXT",
            "'fundFamily' TEXT",
            "'fundInceptionDate' INTEGER",
            
            "'forwardPE' FLOAT",
            "'beta' FLOAT",
            "'beta3Year' FLOAT",
            "'trailingEps' FLOAT",
            "'forwardEps' FLOAT",
            "'pegRatio' FLOAT",
            "'yield' FLOAT",
            
            "'morningStarOverallRating' INTEGER",
            "'morningStarRiskRating' INTEGER",
            
            "'lastCapGain' FLOAT",
            "'lastDividendValue' FLOAT",
            "'lastDividendDate' INTEGER",
            "'lastSplitFactor' TEXT",
            "'lastSplitDate' INTEGER",
            "'lastFiscalYearEnd' INTEGER",
            "'nextFiscalYearEnd' INTEGER",
            "'mostRecentQuarter' INTEGER",
            
            "'floatShares' INTEGER",
            "'sharesOutstanding' INTEGER",
            "'sharesShort' INTEGER",
            "'sharesShortPriorMonth' INTEGER",
            "'sharesShortPreviousMonthDate' INTEGER",
            "'sharesPercentSharesOut' FLOAT",
            "'impliedSharesOutstanding' INTEGER",
            "'heldPercentInsiders' FLOAT",
            "'heldPercentInstitutions' FLOAT",
            
            "'ytdReturn' FLOAT",
            "'threeYearAverageReturn' FLOAT",
            "'fiveYearAverageReturn' FLOAT",
            "'annualHoldingsTurnover' FLOAT",
            "'priceHint' FLOAT",
            "'enterpriseValue' FLOAT",
            "'totalAssets' INTEGER",
            "'profitMargins' FLOAT",
            "'dateShortInterest' INTEGER",
            "'shortRatio' FLOAT",
            "'shortPercentOfFloat' FLOAT",
            "'bookValue' FLOAT",
            "'priceToBook' FLOAT",
            "'earningsQuarterlyGrowth' FLOAT",
            "'annualReportExpenseRatio' FLOAT",
            "'netIncomeToCommon' FLOAT",
            "'enterpriseToRevenue' FLOAT",
            "'enterpriseToEbitda' FLOAT",
            
            "'52WeekChange' FLOAT",
            "'SandP52WeekChange' FLOAT",
            
            "'maxAge' INTEGER",
        ])
        self.apiSet = set()
        self.apiSet.add(yahoo.QuoteSummary)

    def update(self, symbols, updateMax=False):
        log.info('Update Summary database on %s symbols' % len(symbols))
        for apiModule in self.apiSet:
            apiModule(symbols, updateMax=updateMax)

    def getProfiles(self, symbols):
        data = {}

        # db = Database('profiles')
        # values, params = db.getRows('summary')
        # for items in values:
        #     if not items[0] in symbols: continue
        #     data[items[0]] = {}
        #     itemIndex = 2
        #     for item in items[2:]:
        #         param = params[itemIndex]
        #         data[items[0]][param] = item
        #         itemIndex += 1
        
        return data
    
    def getStatistics(self, symbols):
        data = {}

        # db = Database('profiles')
        # values, params = db.getRows('summary')
        # for items in values:
        #     if not items[0] in symbols: continue
        #     data[items[0]] = {}
        #     itemIndex = 2
        #     for item in items[2:]:
        #         param = params[itemIndex]
        #         data[items[0]][param] = item
        #         itemIndex += 1
        
        return data
    
    def getLowestTimestamp(self, symbols):
        lowTimestamp = int(datetime.now().timestamp())
        foundSymbols = []

        # db = Database('profiles')
        # found = {}
        # values, params = db.getRows('summary', ['symbol', 'timestamp'])
        # for result in values:
        #     found[result[0]] = result[1]
        # for symbol in symbols:
        #     if not symbol in found: continue
        #     foundSymbols.append(symbol)
        #     if found[symbol] < lowTimestamp: lowTimestamp = found[symbol]

        return lowTimestamp, foundSymbols

    def updateYahooQuoteSummary(self, symbol, symbolData):
        pass
        # # update summaryProfile
        # params = ['symbol','timestamp']
        # values = [symbol,symbolData['timestamp']]
        # if 'quoteType' in symbolData:
        #     mData = symbolData['quoteType']
        #     mParams = {'name': 'longName', 'type': 'quoteType', 'exchange': 'exchange', 'timezone': 'timeZoneShortName'}
        #     for dbParam, mParam in mParams.items():
        #         if mParam in mData:
        #             params.append(dbParam)
        #             values.append(mData[mParam])
        # if 'summaryProfile' in symbolData:
        #     mData = symbolData['summaryProfile']
        #     mParams = {'industry': 'industry', 'sector': 'sector', 'country': 'country', 'city': 'city', 'state': 'state',
        #         'employees': 'fullTimeEmployees', 'info': 'longBusinessSummary'}
        #     for dbParam, mParam in mParams.items():
        #         if mParam in mData:
        #             params.append(dbParam)
        #             values.append(mData[mParam])
        # db = Database('profiles')
        # db.insertOrReplace('summary', params, tuple(values))

