from ..api import yahoo
from ..utils import log, Database
from datetime import datetime
from pprint import pp
import sqlite3


class Statistics():
    def __init__(self):
        db = Database('statistics')
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
        self.apiSet.add(yahoo.QuoteStatistics)

    def update(self, symbols, updateMax=False):
        log.info('Update Profile database on %s symbols' % len(symbols))
        for apiModule in self.apiSet:
            apiModule(symbols, updateMax=updateMax)

    def getStatistics(self, symbols):
        data = {}

        db = Database('statistics')
        values, params = db.getRows('statistics')
        for items in values:
            if not items[0] in symbols: continue
            data[items[0]] = {}
            itemIndex = 2
            for item in items[2:]:
                param = params[itemIndex]
                data[items[0]][param] = item
                itemIndex += 1
        
        return data
    
    def getLowestTimestamp(self, symbols):
        lowTimestamp = int(datetime.now().timestamp())
        foundSymbols = []

        db = Database('statistics')
        found = {}
        values, params = db.getRows('summary', ['symbol', 'timestamp'])
        for result in values:
            found[result[0]] = result[1]
        for symbol in symbols:
            if not symbol in found: continue
            foundSymbols.append(symbol)
            if found[symbol] < lowTimestamp: lowTimestamp = found[symbol]

        return lowTimestamp, foundSymbols

    def updateYahooQuoteStatistics(self, symbol, symbolData):
        # update QuoteStatistics
        params = ['symbol','timestamp']
        values = [symbol,symbolData['timestamp']]
        if 'defaultKeyStatistics' in symbolData:
            sData = symbolData['defaultKeyStatistics']
            for param, value in sData.items():
                params.append(param)
                values.append(value)
            db = Database('statistics')
            try:
                db.insertOrReplace('statistics', params, tuple(values))
            except:
                log.exception('InsertOrReplace missed')
                pp(sData)
                exit(0)

            # mParams = {'name': 'longName', 'type': 'quoteType', 'exchange': 'exchange', 'timezone': 'timeZoneShortName'}
            # for dbParam, mParam in mParams.items():
            #     if mParam in mData:
            #         params.append(dbParam)
            #         values.append(mData[mParam])
    
    def updateYahooQuoteProfile(self, symbol, symbolData):
        pass

    def updateYahooChart(self, symbol, symbolData):
        pass
