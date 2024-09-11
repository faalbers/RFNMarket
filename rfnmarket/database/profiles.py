from ..api import yahoo
from ..utils import log, Database
from datetime import datetime
from pprint import pp
import sqlite3


class Profiles():
    def __init__(self):
        db = Database('profiles')
        db.addTable('summary', [
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
        self.apiSet = set()
        self.apiSet.add(yahoo.QuoteProfile)

    def update(self, symbols, updateMax=False):
        log.info('Update Profile database on %s symbols' % len(symbols))
        for apiModule in self.apiSet:
            apiModule(symbols, updateMax=updateMax)

    def getProfiles(self, symbols):
        data = {}

        db = Database('profiles')
        values, params = db.getRows('summary')
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

        db = Database('profiles')
        found = {}
        values, params = db.getRows('summary', ['symbol', 'timestamp'])
        for result in values:
            found[result[0]] = result[1]
        for symbol in symbols:
            if not symbol in found: continue
            foundSymbols.append(symbol)
            if found[symbol] < lowTimestamp: lowTimestamp = found[symbol]

        return lowTimestamp, foundSymbols

    def updateYahooQuoteProfile(self, symbol, symbolData):
        # update summaryProfile
        params = ['symbol','timestamp']
        values = [symbol,symbolData['timestamp']]
        if 'quoteType' in symbolData:
            mData = symbolData['quoteType']
            mParams = {'name': 'longName', 'type': 'quoteType', 'exchange': 'exchange', 'timezone': 'timeZoneShortName'}
            for dbParam, mParam in mParams.items():
                if mParam in mData:
                    params.append(dbParam)
                    values.append(mData[mParam])
        if 'summaryProfile' in symbolData:
            mData = symbolData['summaryProfile']
            mParams = {'industry': 'industry', 'sector': 'sector', 'country': 'country', 'city': 'city', 'state': 'state',
                'employees': 'fullTimeEmployees', 'info': 'longBusinessSummary'}
            for dbParam, mParam in mParams.items():
                if mParam in mData:
                    params.append(dbParam)
                    values.append(mData[mParam])
        db = Database('profiles')
        db.insertOrReplace('summary', params, tuple(values))

