from ..api import yahoo
from ..utils import log
from datetime import datetime
from pprint import pp

import sqlite3

class Profiles():
    def __init__(self):
        # set db path
        self._apiSet = set()
        self._apiSet.add(yahoo.QuoteProfile)

    def update(self, symbols, updateMax=False):
        log.info('Update Profile database on %s symbols' % len(symbols))
        for apiModule in self._apiSet:
            apiModule(symbols, updateMax=updateMax)

    def getProfiles(self, symbols):
        connection = sqlite3.connect('database/profiles.db')
        cursor = connection.cursor()

        data = {}
        foundData = cursor.execute("SELECT * FROM 'summary'")
        columns = foundData.description
        for items in foundData.fetchall():
            if not items[0] in symbols: continue
            data[items[0]] = {}
            itemIndex = 2
            for item in items[2:]:
                param = columns[itemIndex][0]
                data[items[0]][param] = item
                itemIndex += 1

        cursor.close()
        connection.close()
        
        return data

    def getLastLowestTimestamp(self, symbols):
        lowTimestamp = int(datetime.now().timestamp())

        # check dividends
        connection = sqlite3.connect('database/profiles.db')
        cursor = connection.cursor()
        
        for result in cursor.execute("SELECT MAX(timestamp) FROM 'summary'"):
            if result[0] < lowTimestamp:
                lowTimestamp = result[0]

        cursor.close()
        connection.close()
        
        return lowTimestamp

    def updateYahooQuoteProfile(self, symbol, symbolData):
        # update summaryProfile
        connection = sqlite3.connect('database/profiles.db')
        cursor = connection.cursor()
        cursor.execute("""CREATE TABLE IF NOT EXISTS 'summary' ( symbol TEXT PRIMARY KEY, timestamp INTEGER, 
        name TEXT, type TEXT, exchange TEXT, industry TEXT, sector TEXT, country TEXT, city TEXT,
        state TEXT, timezone TEXT, employees INTEGER, info TEXT)
        """)

        timestamp = symbolData['timestamp']
        dbParams = {'name': None, 'type': None, 'exchange': None, 'timezone': None, 'industry': None, 'sector': None, 'country': None,
            'city': None, 'state': None, 'employees': None, 'info': None}
        if 'quoteType' in symbolData:
            # spData = symbolData['summaryProfile']
            mData = symbolData['quoteType']
            mParams = {'name': 'longName', 'type': 'quoteType', 'exchange': 'exchange', 'timezone': 'timeZoneShortName'}
            for dbParam, mParam in mParams.items():
                if mParam in mData: dbParams[dbParam] = mData[mParam]
        if 'summaryProfile' in symbolData:
            mData = symbolData['summaryProfile']
            mParams = {'industry': 'industry', 'sector': 'sector', 'country': 'country', 'city': 'city', 'state': 'state',
                'employees': 'fullTimeEmployees', 'info': 'longBusinessSummary'}
            for dbParam, mParam in mParams.items():
                if mParam in mData: dbParams[dbParam] = mData[mParam]
        
        entry = (symbol, timestamp, dbParams['name'], dbParams['type'], dbParams['exchange'],
            dbParams['industry'], dbParams['sector'], dbParams['country'], dbParams['city'], dbParams['state'],
            dbParams['timezone'], dbParams['employees'], dbParams['info'])
        
        cursor.execute('INSERT OR REPLACE INTO "summary" VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', entry)

        connection.commit()
        cursor.close()
        connection.close()

    def updateYahooChart(self, symbol, symbolData):
        pass
