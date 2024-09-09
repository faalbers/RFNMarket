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

    def getProfile(self, symbols):
        connection = sqlite3.connect('database/timeseries_dividends.db')
        cursor = connection.cursor()

        # cursor.execute("SELECT name FROM sqlite_schema WHERE type='table'")
        # foundSymbols = set([x[0] for x in cursor.fetchall()]).intersection(set(symbols))

        data = {}
        # for symbol in foundSymbols:
        #     data[symbol] = {}
        #     for result in cursor.execute("SELECT * FROM '%s'" % symbol):
        #         data[symbol][result[0]] = result[1]

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

    def updateYahooAPI(self, symbol, symbolData):
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
        
        cursor.execute('INSERT OR IGNORE INTO "summary" VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', entry)

        connection.commit()
        cursor.close()
        connection.close()

    def updateYahooQuoteSummary(self, data):
        # update summaryProfile
        connection = sqlite3.connect('database/profiles.db')
        cursor = connection.cursor()
        cursor.execute("""CREATE TABLE IF NOT EXISTS 'summary' ( symbol TEXT PRIMARY KEY, timestamp INTEGER, 
        name TEXT, type TEXT, exchange TEXT, industry TEXT, sector TEXT, country TEXT, city TEXT,
        state TEXT, timezone TEXT, employees INTEGER, info TEXT)
        """)
        entryList = []
        for symbol, symbolData in data.items():
            timestamp = symbolData['timestamp']
            dbParams = {}

            if 'quoteType' in symbolData:
                # spData = symbolData['summaryProfile']
                mData = symbolData['quoteType']
                mParams = {'name': 'longName', 'type': 'quoteType', 'exchange': 'exchange',
                    'timezone': 'timeZoneShortName'}
                for dbParam, mParam in mParams.items():
                    if not mParam in mData:
                        dbParams[dbParam] = None
                    else:
                        dbParams[dbParam] = mData[mParam]
            if 'summaryProfile' in symbolData:
                mData = symbolData['summaryProfile']
                mParams = {'industry': 'industry', 'sector': 'sector', 'country': 'country', 'city': 'city',
                    'state': 'state', 'employees': 'fullTimeEmployees', 'info': 'longBusinessSummary'}
                for dbParam, mParam in mParams.items():
                    if not mParam in mData:
                        dbParams[dbParam] = None
                    else:
                        dbParams[dbParam] = mData[mParam]
            entryList.append((symbol, timestamp, dbParams['name'], dbParams['type'], dbParams['exchange'],
                dbParams['industry'], dbParams['sector'], dbParams['country'], dbParams['city'], dbParams['state'],
                dbParams['timezone'], dbParams['employees'], dbParams['info']))
        
        cursor.executemany('INSERT OR IGNORE INTO "summary" VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', entryList)

        connection.commit()
        cursor.close()
        connection.close()
