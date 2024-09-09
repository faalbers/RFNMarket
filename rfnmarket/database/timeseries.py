from ..api import yahoo
from ..utils import log
from datetime import datetime
import sqlite3

class TimeSeries():
    def __init__(self):
        self._apiSet = set()
        self._apiSet.add(yahoo.Chart)

    def update(self, symbols, updateMax=False):
        log.info('Update Timeseries database on %s symbols' % len(symbols))
        for apiModule in self._apiSet:
            apiModule(symbols, updateMax=updateMax)

    def getDividends(self, symbols):
        connection = sqlite3.connect('database/timeseries_dividends.db')
        cursor = connection.cursor()

        cursor.execute("SELECT name FROM sqlite_schema WHERE type='table'")
        foundSymbols = set([x[0] for x in cursor.fetchall()]).intersection(set(symbols))

        data = {}
        for symbol in foundSymbols:
            data[symbol] = {}
            for result in cursor.execute("SELECT * FROM '%s'" % symbol):
                data[symbol][result[0]] = result[1]

        cursor.close()
        connection.close()
        
        return data
    
    def getSplits(self, symbols):
        connection = sqlite3.connect('database/timeseries_splits.db')
        cursor = connection.cursor()
        
        cursor.execute("SELECT name FROM sqlite_schema WHERE type='table'")
        foundSymbols = set([x[0] for x in cursor.fetchall()]).intersection(set(symbols))

        data = {}
        for symbol in foundSymbols:
            data[symbol] = {}
            for result in cursor.execute("SELECT * FROM '%s'" % symbol):
                # data[symbol][result[0]] = result[1]
                data[symbol][result[0]] = {}
                data[symbol][result[0]]['numerator'] = result[1]
                data[symbol][result[0]]['denominator'] = result[2]
                data[symbol][result[0]]['splitRatio'] = result[3]
        
        cursor.close()
        connection.close()
        
        return data

    def getPrices(self, symbols):
        connection = sqlite3.connect('database/timeseries_price_volume.db')
        cursor = connection.cursor()

        cursor.execute("SELECT name FROM sqlite_schema WHERE type='table'")
        foundSymbols = set([x[0] for x in cursor.fetchall()]).intersection(set(symbols))

        data = {}
        for symbol in foundSymbols:
            data[symbol] = {}
            for result in cursor.execute("SELECT * FROM '%s'" % symbol):
                # data[symbol][result[0]] = result[1]
                data[symbol][result[0]] = {}
                data[symbol][result[0]]['open'] = result[1]
                data[symbol][result[0]]['close'] = result[2]
                data[symbol][result[0]]['low'] = result[3]
                data[symbol][result[0]]['high'] = result[4]
        
        cursor.close()
        connection.close()
        
        return data

    def getVolumes(self, symbols):
        connection = sqlite3.connect('database/timeseries_price_volume.db')
        cursor = connection.cursor()

        cursor.execute("SELECT name FROM sqlite_schema WHERE type='table'")
        foundSymbols = set([x[0] for x in cursor.fetchall()]).intersection(set(symbols))

        data = {}
        for symbol in foundSymbols:
            data[symbol] = {}
            for result in cursor.execute("SELECT * FROM '%s'" % symbol):
                # data[symbol][result[0]] = result[1]
                data[symbol][result[0]] = result[5]
        
        cursor.close()
        connection.close()
        
        return data

    def getLastLowestTimestamp(self, symbols):
        lowTimestamp = int(datetime.now().timestamp())

        # check dividends
        connection = sqlite3.connect('database/timeseries_dividends.db')
        cursor = connection.cursor()
        
        cursor.execute("SELECT name FROM sqlite_schema WHERE type='table'")
        for symbol in cursor.fetchall():
           for result in cursor.execute("SELECT MAX(timestamp) FROM %s" % symbol):
               if result[0] < lowTimestamp:
                   lowTimestamp = result[0]

        cursor.close()
        connection.close()
        
        # check price and volume
        connection = sqlite3.connect('database/timeseries_price_volume.db')
        cursor = connection.cursor()
        
        cursor.execute("SELECT name FROM sqlite_schema WHERE type='table'")
        for symbol in cursor.fetchall():
           for result in cursor.execute("SELECT MAX(timestamp) FROM %s" % symbol):
               if result[0] < lowTimestamp:
                   lowTimestamp = result[0]

        cursor.close()
        connection.close()

        return lowTimestamp

    def updateYahooQuoteProfile(self, symbol, symbolData):
        pass

    def updateYahooChart(self, symbol, symbolData):
        # update dividends
        connection = sqlite3.connect('database/timeseries_dividends.db')
        cursor = connection.cursor()

        for kind, kindData in symbolData.items():
            if kind == 'events':
                for event, eventData in kindData.items():
                    if event == 'dividends':
                        if len(eventData) == 0: continue
                        cursor.execute('CREATE TABLE IF NOT EXISTS "%s" (timestamp INTEGER PRIMARY KEY, amount FLOAT)' % symbol)
                        entryList = []
                        for timestamp, amount in eventData.items():
                            entryList.append((timestamp, amount['amount']))
                        cursor.executemany('INSERT OR IGNORE INTO "%s" (timestamp, amount) VALUES (?, ?)' % symbol, entryList)

        connection.commit()
        cursor.close()
        connection.close()
    
        # update splits
        connection = sqlite3.connect('database/timeseries_splits.db')
        cursor = connection.cursor()

        for kind, kindData in symbolData.items():
            if kind == 'events':
                for event, eventData in kindData.items():
                    if event == 'splits':
                        if len(eventData) == 0: continue
                        cursor.execute('CREATE TABLE IF NOT EXISTS "%s" (timestamp INTEGER PRIMARY KEY, numerator FLOAT, denominator FLOAT, splitRatio TEXT)' % symbol)
                        entryList = []
                        for timestamp, item in eventData.items():
                            entryList.append((timestamp, item['numerator'], item['denominator'], item['splitRatio']))
                        cursor.executemany('INSERT OR IGNORE INTO "%s" VALUES (?, ?, ?, ?)' % symbol, entryList)

        connection.commit()
        cursor.close()
        connection.close()
    
        # update price and volume
        connection = sqlite3.connect('database/timeseries_price_volume.db')
        cursor = connection.cursor()
        
        for kind, kindData in symbolData.items():
            if kind == 'indicators':
                for indicator, indicatorData in kindData.items():
                    if indicator == 'quote':
                        if len(indicatorData) == 0: continue
                        cursor.execute('CREATE TABLE IF NOT EXISTS "%s" (timestamp INTEGER PRIMARY KEY, open FLOAT, close FLOAT, low FLOAT, high FLOAT, volume INTEGER)' % symbol)
                        entryList = []
                        for timestamp, quoteParamData in indicatorData.items():
                            entryList.append((timestamp, quoteParamData['open'], quoteParamData['close'], quoteParamData['low'], quoteParamData['high'], quoteParamData['volume']))
                        cursor.executemany('INSERT OR IGNORE INTO "%s" VALUES (?, ?, ?, ?, ?, ?)' % symbol, entryList)

        connection.commit()
        cursor.close()
        connection.close()
