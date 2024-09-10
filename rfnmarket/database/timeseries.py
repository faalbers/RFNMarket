from ..api import yahoo
from ..utils import log, Database
from datetime import datetime
import sqlite3

class TimeSeries():
    def __init__(self):
        db = Database('timeseries_dividends')
        db = Database('timeseries_splits')
        db = Database('timeseries_price_volume')
        self.apiSet = set()
        self.apiSet.add(yahoo.Chart)

    def update(self, symbols, updateMax=False):
        log.info('Update Timeseries database on %s symbols' % len(symbols))
        for apiModule in self.apiSet:
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
            foundData = cursor.execute("SELECT * FROM '%s'" % symbol)
            columns = foundData.description
            for items in foundData.fetchall():
                data[symbol][items[0]] = {}
                itemIndex = 1
                for item in items[1:]:
                    param = columns[itemIndex][0]
                    data[symbol][items[0]][param] = item
                    itemIndex += 1
        
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

    def getLowestTimestamp(self, symbols):
        lowTimestamp = int(datetime.now().timestamp())
        foundSymbolsAll = set(symbols)
        
        foundSymbols = []
        db = Database('timeseries_price_volume')
        for symbol in db.getTableNames():
            if symbol in symbols:
                foundSymbols.append(symbol)
        foundSymbolsAll = foundSymbolsAll.intersection(set(foundSymbols))
        for maxValues in db.getMaxValues(foundSymbols, ['timestamp']):
            maxTimestamp = maxValues[0]
            if maxTimestamp != None:
                if maxTimestamp < lowTimestamp:
                    lowTimestamp = maxTimestamp
        
        return lowTimestamp, list(foundSymbolsAll)

    def updateYahooQuoteProfile(self, symbol, symbolData):
        pass

    def updateYahooChart(self, symbol, symbolData):
        # update price and volume
        db = Database('timeseries_price_volume')        
        for kind, kindData in symbolData.items():
            if kind == 'indicators':
                for indicator, indicatorData in kindData.items():
                    if indicator == 'quote':
                        if len(indicatorData) == 0: continue
                        db.addTable(symbol, ['timestamp INTEGER PRIMARY KEY', 'open FLOAT', ' close FLOAT', 'low FLOAT', 'high FLOAT', 'volume INTEGER'])
                        entryList = []
                        for timestamp, quoteParamData in indicatorData.items():
                            entryList.append((timestamp, quoteParamData['open'], quoteParamData['close'], quoteParamData['low'], quoteParamData['high'], quoteParamData['volume']))
                        db.insertOrReplace(symbol, ['timestamp', 'open', 'close', 'low', 'high', 'volume', ], entryList)

        # update dividends
        db = Database('timeseries_dividends')        
        for kind, kindData in symbolData.items():
            if kind == 'events':
                for event, eventData in kindData.items():
                    if event == 'dividends':
                        if len(eventData) == 0: continue
                        db.addTable(symbol, ['timestamp INTEGER PRIMARY KEY', 'amount FLOAT'])
                        entryList = []
                        for timestamp, amount in eventData.items():
                            entryList.append((timestamp, amount['amount']))
                        db.insertOrReplace(symbol, ['timestamp', 'amount'], entryList)

        # update splits
        db = Database('timeseries_splits')        
        for kind, kindData in symbolData.items():
            if kind == 'events':
                for event, eventData in kindData.items():
                    if event == 'splits':
                        if len(eventData) == 0: continue
                        db.addTable(symbol, ['timestamp INTEGER PRIMARY KEY', 'numerator FLOAT', 'denominator FLOAT', 'splitRatio TEXT'])
                        entryList = []
                        for timestamp, item in eventData.items():
                            entryList.append((timestamp, item['numerator'], item['denominator'], item['splitRatio']))
                        db.insertOrReplace(symbol, ['timestamp', 'numerator', 'denominator', 'splitRatio'], entryList)

