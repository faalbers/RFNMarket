from ...utils import log, database
from .base import Base
from datetime import datetime
from pprint import pp 

class Chart(Base):
    dbName = 'yahoo_chart'

    def setSymbolPeriod1(self):
        # set all symbols with lowest period1 of 10 years

        # check last timestamp of symbols in quote database
        db = database.Database(self.dbName)
        values, params = db.getRows('status_db')
        foundSymbolTimestamps = {}
        for value in values:
            foundSymbolTimestamps[value[0]] = value[1]

        # set period1 for all symbol requests
        self.symbolPeriod1 = {}
        now = datetime.now()
        self.lowestTimestamp = int(now.timestamp())
        # update period is 1 day
        updateTimestamp = int(now.timestamp()) - int(60*60*24)
        for symbol in self.symbols:
            if symbol in foundSymbolTimestamps:
                # if needed update add symbol with last timestamp
                if updateTimestamp >= foundSymbolTimestamps[symbol]:
                    self.symbolPeriod1[symbol] = foundSymbolTimestamps[symbol]
                if foundSymbolTimestamps[symbol] < self.lowestTimestamp:
                    self.lowestTimestamp = foundSymbolTimestamps[symbol]
            else:
                # add symbol with period1 of 10 years
                self.symbolPeriod1[symbol] = int(now.timestamp()) - int(60*60*24*365.2422*10)

        for symbol in set(self.symbols).intersection(foundSymbolTimestamps.keys()):
            if updateTimestamp >= foundSymbolTimestamps[symbol]:
                self.symbolPeriod1[symbol] = foundSymbolTimestamps[symbol]
            if foundSymbolTimestamps[symbol] < self.lowestTimestamp: self.lowestTimestamp = foundSymbolTimestamps[symbol]

    def updateTables(self, tables):
        if tables == None: return
        
        print('tables: %s' % tables)

    def __init__(self, symbols=None, types=None, tables=None, forceUpdate=False):
        super().__init__()

        self.updateTables(tables)
        
        print('Running Chart update but not executing')
        return
        # if we are not updating just use class for data retrieval
        if symbols == None or types == None: return

        # make shore we don't mess up the referenced symbols variable
        self.symbols = list(symbols)
        self.setSymbolPeriod1()

        # dont'run if no symbols
        if len(self.symbolPeriod1) == 0: return

        log.info('Chart update')
        log.info('last time updated   : %s' % (datetime.now() - datetime.fromtimestamp(self.lowestTimestamp)))
        log.info('symbols processing  : %s' % len(self.symbolPeriod1))

        return
    
        requestArgsList = []
        self.symbols = []
        for symbol, period1 in self.symbolPeriod1.items():
            period2 = int(datetime.now().timestamp())
            # print('symbol : %s' % symbol)
            # print('period1: %s' % period1)
            # print('period2: %s' % period2)
            requestArgs = {
                'url': 'https://query2.finance.yahoo.com/v8/finance/chart/'+symbol.upper(),
                'params': {
                    'period1': period1,
                    'period2': period2,
                    'interval': '1d',
                    'events': 'div,splits,capitalGains',
                },
                'timeout': 30,
            }                      
            requestArgsList.append(requestArgs)
            self.symbols.append(symbol)
        self.start = datetime.now()
        self.multiRequest(requestArgsList, blockSize=50)
    
    def updateStatus(self, symbol):
        start = datetime.now()
        db = database.Database(self.dbName)
        db.createTable('status_db', ["'keySymbol' TEXT PRIMARY KEY", "'timestamp' TIMESTAMP"])
        db.insertOrIgnore('status_db', ['keySymbol'], (symbol,))
        db.update( 'status_db', 'keySymbol', symbol, ['timestamp'], tuple([int(datetime.now().timestamp())]) )
        # log.info('status time : %s: %s' % (datetime.now()- start, symbol))

    def updateChartDB(self, symbol, typeName, chartData):
        start = datetime.now()
        if typeName == 'indicators':
            params = ["'timestamp' TIMESTAMP PRIMARY KEY", "'open' FLOAT", "'close' FLOAT", "'adjclose' FLOAT",
                "'high' FLOAT", "'low' FLOAT", "'volume' INTEGER"]
        elif typeName == 'splits':
            params = ["'timestamp' TIMESTAMP PRIMARY KEY", "'denominator' FLOAT", "'numerator' FLOAT", "'splitRatio' TEXT"]
        elif typeName == 'dividends' or typeName == 'capitalGains':
            params = ["'timestamp' TIMESTAMP PRIMARY KEY", "'amount' FLOAT"]
        else:
            log.info('chart type not found: %s' + typeName)
            return
        
        # create table name 
        tableName = str(typeName)+'_'
        for c in symbol:
            if c.isalnum():
                tableName += c
            else:
                tableName += '_'

        # create typeName table and symbol entry
        db = database.Database(self.dbName)
        db.createTable(typeName, ["'keySymbol' TEXT PRIMARY KEY", "'tableName' TEXT"])
        db.insertOrIgnore(typeName, ['keySymbol', 'tableName'], (symbol, tableName))
        
        # create chart table for keySymbol
        db.createTable(tableName, params)

        # gather data to write
        params = [x.split(' ')[0].strip("'")  for x in params]
        values = []
        for timestamp, timestampData in chartData.items():
            value = [timestamp]
            for param in params[1:]:
                value.append(timestampData[param])
            values.append(tuple(value))
        db.insertOrIgnore(tableName, params, values)
        seconds = datetime.now().timestamp()-start.timestamp()
        secPerWrite = seconds / len(chartData)
        overTime = (seconds > 0.4)
        if overTime:
            log.info('%.2f s: %s writes: %.4f s/w: write %s: %s overtime: %s' % (seconds, len(chartData), secPerWrite, typeName, overTime, symbol))
       
    def pushAPIData(self, symbolIndex, response):
        symbol = self.symbols[symbolIndex]
        # log.info('request time: %s: %s' % (datetime.now()- self.start, symbol))
        start = datetime.now()
        db = database.Database(self.dbName)
        if response.headers.get('content-type').startswith('application/json'):
            symbolData = response.json()
            if 'chart' in symbolData:
                # handle API response
                symbolData = symbolData['chart']
                if symbolData['error'] != None:
                    # handle error response
                    symbolData = symbolData['error']
                elif symbolData['result'] != None:
                    # handle data return response
                    symbolData = symbolData['result'][0]
                    if 'timestamp' in symbolData:
                        timestamps = symbolData['timestamp']
                        if 'indicators' in symbolData:
                            # extract all the indicators
                            indicators = symbolData['indicators']
                            chartData = {}
                            for timestamp in timestamps:
                                chartData[timestamp] = {}
                            for indicator, indicatorDataList in indicators.items():
                                for param, values in indicatorDataList[0].items():
                                    timestampIndex = 0
                                    for value in values:
                                        chartData[timestamps[timestampIndex]][param] = value
                                        timestampIndex += 1
                            self.updateChartDB(symbol, 'indicators', chartData)
                    if 'events' in symbolData:
                        # extract all the events
                        events = symbolData['events']
                        for event, eventData in events.items():
                            chartData = {}
                            for timestamp, eventEntry in eventData.items():
                                chartData[eventEntry['date']] = {}
                                for param, value in eventEntry.items():
                                    if param == 'date': continue
                                    chartData[eventEntry['date']][param] = value
                            self.updateChartDB(symbol, event, chartData)

            if 'finance' in symbolData:
                # handle other possible errors
                symbolData = symbolData['finance']
                if symbolData['error'] != None:
                    symbolData = symbolData['error']
        self.updateStatus(symbol)
        # log.info('full time   : %s: %s' % (datetime.now()- start, symbol))
        self.start = datetime.now()    
    
    def getChart(self, symbol, typeName):
        chart = {}
        db = database.Database(self.dbName)
        values, params = db.getRows(typeName, columns=['tableName'], whereColumns=['keySymbol'], areValues=[symbol])
        if len(values) == 0 or len(values[0]) == 0: return chart
        tableName = values[0][0]
        values, params = db.getRows(tableName)
        for value in values:
            timestamp = value[0]
            chart[timestamp] = {}
            index = 1
            for item in value[1:]:
                chart[timestamp][params[index]] = item
                index += 1
        return chart

    def getData(self, symbols, types):
        data = {}
        if len(symbols) == 0 or not 'price' in types: return data
        return data


        
