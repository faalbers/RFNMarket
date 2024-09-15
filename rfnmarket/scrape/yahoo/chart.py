from ...utils import log, database
from .base import Base
from datetime import datetime
from pprint import pp 

class Chart(Base):
    def setSymbolPeriod1(self):
        # set all symbols with lowest period1 of 10 years

        # check last timestamp of symbols in quote database
        db = database.Database(self.dbNameQuote)
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

    def __init__(self, symbols=None, types=None):
        super().__init__()
        self.dbNameQuote = 'yahoo_chart_quote'
        self.dbNameDividend = 'yahoo_chart_dividend'
        self.dbNameSplit = 'yahoo_chart_split'
        self.dbNameCapitalGains = 'yahoo_chart_capitalgains'

        # if we are not updating just use class for data retrieval
        if symbols == None : return

        # make shore we don't mess up the referenced symbols variable
        self.symbols = list(symbols)
        self.setSymbolPeriod1()

        # dont'run if no symbols
        if len(self.symbolPeriod1) == 0: return

        log.info('Chart update')
        log.info('last time updated   : %s' % (datetime.now() - datetime.fromtimestamp(self.lowestTimestamp)))
        log.info('symbols processing  : %s' % len(self.symbolPeriod1))

        requestArgsList = []
        self.symbols = []
        for symbol, period1 in self.symbolPeriod1.items():
            period2 = int(datetime.now().timestamp())
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
        self.multiRequest(requestArgsList, blockSize=50)
    
    def updateStatus(self, symbol, db):
        db.createTable('status_db', ["'keySymbol' TEXT PRIMARY KEY", "'timestamp' TIMESTAMP"])
        db.insertOrIgnore('status_db', ['keySymbol'], (symbol,))
        db.update( 'status_db', 'keySymbol', symbol, ['timestamp'], tuple([int(datetime.now().timestamp())]) )

    def updateTimeseriesDB(self, symbol, timeseriesData, db):
        params = []
        for timestamp, data in timeseriesData.items():
            for param, value in data.items():
                type = None
                if isinstance(value, float):
                    type = 'FLOAT'
                elif isinstance(value, int):
                    type = 'INTEGER'
                elif isinstance(value, str):
                    type = 'TEXT'
                else:
                    params = []
                    break
                params.append("'"+param+"' "+type)
            break
        if len(params) == 0: return
        params.sort()
        params = ["'timestamp' TIMESTAMP PRIMARY KEY"]+params
        db.createTable(symbol, params)
        for timestamp, timestampData in timeseriesData.items():
            db.insertOrIgnore(symbol, ['timestamp'], (timestamp,))
            params = []
            values = []
            for param, value in timestampData.items():
                params.append(param)
                values.append(value)
            db.update( symbol, 'timestamp', timestamp, params, tuple(values) )
       
    def pushAPIData(self, symbolIndex, response):
        symbol = self.symbols[symbolIndex]
        dbQuote = database.Database(self.dbNameQuote)
        dbDividend = database.Database(self.dbNameDividend)
        dbSplit = database.Database(self.dbNameSplit)
        dbCapitalGains = database.Database(self.dbNameCapitalGains)
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
                            timeseriesData = {}
                            for timestamp in timestamps:
                                timeseriesData[timestamp] = {}
                            for indicator, indicatorDataList in indicators.items():
                                for param, values in indicatorDataList[0].items():
                                    timestampIndex = 0
                                    for value in values:
                                        timeseriesData[timestamps[timestampIndex]][param] = value
                                        timestampIndex += 1
                            self.updateTimeseriesDB(symbol, timeseriesData, dbQuote)
                    if 'events' in symbolData:
                        # extract all the events
                        events = symbolData['events']
                        for event, eventData in events.items():
                            timeseriesData = {}
                            for timestamp, eventEntry in eventData.items():
                                timeseriesData[eventEntry['date']] = {}
                                for param, value in eventEntry.items():
                                    if param == 'date': continue
                                    timeseriesData[eventEntry['date']][param] = value
                            if event == 'dividends':
                                self.updateTimeseriesDB(symbol, timeseriesData, dbDividend)
                            if event == 'splits':
                                self.updateTimeseriesDB(symbol, timeseriesData, dbSplit)
                            if event == 'capitalGains':
                                self.updateTimeseriesDB(symbol, timeseriesData, dbCapitalGains)

            if 'finance' in symbolData:
                # handle other possible errors
                symbolData = symbolData['finance']
                if symbolData['error'] != None:
                    symbolData = symbolData['error']
        
        self.updateStatus(symbol, dbQuote)
