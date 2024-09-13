from ...utils import log, database
from .base import Base
from datetime import datetime
from pprint import pp 

class Chart(Base):
    def setUpdatePeriods(self):
        mult = 1
        # for now we only use default of 1 day
        self.updatePeriods = {
            'default': mult*60*60*24,
        }

    def trimSymbols(self):
        # set all symbols with lowest period1 of 10 years
        now = datetime.now()
        self.lowestTimestamp = int(now.timestamp())
        self.symbolPeriod1 = {}
        for symbol in self.symbols:
            self.symbolPeriod1[symbol] = self.lowestTimestamp - int(60*60*24*365.2422*10)

        # # check last update period of symbols in quote database and use that for period1
        db = database.Database(self.dbNameQuote)
        self.lowestTimestamp = int(now.timestamp())
        values, params = db.getRows('status_db')
        for value in values:
            symbol = value[0]
            if symbol in self.symbols:
                timestamp = value[1]
                if timestamp < self.lowestTimestamp: self.lowestTimestamp = timestamp
                self.symbolPeriod1[symbol] = timestamp

        # if last update is less then update period, remove symbol
        # use default update period for now        
        for symbol, timestamp in self.symbolPeriod1.items():
            updatePeriod = int(now.timestamp()) - timestamp
            if updatePeriod < self.updatePeriods['default']:
                self.symbols.remove(symbol)

    def __init__(self, symbols, types):
        super().__init__()
        log.info('Chart update')
        self.dbNameQuote = 'yahoo_chart_quote'
        self.dbNameDividend = 'yahoo_chart_dividend'
        self.dbNameSplit = 'yahoo_chart_split'
        self.dbNameCapitalGains = 'yahoo_chart_capitalgains'
        # make shore we don't mess up the referenced symbols variable
        self.symbols = list(symbols)
        self.types = types
        self.setUpdatePeriods()
        self.trimSymbols()

        log.info('last time updated   : %s' % (datetime.now() - datetime.fromtimestamp(self.lowestTimestamp)))
        log.info('symbols processing  : %s' % len(self.symbols))

        # dont'run if no symbols
        if len(self.symbols) == 0: return

        requestArgsList = []
        for symbol in self.symbols:
            period1 = self.symbolPeriod1[symbol]
            period2 = int(datetime.now().timestamp())
            # print(symbol)
            # print('period1: %s' % datetime.fromtimestamp(period1))
            # print('period2: %s' % datetime.fromtimestamp(period2))
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
        self.multiRequest(requestArgsList, blockSize=50)
    
    def updateStatus(self, symbol, db):
        db.addTable('status_db', ["'keySymbol' TEXT PRIMARY KEY", "'timestamp' TIMESTAMP"])
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
        db.addTable(symbol, params)
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
                            dbq = database.Database(self.dbNameQuote)
                            self.updateStatus(symbol, dbq)
                            self.updateTimeseriesDB(symbol, timeseriesData, dbq)
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
                                dbd = database.Database(self.dbNameDividend)
                                self.updateStatus(symbol, dbd)
                                self.updateTimeseriesDB(symbol, timeseriesData, dbd)
                            if event == 'splits':
                                dbs = database.Database(self.dbNameSplit)
                                self.updateStatus(symbol, dbs)
                                self.updateTimeseriesDB(symbol, timeseriesData, dbs)
                            if event == 'capitalGains':
                                dbc = database.Database(self.dbNameCapitalGains)
                                self.updateStatus(symbol, dbc)
                                self.updateTimeseriesDB(symbol, timeseriesData, dbc)

            if 'finance' in symbolData:
                # handle other possible errors
                symbolData = symbolData['finance']
                if symbolData['error'] != None:
                    symbolData = symbolData['error']
