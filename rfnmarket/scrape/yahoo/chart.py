from ...utils import log, database
from .base import Base
from datetime import datetime
from pprint import pp 
import pandas as pd
import numpy as np

class Chart(Base):
    dbName = 'yahoo_chart'

    @staticmethod
    def getTableNames(tableName):
        return [tableName]

    def update(self, symbols, forceUpdate=False):
        symbolPeriod1 = {}

        now = datetime.now()
        # for symbol in symbols:
        #     symbolPeriod1[symbol] = int(now.timestamp()) - int(60*60*24*365.2422*10)
        
        # check last timestamp of symbols in quote database
        foundSymbolTimestamps = {}
        status = 'status_db'
        dfStatus = self.db.tableRead(status, keyValues=symbols)

        # force  chart read
        if forceUpdate:
            for symbol in symbols:
                if symbol in dfStatus:
                    symbolPeriod1[symbol] = dfStatus[symbol]['chart']
                else:
                    symbolPeriod1[symbol] = int(datetime.now().timestamp()) - int(60*60*24*365.2422*10)
            return symbolPeriod1
        
        # set period1 for all symbol requests
        now = datetime.now()
        # update period is 1 day
        updateTimestamp = int(now.timestamp()) - int(60*60*24)
        for symbol in symbols:
            if symbol in dfStatus:
                # if needed update add symbol with last timestamp
                if updateTimestamp >= dfStatus[symbol]['chart']:
                    symbolPeriod1[symbol] = dfStatus[symbol]['chart']
            else:
                # add symbol with period1 of 10 years
                symbolPeriod1[symbol] = int(now.timestamp()) - int(60*60*24*365.2422*10)

        for symbol in set(symbols).intersection(dfStatus.keys()):
            if updateTimestamp >= dfStatus[symbol]['chart']:
                symbolPeriod1[symbol] = dfStatus[symbol]['chart']

        return symbolPeriod1

    def __init__(self, symbols=[], tables=[], forceUpdate=False):
        super().__init__()

        # setup database
        self.db = database.Database(self.dbName)

        # lets see if we need an update
        symbolPeriod1 = self.update(symbols, forceUpdate=forceUpdate)

        # guess there is nothing to update
        if len(symbolPeriod1) == 0: return

        log.info('Chart update')
        log.info('symbols processing  : %s' % len(symbolPeriod1))

        requestArgsList = []

        # these symbols are needed for indexing while retrieving
        self.symbols = []
        requestArgsList = []
        lowestTimestamp = int(datetime.now().timestamp())
        for symbol, period1 in symbolPeriod1.items():
            if period1 < lowestTimestamp:
                lowestTimestamp = period1
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
        log.info('last time updated   : %s' % datetime.fromtimestamp(lowestTimestamp))

        self.start = datetime.now()
        self.multiRequest(requestArgsList, blockSize=50)
    
    def pushAPIData(self, symbolIndex, response):
        symbol = self.symbols[symbolIndex]
        # log.info('request time: %s: %s' % (datetime.now()- self.start, symbol))
        start = datetime.now()
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
                    dfs = []
                    symbolData = symbolData['result'][0]
                    chartData = {}
                    if 'timestamp' in symbolData:
                        timestamps = symbolData['timestamp']
                        if 'indicators' in symbolData:
                            # extract all the indicators
                            indicators = symbolData['indicators']
                            mergedQuote = {**indicators['quote'][0], **indicators['adjclose'][0]}
                            tsIndex = 0
                            for timestamp in timestamps:
                                chartData[timestamp] = {}
                                for param in mergedQuote.keys():
                                    chartData[timestamp][param] = mergedQuote[param][tsIndex]
                                tsIndex += 1

                    if 'events' in symbolData:
                        # extract all the events
                        events = symbolData['events']
                        for event, eventData in events.items():
                            for date, dateData in eventData.items():
                                if not dateData['date'] in chartData:
                                    chartData[dateData['date']] = {}
                                chartRow = chartData[dateData['date']]
                                if event == 'dividends':
                                    chartRow['dividend'] = dateData['amount']
                                elif event == 'capitalGains':
                                    chartRow['capitalGain'] = dateData['amount']
                                elif event == 'splits':
                                    chartRow['numerator'] = dateData['numerator']
                                    chartRow['denominator'] = dateData['denominator']
                                    chartRow['splitRatio'] = dateData['splitRatio']
                    
                    # make unique table name
                    tableName = 'chart_'
                    for c in symbol:
                        if c.isalnum():
                            tableName += c
                        else:
                            tableName += '_'

                    # write data 
                    if len(chartData) > 0:
                        self.db.tableWrite(tableName, chartData, 'timestamp', method='append')

                        # write table reference
                        self.db.tableWrite('table_reference', {symbol: {'chart': tableName}}, 'keySymbol', method='append')
                        
                        # get last iimestamp from chart
                        lastTimeStamp = max(list(chartData.keys()))
                    else:
                        # get now timestamp to show we tried
                        lastTimeStamp = int(datetime.now().timestamp())

                    # update status
                    self.db.tableWrite('status_db', {symbol: {'chart': lastTimeStamp}}, 'keySymbol', method='update')

                          
    def dbCommit(self):
        # call from base to commit
        self.db.commit()
