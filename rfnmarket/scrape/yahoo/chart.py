from ...utils import log, database
from .base import Base
from datetime import datetime, time, timedelta
from pprint import pp 
import pandas as pd
import numpy as np

class Chart(Base):
    dbName = 'yahoo_chart'

    @staticmethod
    def getTableNames(tableName):
        if tableName == 'all':
            return ['chart']
        return [tableName]

    def update(self, symbols, forceUpdate=False):
        dataStatus = self.db.tableRead('status_db', keyValues=symbols, columns=['chart'])
        dataLastEntry = self.db.tableRead('lastentry_db', keyValues=symbols, columns=['chart'])

        today = datetime.now().date()
        tenYears = datetime(year=today.year-10, month=today.month, day=today.day).date()
        oneWeek = today - timedelta(days=7)
        today = int(datetime.combine(today, time()).timestamp())
        tenYears = int(datetime.combine(tenYears, time()).timestamp())
        oneWeek = int(datetime.combine(oneWeek, time()).timestamp())
        
        symbolPeriods = {}
        for symbol in symbols:
            if not symbol in dataStatus:
                symbolPeriods[symbol] = tenYears
            elif symbol in dataLastEntry:
                lastEntry = dataLastEntry[symbol]['chart']
                # not sure about this one week thing yet
                # if lastEntry >= oneWeek:
                #     symbolPeriods[symbol] = lastEntry
                symbolPeriods[symbol] = lastEntry
        
        return symbolPeriods
    
    def __init__(self, symbols=[], tables=[], forceUpdate=False):
        super().__init__()

        # setup database
        self.db = database.Database(self.dbName)

        # lets see if we need an update
        symbolPeriods = self.update(symbols, forceUpdate=forceUpdate)

        # guess there is nothing to update
        if len(symbolPeriods) == 0: return

        log.info('Chart update')
        log.info('symbols processing: %s' % len(symbolPeriods))

        # update procs need these
        self.symbols = [] # accessed by index
        requestArgsList = []
        typesProcessed = set()
        for symbol, period1 in symbolPeriods.items():
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
            self.symbols.append(symbol)
        log.info('requests running : %s' % len(requestArgsList))
        self.multiRequest(requestArgsList, blockSize=100)
    
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
                                timestamp = datetime.fromtimestamp(timestamp).date()
                                timestamp = int(datetime.combine(timestamp, time()).timestamp())
                                rowData = {}
                                for param in mergedQuote.keys():
                                    if mergedQuote[param][tsIndex] != None:
                                        rowData[param] = mergedQuote[param][tsIndex]
                                if len(rowData) > 0:
                                    chartData[timestamp] = rowData
                                tsIndex += 1

                    if 'events' in symbolData:
                        # extract all the events
                        events = symbolData['events']
                        for event, eventData in events.items():
                            for date, dateData in eventData.items():
                                timestamp = datetime.fromtimestamp(dateData['date']).date()
                                timestamp = int(datetime.combine(timestamp, time()).timestamp())
                                if not timestamp in chartData:
                                    chartData[timestamp] = {}
                                chartRow = chartData[timestamp]
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
                        self.db.tableWrite(tableName, chartData, 'timestamp', method='update')

                        # write table reference
                        self.db.tableWrite('table_reference', {symbol: {'chart': tableName}}, 'keySymbol', method='append')

                        # write last entry timestamp
                        lastTimeStamp = max(list(chartData.keys()))
                        self.db.tableWrite('lastentry_db', {symbol: {'chart': lastTimeStamp}}, 'keySymbol', method='update')
                        
        # update status
        status = {symbol: {'chart': int(datetime.now().timestamp())}}
        self.db.tableWrite('status_db', status, 'keySymbol', method='update')

                          
    def dbCommit(self):
        # call from base to commit
        self.db.commit()
