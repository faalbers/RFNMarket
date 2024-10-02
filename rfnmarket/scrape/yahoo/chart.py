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
        print(forceUpdate)
        symbolPeriod1 = {}

        # check last timestamp of symbols in quote database
        foundSymbolTimestamps = {}
        dfStatus = self.db.idxTableReadData('status_db')
        for symbol in dfStatus.index:
            foundSymbolTimestamps[symbol] = int(dfStatus.loc[symbol,'chart'])

        # force  chart read
        if forceUpdate:
            for symbol in symbols:
                if symbol in foundSymbolTimestamps:
                    symbolPeriod1[symbol] = foundSymbolTimestamps[symbol]
                else:
                    symbolPeriod1[symbol] = int(datetime.now().timestamp()) - int(60*60*24*365.2422*10)
            return symbolPeriod1
        
        # set period1 for all symbol requests
        now = datetime.now()
        # update period is 1 day
        updateTimestamp = int(now.timestamp()) - int(60*60*24)
        for symbol in symbols:
            if symbol in foundSymbolTimestamps:
                # if needed update add symbol with last timestamp
                if updateTimestamp >= foundSymbolTimestamps[symbol]:
                    symbolPeriod1[symbol] = foundSymbolTimestamps[symbol]
            else:
                # add symbol with period1 of 10 years
                symbolPeriod1[symbol] = int(now.timestamp()) - int(60*60*24*365.2422*10)

        for symbol in set(symbols).intersection(foundSymbolTimestamps.keys()):
            if updateTimestamp >= foundSymbolTimestamps[symbol]:
                symbolPeriod1[symbol] = foundSymbolTimestamps[symbol]

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
        log.info('last time updated   : %s' % (datetime.now() - datetime.fromtimestamp(lowestTimestamp)))

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
                    dfChart = None
                    if 'timestamp' in symbolData:
                        timestamps = symbolData['timestamp']
                        if 'indicators' in symbolData:
                            # extract all the indicators
                            indicators = symbolData['indicators']
                            mergedQuote = {**indicators['quote'][0], **indicators['adjclose'][0]}
                            dfChart = pd.DataFrame(mergedQuote, index=timestamps)

                    if 'events' in symbolData:
                        # extract all the events
                        events = symbolData['events']
                        for event, eventData in events.items():
                            dfEvent = pd.DataFrame(eventData).T
                            dfEvent.set_index('date', inplace=True)
                            rename = {}
                            for columnName in dfEvent.columns:
                                rename[columnName] = event + columnName.capitalize()
                            dfEvent.rename(columns=rename, inplace=True)
                            dfChart = pd.merge(dfChart, dfEvent, left_index=True, right_index=True, how='outer')

                    if isinstance(dfChart, pd.DataFrame):
                        # create tablename with no illegal characters
                        tableName = 'chart_'
                        for c in symbol:
                            if c.isalnum():
                                tableName += c
                            else:
                                tableName += '_'
                        
                        dfChart.rename_axis('timestamp', inplace=True)

                        # get current chart and find chart timestamps that need to be added
                        dfChartDB = self.db.idxTableReadData(tableName)
                        dfChart = dfChart[~dfChart.index.isin(dfChartDB.index)].dropna(axis=1, how='all')

                        lastTimeStamp = None
                        if len(dfChart) > 0:
                            if len(dfChartDB) > 0:
                                dfChart = pd.concat([dfChartDB, dfChart])
                            # replace chart
                            dType = {'timestamp': 'INTEGER PRIMARY KEY'}
                            dfChart.to_sql(tableName, self.db.getConnection(), if_exists='replace', index=True, dtype=dType)
                            lastTimeStamp = int(dfChart.index[-1])
                        elif len(dfChartDB) == 0:
                            # if no prior entry and no current entry, just set last entry date to now
                            lastTimeStamp = int(datetime.now().timestamp())
                        
                        # update chart tablenames
                        if lastTimeStamp != None:
                            self.db.idxTableWriteData({'chart': lastTimeStamp}, 'status_db', 'keySymbol', symbol, 'update')
                    

    
    def dbCommit(self):
        # call from base to commit
        self.db.commit()
