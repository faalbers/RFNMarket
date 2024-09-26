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
        # check last timestamp of symbols in quote database
        foundSymbolTimestamps = {}
        if self.db.tableExists('status_db'):
            dfStatus = pd.read_sql("SELECT * FROM 'status_db'", self.db.getConnection(), index_col='keySymbol')
            for symbol in dfStatus.index:
                foundSymbolTimestamps[symbol] = int(dfStatus.loc[symbol,'chart'])
        print(foundSymbolTimestamps)
        
        # set period1 for all symbol requests
        symbolPeriod1 = {}
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
        print(tables)

        # setup database
        self.db = database.Database(self.dbName)

        # lets see if we need an update
        symbolPeriod1 = self.update(symbols, forceUpdate=False)

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
                    if 'timestamp' in symbolData:
                        timestamps = symbolData['timestamp']
                        if 'indicators' in symbolData:
                            # extract all the indicators
                            indicators = symbolData['indicators']
                            dfIndicator = pd.DataFrame(indicators)
                            quote = dfIndicator['quote'].get(0, None)
                            adjclose = dfIndicator['adjclose'].get(0, None)
                            dfQuote = pd.DataFrame(quote, index=timestamps)
                            dfQuote['adjclose'] = adjclose['adjclose'].copy()
                            dfQuote.rename_axis('date', inplace=True)
                            dfs.append(dfQuote)
                    if 'events' in symbolData:
                        # extract all the events
                        events = symbolData['events']
                        dfEvents = pd.DataFrame(events)
                        for column in dfEvents.columns:
                            dfEvent = pd.DataFrame(list(dfEvents[column].dropna()))
                            dfEvent.set_index('date', inplace=True)
                            rename = {}
                            for eventColumn in dfEvent.columns:
                                rename[eventColumn] = column+eventColumn.capitalize()
                            dfEvent.rename(columns=rename, inplace=True)
                            dfs.append(dfEvent)
                    # merge all dataframes
                    if len(dfs) == 0: return
                    dfChart = dfs[0]
                    for df in dfs[1:]:
                        dfChart = pd.merge(dfChart, df, left_index=True, right_index=True, how='outer')

                    # create tablename with no illegal characters
                    tableName = 'chart_'
                    for c in symbol:
                        if c.isalnum():
                            tableName += c
                        else:
                            tableName += '_'

                    # add new found date to existing dfChart
                    if self.db.tableExists(tableName):
                        dfChartDB = pd.read_sql("SELECT * FROM '%s'" % tableName, self.db.getConnection(), index_col='date')
                        # takes out the None values
                        dfChartDB.fillna(value=np.nan, inplace=True)
                        # remove rows with dates that are already in it
                        dfChart = dfChart[~dfChart.index.isin(dfChartDB.index)]
                        # concat new dates
                        dfChart = pd.concat([dfChartDB, dfChart], )
                    
                    # write new table
                    dtype = {'date': 'TIMESTAMP PRIMARY KEY'}
                    dfChart.to_sql(tableName, self.db.getConnection(), if_exists='replace', dtype=dtype)

                    # update chart tablenames
                    timeTableName = 'chart'
                    if not self.db.tableExists(timeTableName):
                        # create new on if it does not exist
                        statusData = [{'keySymbol': symbol, 'tableName': tableName}]
                        dfChartTables = pd.DataFrame(statusData)
                        dfChartTables.set_index('keySymbol', inplace=True)
                    else:
                        # enter in existing status
                        dfChartTables = pd.read_sql("SELECT * FROM '%s'" % timeTableName, self.db.getConnection(), index_col='keySymbol')
                        dfChartTables.loc[symbol,'tableName'] = tableName
                    dtype = {'keySymbol': 'STRING PRIMARY KEY'}
                    dfChartTables.to_sql(timeTableName, self.db.getConnection(), if_exists='replace', dtype=dtype)
                    
                    
                    # update status_db
                    statusTableName = 'status_db'
                    if not self.db.tableExists(statusTableName):
                        # create new on if it does not exist
                        statusData = [{'keySymbol': symbol, timeTableName: dfChart.index[-1]}]
                        dfStatus = pd.DataFrame(statusData)
                        dfStatus.set_index('keySymbol', inplace=True)
                    else:
                        # enter in existing status
                        dfStatus = pd.read_sql("SELECT * FROM '%s'" % statusTableName, self.db.getConnection(), index_col='keySymbol')
                        dfStatus.loc[symbol,timeTableName] = dfChart.index[-1]
                    dtype = {'keySymbol': 'STRING PRIMARY KEY'}
                    dfStatus.to_sql(statusTableName, self.db.getConnection(), if_exists='replace', dtype=dtype)
    
    def dbCommit(self):
        # call from base to commit
        self.db.commit()
