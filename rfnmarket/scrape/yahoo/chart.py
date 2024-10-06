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

    @staticmethod
    def getTsTypeUpdatePeriods():
        tsTypeUpdatePeriods = {
            'default': 60*60*24,
            'chart': 60*60*24,
        }
        return tsTypeUpdatePeriods

    def update(self, symbols, tables, forceUpdate=False):
        tsTypes = set(tables)
        symbolSettings = {}

        if forceUpdate:
            now = int(datetime.now().timestamp())
            tenYear = int(now - (60*60*24*365.2422*10))
            for symbol in symbols:
                symbolSettings[symbol] = {}
                symbolSettings[symbol][tenYear] = tsTypes
            return symbolSettings

        # check last timestamp of symbols in quote database
        dataStatus = self.db.tableRead('status_db', keyValues=symbols, columns=tables)
        dataLastEntry = self.db.tableRead('lastentry_db', keyValues=symbols, columns=tables)

        # build status check for all symbols
        statusCheck = {}
        for symbol in symbols:
            statusCheck[symbol] = {}
            if symbol in dataStatus:
                for tsType in tsTypes:
                    statusCheck[symbol][tsType] = {}
                    if tsType in dataStatus[symbol]:
                        # tsType was done for symbol before
                        statusCheck[symbol][tsType]['status'] = dataStatus[symbol][tsType]
                        if symbol in dataLastEntry and tsType in dataLastEntry[symbol]:
                            # tsType has latest entry for symbol
                            statusCheck[symbol][tsType]['latest'] = dataLastEntry[symbol][tsType]
                        else:
                            statusCheck[symbol][tsType]['latest'] = None
                    else:
                        # No status for tstype in symbol, set both to none
                        statusCheck[symbol][tsType] = {'status': None, 'last': None}
            else:
                # no status for symbol, set all tsTypes to status and latest None
                for tsType in tsTypes:
                    statusCheck[symbol][tsType] = {'status': None, 'last': None}
        
        # create symbolSettings
        tsTypeUpdatePeriods = self.getTsTypeUpdatePeriods()
        tenyearTimediff = int(60*60*24*365.2422*10)
        now = int(datetime.now().timestamp())
        for symbol, checkData in statusCheck.items():
            # setup settings 
            settings = {}
            for tsType, statusData in  checkData.items():
                if statusData['status'] == None:
                    # not done yet , we search for 10 years
                    lastTimeStamp = now - tenyearTimediff
                    if not lastTimeStamp in settings:
                        settings[lastTimeStamp] = set()
                    settings[lastTimeStamp].add(tsType)
                elif statusData['latest'] == None:
                    # it has been tried before, but nothing popped up
                    # we just dont't try anymore. might change later
                    pass
                    # updateTimestamp = now - annualTimediff
                    # lastTimeStamp = now
                    # if lastTimeStamp <= updateTimestamp:
                    #     if not lastTimeStamp in settings:
                    #         settings[lastTimeStamp] = set()
                    #     settings[lastTimeStamp].add(tsType)
                else:
                    # set the update time to check based on naming of tsType
                    if tsType in tsTypeUpdatePeriods:
                        updateTimestamp = now - tsTypeUpdatePeriods[tsType]
                    else:
                        updateTimestamp = now - tsTypeUpdatePeriods['default']
                    # get last entry timestamp for tsType
                    lastTimeStamp = dataLastEntry[symbol][tsType]
                    if lastTimeStamp <= updateTimestamp:
                        # we need to update with found period timestamp
                        if not lastTimeStamp in settings:
                            settings[lastTimeStamp] = set()
                        settings[lastTimeStamp].add(tsType)
            # if settings is not empty addit to the symbol entry of symbolSettings
            if len(settings) > 0:
                symbolSettings[symbol] = settings

        return symbolSettings

    def __init__(self, symbols=[], tables=[], forceUpdate=False):
        super().__init__()

        # setup database
        self.db = database.Database(self.dbName)

        # lets see if we need an update
        symbolSettings = self.update(symbols, tables, forceUpdate=forceUpdate)

        # guess there is nothing to update
        if len(symbolSettings) == 0: return

        log.info('Chart update')
        log.info('requested types   : %s' % " ".join(tables))
        log.info('symbols processing: %s' % len(symbolSettings))

        self.symbolSettings = symbolSettings

        # update procs need these
        self.symbols = [] # accessed by index
        self.tsTypes = []
        requestArgsList = []
        typesProcessed = set()
        for symbol, tsPeriodTypes in symbolSettings.items():
            # print(symbol)
            for period1, types in tsPeriodTypes.items():
                # for symbol in self.symbols:
                # for symbol, settings in symbolSettings.items():
                typesProcessed = typesProcessed.union(types)
                period2 = int(datetime.now().timestamp())

                typesString = ",".join(types)
                # print('types  : %s' % typesString)
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
                self.tsTypes.append(types)
        log.info('types processing : %s' % " ".join(typesProcessed))
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

                        # write last entry timestamp
                        lastTimeStamp = max(list(chartData.keys()))
                        self.db.tableWrite('lastentry_db', {symbol: {'chart': lastTimeStamp}}, 'keySymbol', method='update')
                        
        # update status
        status = {symbol: {}}
        now = int(datetime.now().timestamp())
        for tsType in self.tsTypes[symbolIndex]:
            status[symbol][tsType] = now
        self.db.tableWrite('status_db', status, 'keySymbol', method='update')

                          
    def dbCommit(self):
        # call from base to commit
        self.db.commit()
