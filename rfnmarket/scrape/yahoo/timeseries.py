from ...utils import log, database
from .base import Base
from pprint import pp
from datetime import datetime
import json
import pandas as pd
from . import const

class TimeSeries(Base):
    dbName = 'yahoo_timeseries'

    @staticmethod
    def getTableNames(tableName):
        if tableName.startswith('all_'):
            tnSplits = tableName.split('_')
            period = tnSplits[1]
            category = tnSplits[2]
            if not period in const.FUNDAMENTALS_PERIODTYPES: return []
            if not category in const.FUNDAMENTALS_KEYS: return []
            tableNames = [(period+x) for x in const.FUNDAMENTALS_KEYS[category]]
            return tableNames
        return [tableName]

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
        annualTimediff = 60*60*24*365
        quarterlyTimediff = 60*60*24*31*3
        trailingTimediff = 60*60*24*365
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
                    # we triy once every year
                    updateTimestamp = now - annualTimediff
                    lastTimeStamp = now
                    if lastTimeStamp <= updateTimestamp:
                        if not lastTimeStamp in settings:
                            settings[lastTimeStamp] = set()
                        settings[lastTimeStamp].add(tsType)
                else:
                    # set the update time to check based on naming of tsType
                    if tsType.startswith('annual'):
                        updateTimestamp = now - annualTimediff
                    elif tsType.startswith('quarterly'):
                        updateTimestamp = now - quarterlyTimediff
                    elif tsType.startswith('trailing'):
                        updateTimestamp = now - trailingTimediff
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
        self.db = database.Database(self.dbName)

        # update if needed 
        symbolSettings = self.update(symbols, tables, forceUpdate=forceUpdate)

        # dont'run  update if no symbols
        if len(symbolSettings) == 0: return

        log.info('TimeSeries update')
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
                # print('types  : %s' % len(types))
                # print('types  : %s' % typesString)
                # print('period1: %s' % datetime.fromtimestamp(period1))
                # print('period2: %s' % datetime.fromtimestamp(period2))
                requestArgs = {
                            'url': 'https://query2.finance.yahoo.com/ws/fundamentals-timeseries/v1/finance/timeseries/'+symbol.upper(),
                            'params': {
                                'type': typesString,
                                'period1': period1,
                                'period2': period2,
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

        typeLastTimestamps = {}
        if response.headers.get('content-type').startswith('application/json'):
            symbolData = response.json()
            if symbolData['timeseries']['result'] != None:
                for typeData in symbolData['timeseries']['result']:
                    tsType = typeData['meta']['type'][0]
                    if not tsType in typeData: continue
                    typeData = typeData[tsType]
                    writeData = {}
                    # pp(typeData)
                    lastTimestamp = 0
                    if len(typeData) > 0:
                        # print(tsType)
                        # pp(typeData)
                        writeData[symbol] = {}
                        for entry in typeData:
                            writeData[symbol]['currency'] = entry['currencyCode']
                            writeData[symbol][entry['asOfDate']] = entry['reportedValue']['raw']
                            entryTimestamp = int(datetime.strptime(entry['asOfDate'], '%Y-%m-%d').timestamp())
                            if entryTimestamp > lastTimestamp: lastTimestamp = entryTimestamp
                    
                    if len(writeData) > 0:
                        self.db.tableWrite(tsType, writeData, 'keySymbol', method='update')
                        self.db.tableWrite('lastentry_db', {symbol: {tsType: lastTimestamp}}, 'keySymbol', method='update')
                    
        # update status
        status = {symbol: {}}
        now = int(datetime.now().timestamp())
        for tsType in self.tsTypes[symbolIndex]:
            status[symbol][tsType] = now
        self.db.tableWrite('status_db', status, 'keySymbol', method='update')

    
    def dbCommit(self):
        # call from base to commit
        self.db.commit()
    
