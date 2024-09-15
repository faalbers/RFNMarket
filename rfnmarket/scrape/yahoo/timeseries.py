from ...utils import log, database
from .base import Base
from pprint import pp
from datetime import datetime
import json

class TimeSeries(Base):
    def setUpdatePeriods(self):
        mult = 1
        # we use a default of a bit more then 3 months to make sure we get latest quarterly
        self.updateTsTypePeriods = {
            'default': int(mult*60*60*24*31*3.1),
        }

    def trimSymbols(self):
        # set timeseries types for type requests
        tsTypesForTypes = {
            # 'test': ['trailingPegRatio'],
            # 'test': ['trailingPegRatio', 'quarterlyNormalizedEBITDA', 'quarterlyTaxRateForCalcs'],
            'test': ['quarterlyTaxRateForCalcs', 'quarterlyNormalizedEBITDA','trailingPegRatio', 'annualRetainedEarnings'],
        }
        
        # find timeseries types
        self.tsTypes = set()
        for type in self.types:
            self.tsTypes = self.tsTypes.union(set(tsTypesForTypes[type]))

        # find timeseries types for symbols
        self.symbolTsTypes = {}
        for symbol in self.symbols:
            self.symbolTsTypes[symbol] = set(self.tsTypes)

        # set all symbols with 10 year period 1 for if we never did them before
        now = datetime.now()
        self.lowestTimestamp = int(now.timestamp())
        self.symbolPeriod1 = {}
        for symbol in self.symbols:
            self.symbolPeriod1[symbol] = self.lowestTimestamp - int(60*60*24*365.2422*10)
        
        # remove modules per symbol if update period is not over yet based on last runs
        # # also remove symbols if they don't need to be updated     
        db = database.Database(self.dbName)
        now = datetime.now()
        self.lowestTimestamp = int(now.timestamp())
        values, params = db.getRows('status_db')
        for value in values:
            symbol = value[0]
            # only check symbols we are requesting
            if symbol in self.symbolTsTypes:
                valIndex = 1
                for tsType in params[1:]:
                    tsTypeTimestamp = value[valIndex]
                    # check if found tsTypes are in curren ymbol's tsType list
                    if tsType in self.symbolTsTypes[symbol]:
                        # set default period if we cant find period of the tsType in the presets
                        updateTimestamp = int(now.timestamp())-self.updateTsTypePeriods['default']
                        if tsType in self.updateTsTypePeriods:
                            # we found it, use that one
                            updateTimestamp = int(now.timestamp())-self.updateTsTypePeriods[tsType]
                        # find the lowest tsTypes update on all requested symbols
                        if tsTypeTimestamp < self.lowestTimestamp: self.lowestTimestamp = tsTypeTimestamp
                        # if we did not pass update period yet, remove module from symbol modules
                        if tsTypeTimestamp >= updateTimestamp:
                            self.symbolTsTypes[symbol].remove(tsType)
                        else:
                            self.symbolPeriod1[symbol] = tsTypeTimestamp
                    valIndex += 1
                # finally check if we still have modules left on the symbols. If not, reove symbol from list
                if len(self.symbolTsTypes[symbol]) == 0:
                    self.symbols.remove(value[0])
        
        # finally gather all tsTypes that will be read
        self.tsTypes = set()
        for symbol in self.symbols:
            self.tsTypes = self.tsTypes.union(self.symbolTsTypes[symbol])
    
    def __init__(self, symbols, types):
        super().__init__()
        log.info('QuoteSummary update')
        self.dbName = 'yahoo_timeseries'
        # make shore we don't mess up the referenced symbols variable
        self.symbols = list(symbols)
        self.types = types
        self.setUpdatePeriods()
        self.trimSymbols()

        log.info('types requested     : %s' % " ".join(types))
        log.info('requested tsTypes   : %s' % " ".join(self.tsTypes))
        # log.info('update before       : %s' % datetime.fromtimestamp(updateTimestamp))
        log.info('last time updated   : %s' % (datetime.now() - datetime.fromtimestamp(self.lowestTimestamp)))
        log.info('symbols processing  : %s' % len(self.symbols))

        requestArgsList = []
        for symbol in self.symbols:
            period1 = self.symbolPeriod1[symbol]
            period2 = int(datetime.now().timestamp())

            self.tsTypes
            typesString = ",".join(self.symbolTsTypes[symbol])
            print(symbol)
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
        self.multiRequest(requestArgsList, blockSize=50)

    def updateStatus(self, symbol, db):
        db.createTable('status_db', ["'keySymbol' TEXT PRIMARY KEY"])
        db.insertOrIgnore('status_db', ['keySymbol'], (symbol,))
        params = []
        values = []
        for tsType in self.symbolTsTypes[symbol]:
            db.addColumn('status_db', tsType, 'TIMESTAMP')
            params.append(tsType)
            values.append(int(datetime.now().timestamp()))
        db.update( 'status_db', 'keySymbol', symbol, params, tuple(values) )

    def updateTimeseriesDB(self, symbol, timeseriesData, db):
        type = timeseriesData['meta']['type'][0]
        if not type in timeseriesData: return
        timestamps = timeseriesData['timestamp']
        tsDataList = timeseriesData[type]
        db.createTable(type, ["'keySymbolTimestamp' TEXT PRIMARY KEY", "'symbol' TEXT", "'timestamp' TIMESTAMP"])
        index = 0
        for tsData in tsDataList:
            keySymbolTimestamp = symbol+":"+str(timestamps[index])
            db.insertOrIgnore(type, ['keySymbolTimestamp'], (keySymbolTimestamp,))
            params = ['symbol', 'timestamp']
            values = [symbol, timestamps[index]]
            missedTypes = set()
            allTsData = {}
            for param, value in tsData.items():
                if isinstance(value, dict):
                    for vparam, vvalue in value.items():
                        allTsData[vparam] = vvalue
                else:
                    allTsData[param] = value
            for param, value in allTsData.items():
                if isinstance(value, int):
                    db.addColumn(type, param, 'INTEGER')
                    params.append(param)
                    values.append(value)
                elif isinstance(value, float):
                    db.addColumn(type, param, 'FLOAT')
                    params.append(param)
                    values.append(value)
                elif isinstance(value, str):
                    db.addColumn(type, param, 'TEXT')
                    params.append(param)
                    values.append(value)
                elif isinstance(value, bool):
                    db.addColumn(type, param, 'BOOLEAN')
                    params.append(param)
                    values.append(value)
                elif isinstance(value, list):
                    db.addColumn(type, param, 'JSON')
                    params.append(param)
                    values.append(json.dumps(value))
                elif isinstance(value, dict):
                    db.addColumn(type, param, 'JSON')
                    params.append(param)
                    values.append(json.dumps(value))
                elif isinstance(value, type(None)):
                    pass
                else:
                    missedTypes.add(type(value))
            db.update( type, 'keySymbolTimestamp', keySymbolTimestamp, params, tuple(values) )
            if len(missedTypes) > 0:
                log.info('TimeSeries: missed data types: %s' % list(missedTypes))
            index += 1

    def pushAPIData(self, symbolIndex, response):
        symbol = self.symbols[symbolIndex]
        if response.headers.get('content-type').startswith('application/json'):
            symbolData = response.json()
            if 'timeseries' in symbolData:
                # handle API response
                symbolData = symbolData['timeseries']
                if symbolData['error'] != None:
                    # handle error response
                    symbolData = symbolData['error']
                elif symbolData['result'] != None:
                    # handle data return response
                    symbolData = symbolData['result']
                    db = database.Database(self.dbName)
                    for timeseriesData in symbolData:
                        self. updateTimeseriesDB(symbol, timeseriesData, db)
                    self.updateStatus(symbol, db)
