from ...utils import log, database
from .base import Base
from pprint import pp
from datetime import datetime
import json
import pandas as pd

class TimeSeries(Base):
    dbName = 'yahoo_timeseries'

    def setUpdatePeriods(self):
        mult = 1
        # we use a default of a bit more then 3 months to make sure we get latest quarterly
        self.updateTsTypePeriods = {
            'default': int(mult*60*60*24*31*3.1),
        }

    def getSymbolSettings(self, symbols, tables, forceUpdate):
        symbolSettings = {}

        for symbol in symbols:
            symbolSettings[symbol] = {
                'types': set(tables),
                'period1': int( datetime.now().timestamp() - (60*60*24*365.2422*10) ),
            }

        return symbolSettings
    
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

    def __init__(self, symbols=[], tables=[], forceUpdate=False):
        super().__init__()
        self.db = database.Database(self.dbName)

        # update if needed 
        # modules not used , might as well remove it, it's always empty
        symbolSettings = self.getSymbolSettings(symbols, tables, forceUpdate=forceUpdate)

        # dont'run  update if no symbols
        if len(symbolSettings) == 0: return

        log.info('TimeSeries update')
        log.info('requested types   : %s' % " ".join(tables))
        log.info('symbols processing: %s' % len(symbolSettings))


        # log.info('QuoteSummary update')
        # self.dbName = 'yahoo_timeseries'
        # # make shore we don't mess up the referenced symbols variable
        # self.symbols = list(symbols)
        # self.types = types
        # self.setUpdatePeriods()
        # self.trimSymbols()


        # log.info('types requested     : %s' % " ".join(types))
        # log.info('requested tsTypes   : %s' % " ".join(self.tsTypes))
        # log.info('update before       : %s' % datetime.fromtimestamp(updateTimestamp))
        # log.info('last time updated   : %s' % (datetime.now() - datetime.fromtimestamp(self.lowestTimestamp)))
        # log.info('symbols processing  : %s' % len(self.symbols))

        # update procs need these
        self.symbols = [] # accessed by index
        self.symbolSettings = symbolSettings

        requestArgsList = []
        typesProcessed = set()
        # for symbol in self.symbols:
        for symbol, settings in symbolSettings.items():
            typesProcessed = typesProcessed.union(settings['types'])
            period1 = settings['period1']
            period2 = int(datetime.now().timestamp())

            typesString = ",".join(settings['types'])
            print('types  : %s' % typesString)
            print('period1: %s' % datetime.fromtimestamp(period1))
            print('period2: %s' % datetime.fromtimestamp(period2))
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
        log.info('types processing : %s' % " ".join(typesProcessed))
        self.multiRequest(requestArgsList, blockSize=50)

    def updateStatus(self, symbol):
        self.db.createTable('status_db', ["'keySymbol' TEXT PRIMARY KEY"])
        self.db.insertOrIgnore('status_db', ['keySymbol'], (symbol,))
        params = []
        values = []
        for tsType in self.symbolSettings[symbol]['types']:
            self.db.addColumn('status_db', tsType, 'TIMESTAMP')
            params.append(tsType)
            values.append(int(datetime.now().timestamp()))
        self.db.update( 'status_db', 'keySymbol', symbol, params, tuple(values) )

    def pushAPIData(self, symbolIndex, response):
        symbol = self.symbols[symbolIndex]
        if response.headers.get('content-type').startswith('application/json'):
            symbolData = response.json()
            for typeData in symbolData['timeseries']['result']:
                type = typeData['meta']['type'][0]
                if not type in typeData: continue

                # get entry dataFrame
                entry = pd.DataFrame(typeData[type])
                entry.insert(0, 'timestamp', typeData['timestamp'])
                # entry['date'] = pd.to_datetime(entry['date'], unit='s')
                entry['reportedValue'] = entry['reportedValue'].apply(json.dumps)
                dtype = {
                    'timestamp': 'TIMESTAMP PRIMARY KEY',
                    'reportedValue': 'JSON',
                }

                if not self.db.tableExists(type):
                    # table does not exist, create new one
                    entry.to_sql(type, self.db.getConnection(), if_exists='replace', index=False, dtype=dtype)
                else:
                    # it exists, filter out already existing timestamps. Then append
                    foundDates = self.db.getColumn(type, 'timestamp')
                    entry = entry[~entry['timestamp'].isin(foundDates)]
                    entry.to_sql(type, self.db.getConnection(), if_exists='append', index=False, dtype=dtype)

            

    # def pushAPIData(self, symbolIndex, response):
    #     symbol = self.symbols[symbolIndex]
    #     if response.headers.get('content-type').startswith('application/json'):
    #         symbolData = response.json()
    #         if 'timeseries' in symbolData:
    #             # handle API response
    #             symbolData = symbolData['timeseries']
    #             if symbolData['error'] != None:
    #                 # handle error response
    #                 symbolData = symbolData['error']
    #             elif symbolData['result'] != None:
    #                 # handle data return response
    #                 symbolData = symbolData['result']
    #                 for timeseriesData in symbolData:
    #                     self. updateTimeseriesDB(symbol, timeseriesData)
    #                 self.updateStatus(symbol)
    
    # def updateTimeseriesDB(self, symbol, timeseriesData):
    #     type = timeseriesData['meta']['type'][0]
    #     if not type in timeseriesData: return
    #     timestamps = timeseriesData['timestamp']
    #     tsDataList = timeseriesData[type]
    #     self.db.createTable(type, ["'keySymbolTimestamp' TEXT PRIMARY KEY", "'symbol' TEXT", "'timestamp' TIMESTAMP"])
    #     index = 0
    #     for tsData in tsDataList:
    #         keySymbolTimestamp = symbol+":"+str(timestamps[index])
    #         self.db.insertOrIgnore(type, ['keySymbolTimestamp'], (keySymbolTimestamp,))
    #         params = ['symbol', 'timestamp']
    #         values = [symbol, timestamps[index]]
    #         missedTypes = set()
    #         allTsData = {}
    #         for param, value in tsData.items():
    #             if isinstance(value, dict):
    #                 for vparam, vvalue in value.items():
    #                     allTsData[vparam] = vvalue
    #             else:
    #                 allTsData[param] = value
    #         for param, value in allTsData.items():
    #             if isinstance(value, int):
    #                 self.db.addColumn(type, param, 'INTEGER')
    #                 params.append(param)
    #                 values.append(value)
    #             elif isinstance(value, float):
    #                 self.db.addColumn(type, param, 'FLOAT')
    #                 params.append(param)
    #                 values.append(value)
    #             elif isinstance(value, str):
    #                 self.db.addColumn(type, param, 'TEXT')
    #                 params.append(param)
    #                 values.append(value)
    #             elif isinstance(value, bool):
    #                 self.db.addColumn(type, param, 'BOOLEAN')
    #                 params.append(param)
    #                 values.append(value)
    #             elif isinstance(value, list):
    #                 self.db.addColumn(type, param, 'JSON')
    #                 params.append(param)
    #                 values.append(json.dumps(value))
    #             elif isinstance(value, dict):
    #                 self.db.addColumn(type, param, 'JSON')
    #                 params.append(param)
    #                 values.append(json.dumps(value))
    #             elif isinstance(value, type(None)):
    #                 pass
    #             else:
    #                 missedTypes.add(type(value))
    #         self.db.update( type, 'keySymbolTimestamp', keySymbolTimestamp, params, tuple(values) )
    #         if len(missedTypes) > 0:
    #             log.info('TimeSeries: missed data types: %s' % list(missedTypes))
    #         index += 1
    
    def dbCommit(self):
        # call from base to commit
        self.db.commit()
    
