from ...utils import log, database
from .base import Base
from pprint import pp
from datetime import datetime
import json
from . import const

# https://yahooquery.dpguthrie.com/guide/ticker/modules/

class QuoteSummary(Base):
    dbName = 'yahoo_quotesummary'

    @staticmethod
    def getModuleUpdatePeriods():
        mult = 1
        # maybe use actual dates instead of time differences from now ?
        moduleUpdatePeriods = {
            'default': mult*60*60*24,
            'price': mult*60*60*24,
            'defaultKeyStatistics': mult*60*60*24,
            'summaryDetail': mult*60*60*24,
            'quoteType': mult*60*60*24*31*3,
            'assetProfile': mult*60*60*24*31*3,
            'fundProfile': mult*60*60*24*31*3,
        }
        return moduleUpdatePeriods
    
    def getSymbolModules(self, symbols, tables):
        modules = set(tables)
        moduleUpdatePeriods = self.getModuleUpdatePeriods()
        symbolModules = {}

        # collect modules per symbol if update period is over for that module
        db = database.Database(self.dbName)
        now = datetime.now()
        
        values, params = db.getRows('status_db')
        
        # get symbol indices of symbols that were last collected
        foundSymbolIndices = {}
        index = 0
        for value in values:
            foundSymbolIndices[value[0]] = index
            index += 1
        
        # get mdule indices of modules that were last collected
        foundModuleIndices = {}
        index = 0
        for param in params:
            foundModuleIndices[param] = index
            index += 1

        # check all requested symbols
        for symbol in symbols:
            if symbol in foundSymbolIndices:
                # get status data for symbol
                value = values[foundSymbolIndices[symbol]]
                foundModules = set()
                for module in modules:
                    if module in foundModuleIndices:
                        moduleTimestamp = value[foundModuleIndices[module]]
                        if moduleTimestamp != None:
                            # check if found modules are in curren symbol's modules list and if there was a moduleTimestamp
                            if module in modules:
                                # set update timestamp
                                updateTimestamp = int(now.timestamp())-moduleUpdatePeriods['default']
                                if module in moduleUpdatePeriods:
                                    # we found it, use that one
                                    updateTimestamp = int(now.timestamp())-moduleUpdatePeriods[module]
                                
                                # add module if update timestamp is lower or equal then the module update timestamp
                                if updateTimestamp >= moduleTimestamp:
                                    foundModules.add(module)
                        else:
                            # added because dymbol does not have these modules done before
                            foundModules.add(module)
                # now set the modules that need to be updated for that symbol if there are any
                if len(foundModules) > 0:
                    symbolModules[symbol] = foundModules

        return symbolModules

    def update(self, symbols, tables):
        if len(symbols) == 0 or len(symbols) == 0: return {}, set()

        symbolModules = self.getSymbolModules(symbols, tables)

        return symbolModules, set()

    def __init__(self, symbols=[], types=None, tables=[]):
        super().__init__()
        # update if needed 
        symbolModules, modules = self.update(symbols, tables)

        # dont'run  update if no symbols
        if len(symbolModules) == 0: return
        
        log.info('QuoteSummary update')
        log.info('requested modules  : %s' % " ".join(tables))
        log.info('symbols processing : %s' % len(symbolModules))

        # update procs need these
        self.symbols = [] # accessed by index
        self.symbolModules = symbolModules

        requestArgsList = []
        modulesProcessed = set()
        for symbol, modules in symbolModules.items():
            modulesProcessed = modulesProcessed.union(modules)
            modulesString = ",".join(modules)
            requestArgs = {
                'url': 'https://query2.finance.yahoo.com/v10/finance/quoteSummary/'+symbol.upper(),
                'params': {
                    'modules': modulesString,
                    'corsDomain': 'finance.yahoo.com',
                    'formatted': 'false',
                },
                'timeout': 30,
            }
            requestArgsList.append(requestArgs)
            self.symbols.append(symbol)
        log.info('modules processing : %s' % " ".join(modulesProcessed))
        self.multiRequest(requestArgsList, blockSize=100)
    
    def updateDatabaseRow(self, symbol, module, moduleData, db):
            db.createTable(module, ["'keySymbol' TEXT PRIMARY KEY", "'timestamp' TIMESTAMP"])
            db.insertOrIgnore(module, ['keySymbol'], (symbol,))
            params = ['timestamp']
            values = [int(datetime.now().timestamp())]
            missedTypes = set()
            for param, value in moduleData.items():
                if isinstance(value, int):
                    db.addColumn(module, param, 'INTEGER')
                    params.append(param)
                    values.append(value)
                elif isinstance(value, float):
                    db.addColumn(module, param, 'FLOAT')
                    params.append(param)
                    values.append(value)
                elif isinstance(value, str):
                    db.addColumn(module, param, 'TEXT')
                    params.append(param)
                    values.append(value)
                elif isinstance(value, bool):
                    db.addColumn(module, param, 'BOOLEAN')
                    params.append(param)
                    values.append(value)
                elif isinstance(value, list):
                    db.addColumn(module, param, 'JSON')
                    params.append(param)
                    values.append(json.dumps(value))
                elif isinstance(value, dict):
                    db.addColumn(module, param, 'JSON')
                    params.append(param)
                    values.append(json.dumps(value))
                elif isinstance(value, type(None)):
                    pass
                else:
                    missedTypes.add(type(value))
            db.update( module, 'keySymbol', symbol, params, tuple(values) )
            if len(missedTypes) > 0:
                log.info('QuoteSummary: missed data types: %s' % list(missedTypes))

    def updateStatus(self, symbol, db):
        db.createTable('status_db', ["'keySymbol' TEXT PRIMARY KEY"])
        db.insertOrIgnore('status_db', ['keySymbol'], (symbol,))
        params = []
        values = []
        for module in self.symbolModules[symbol]:
            db.addColumn('status_db', module, 'TIMESTAMP')
            params.append(module)
            values.append(int(datetime.now().timestamp()))
        db.update( 'status_db', 'keySymbol', symbol, params, tuple(values) )

    def pushAPIData(self, symbolIndex, response):
        symbol = self.symbols[symbolIndex]
        db = database.Database(self.dbName)
        if response.headers.get('content-type').startswith('application/json'):
            symbolData = response.json()
            if 'quoteSummary' in symbolData:
                # handle API response
                symbolData = symbolData['quoteSummary']
                if symbolData['error'] != None:
                    # handle error response
                    symbolData = symbolData['error']
                elif symbolData['result'] != None:
                    # handle data return response
                    symbolData = symbolData['result'][0]
                    for module, moduleData in symbolData.items():
                        self.updateDatabaseRow(symbol, module, moduleData, db)
            if 'finance' in symbolData:
                # handle other possible errors
                symbolData = symbolData['finance']
                if symbolData['error'] != None:
                    symbolData = symbolData['error']
        self.updateStatus(symbol, db)

    def getQuoteTypeSymbols(self):
        db = database.Database(self.dbName)
        values, params = db.getRows('quoteType', columns=['keySymbol'])
        return [x[0] for x in values]

    def getData(self, symbols, types):
        data = {}
        types = set(types).intersection(self.__modulesForTypes.keys())
        if len(symbols) == 0 or len(types) == 0: return data

        modules = set()
        for type in types:
            modules = modules.union(set(self.__modulesForTypes[type]))

        db = database.Database(self.dbName)
        for symbol in symbols:
            data[symbol] = {}
            for module in modules:
                values, params = db.getRows(module, whereColumns=['keySymbol'], areValues=[symbol])
                if len(values) == 0: continue
                data[symbol][module] = {}
                for value in values:
                    index = 1
                    for param in params[1:]:
                        data[symbol][module][param] = value[index]
                        index += 1
        return data
