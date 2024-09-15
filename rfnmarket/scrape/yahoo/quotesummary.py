from ...utils import log, database
from .base import Base
from pprint import pp
from datetime import datetime
import json

# https://yahooquery.dpguthrie.com/guide/ticker/modules/

class QuoteSummary(Base):
    def setModuleUpdatePeriods(self):
        mult = 1
        # maybe use actual dates instead of time differences from now ?
        self.moduleUpdatePeriods = {
            'default': mult*60*60*24,
            'price': mult*60*60*24,
            'defaultKeyStatistics': mult*60*60*24,
            'summaryDetail': mult*60*60*24,
            'quoteType': mult*60*60*24*31*3,
            'assetProfile': mult*60*60*24*31*3,
            'fundProfile': mult*60*60*24*31*3,
        }
    
    def setSymbolModules(self):
        modulesForTypes = {
            'profile': ['quoteType', 'assetProfile', 'fundProfile'],
            'statistics': ['defaultKeyStatistics', 'summaryDetail'],
            'price': ['price'],
        }
        # find requested modules
        self.modules = set()
        for type in self.types:
            self.modules = self.modules.union(set(modulesForTypes[type]))

        # collect modules per symbol if update period is over for that module
        self.symbolModules = {}
        modulesToBeDone = set()
        db = database.Database(self.dbName)
        now = datetime.now()
        self.lowestTimestamp = int(now.timestamp())
        values, params = db.getRows('status_db')
        foundSymbolIndices = {}
        index = 0
        for value in values:
            foundSymbolIndices[value[0]] = index
            index += 1
        foundModuleIndices = {}
        index = 0
        for param in params:
            foundModuleIndices[param] = index
            index += 1
        # check all requested symbols
        for symbol in self.symbols:
            if symbol in foundSymbolIndices:
                value = values[foundSymbolIndices[symbol]]
                modules = set()
                for module in self.modules:
                    if module in foundModuleIndices:
                        moduleTimestamp = value[foundModuleIndices[module]]
                        if moduleTimestamp != None:
                            # check if found modules are in curren symbol's modules list and if there was a moduleTimestamp
                            if module in self.modules:
                                updateTimestamp = int(now.timestamp())-self.moduleUpdatePeriods['default']
                                if module in self.moduleUpdatePeriods:
                                    # we found it, use that one
                                    updateTimestamp = int(now.timestamp())-self.moduleUpdatePeriods[module]
                                # find the lowest module update on all requested symbols
                                if moduleTimestamp < self.lowestTimestamp: self.lowestTimestamp = moduleTimestamp
                                # add module module timestamp is lower then the module update timestamp
                                if updateTimestamp >= moduleTimestamp:
                                    modules.add(module)
                                    modulesToBeDone.add(module)
                        else:
                            modules.add(module)
                            modulesToBeDone.add(module)
                    else:
                        modules.add(module)
                        modulesToBeDone.add(module)
                if len(modules) > 0:
                    self.symbolModules[symbol] = modules
            elif len(self.modules) > 0:
                self.symbolModules[symbol] = set(self.modules)
                modulesToBeDone = modulesToBeDone.union(self.modules)
        
        # update modules with modules to be done
        self.modules = modulesToBeDone

    def __init__(self, symbols=None, types=None):
        super().__init__()
        self.dbName = 'yahoo_quotesummary'

        # if we are not updating just use class for data retrieval
        if symbols == None or types == None: return
        
        # make shore we don't mess up the referenced symbols variable
        self.symbols = list(symbols)
        self.types = types
        self.setModuleUpdatePeriods()
        self.setSymbolModules()
        
        # dont'run if no symbols
        if len(self.symbolModules) == 0: return
        
        log.info('QuoteSummary update')
        log.info('types requested     : %s' % " ".join(types))
        log.info('requested modules   : %s' % " ".join(self.modules))
        # log.info('update before       : %s' % datetime.fromtimestamp(updateTimestamp))
        log.info('last time updated   : %s' % (datetime.now() - datetime.fromtimestamp(self.lowestTimestamp)))
        log.info('symbols processing  : %s' % len(self.symbolModules))

        requestArgsList = []
        self.symbols = []
        for symbol, modules in self.symbolModules.items():
            modulesString = ",".join(self.symbolModules[symbol])
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

    def getStockSymbols(self):
        db = database.Database(self.dbName)
        values, params = db.getRows('quoteType', columns=['keySymbol'])
        return [x[0] for x in values]

