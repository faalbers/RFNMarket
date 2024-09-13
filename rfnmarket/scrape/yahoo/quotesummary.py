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
    
    def trimModulesAndSymbols(self):
        modulesForTypes = {
            'profile': ['quoteType', 'assetProfile', 'fundProfile'],
            'statistics': ['defaultKeyStatistics', 'summaryDetail'],
            'price': ['price'],
        }
        # find requested modules
        self.modules = set()
        for type in self.types:
            self.modules = self.modules.union(set(modulesForTypes[type]))
        
        # set all symbols with those modules
        self.symbolModules = {}
        for symbol in self.symbols:
            self.symbolModules[symbol] = set(self.modules)

        # remove modules per symbol if update period is not over yet based on last runs
        # # also remove symbols if they don't need to be updated     
        db = database.Database(self.dbName)
        now = datetime.now()
        self.lowestTimestamp = int(now.timestamp())
        values, params = db.getRows('status')
        for value in values:
            symbol = value[0]
            # only check symbols we are requesting
            if symbol in self.symbolModules:
                valIndex = 1
                for module in params[1:]:
                    moduleTimestamp = value[valIndex]
                    # check if foumd modules are in curren tymbol's modules lidt
                    if module in self.symbolModules[symbol]:
                        # set default period if we cant find period of the module in the presets
                        updateTimestamp = int(now.timestamp())-self.moduleUpdatePeriods['default']
                        if module in self.moduleUpdatePeriods:
                            # we found it, use that one
                            updateTimestamp = int(now.timestamp())-self.moduleUpdatePeriods[module]
                        # find the lowest module update on all requested symbols
                        if moduleTimestamp < self.lowestTimestamp: self.lowestTimestamp = moduleTimestamp
                        # if we did not pass update period yet, remove module from symbol modules
                        if moduleTimestamp >= updateTimestamp:
                            self.symbolModules[symbol].remove(module)
                    valIndex += 1
                # finally check if we still have modules left on the symbols. If not, reove symbol from list
                if len(self.symbolModules[symbol]) == 0:
                    self.symbols.remove(value[0])
        
        # finally gather all modules that will be read
        self.modules = set()
        for symbol in self.symbols:
            self.modules = self.modules.union(self.symbolModules[symbol])

    def __init__(self, symbols, types):
        super().__init__()
        log.info('QuoteSummary update')
        self.dbName = 'yahoo_quotesummary'
        # make shore we don't mess up the referenced symbols variable
        self.symbols = list(symbols)
        self.types = types
        self.setModuleUpdatePeriods()
        self.trimModulesAndSymbols()
        
        log.info('types requested     : %s' % " ".join(types))
        log.info('requested modules   : %s' % " ".join(self.modules))
        # log.info('update before       : %s' % datetime.fromtimestamp(updateTimestamp))
        log.info('last time updated   : %s' % (datetime.now() - datetime.fromtimestamp(self.lowestTimestamp)))
        log.info('symbols processing  : %s' % len(self.symbols))

        # dont'run if no symbols
        if len(self.symbols) == 0: return

        requestArgsList = []
        for symbol in self.symbols:
            modulesString = ",".join(self.symbolModules[symbol])
            # print('%s: %s' % (symbol, modulesString))
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
        self.multiRequest(requestArgsList, blockSize=100)
    
    def updateDatabaseRow(self, symbol, module, moduleData, db):
            db.addTable(module, ["'keySymbol' TEXT PRIMARY KEY", "'timestamp' TIMESTAMP"])
            db.insertOrIgnore(module, ['keySymbol'], (symbol,))
            params = ['timestamp']
            values = [int(datetime.now().timestamp())]
            missedTypes = set()
            for param, value in moduleData.items():
                if isinstance(value, int):
                    db.addColumnIfNotExists(module, param, 'INTEGER')
                    params.append(param)
                    values.append(value)
                elif isinstance(value, float):
                    db.addColumnIfNotExists(module, param, 'FLOAT')
                    params.append(param)
                    values.append(value)
                elif isinstance(value, str):
                    db.addColumnIfNotExists(module, param, 'TEXT')
                    params.append(param)
                    values.append(value)
                elif isinstance(value, bool):
                    db.addColumnIfNotExists(module, param, 'BOOLEAN')
                    params.append(param)
                    values.append(value)
                elif isinstance(value, list):
                    db.addColumnIfNotExists(module, param, 'JSON')
                    params.append(param)
                    values.append(json.dumps(value))
                elif isinstance(value, dict):
                    db.addColumnIfNotExists(module, param, 'JSON')
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
        db.addTable('status', ["'keySymbol' TEXT PRIMARY KEY"])
        db.insertOrIgnore('status', ['keySymbol'], (symbol,))
        params = []
        values = []
        for module in self.symbolModules[symbol]:
            db.addColumnIfNotExists('status', module, 'TIMESTAMP')
            params.append(module)
            values.append(int(datetime.now().timestamp()))
        db.update( 'status', 'keySymbol', symbol, params, tuple(values) )

    def pushAPIData(self, symbolIndex, response):
        symbol = self.symbols[symbolIndex]
        db = database.Database(self.dbName)
        self.updateStatus(symbol, db)
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





