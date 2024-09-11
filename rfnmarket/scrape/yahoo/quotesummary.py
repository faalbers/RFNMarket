from ...utils import log, database, storage
from . import const
from .base import Base
from pprint import pp
from datetime import datetime
import json

# https://yahooquery.dpguthrie.com/guide/ticker/modules/

class QuoteSummary(Base):
    def __init__(self, symbols, types):
        super().__init__()
        log.info('QuoteSummary update')
        self.symbols = symbols

        # decide what modules to search for based of request types
        modules = []
        for type in types:
            if type == 'profile':
                modules += ['quoteType', 'assetProfile', 'fundProfile']
            if type == 'statistics':
                modules += ['defaultKeyStatistics', 'summaryDetail']
            if type == 'price':
                modules += ['price']
        modules = set(modules)

        # find existing and not existing modules in database
        db = database.Database('yahoo_quotesummary')
        modulesExist = set(db.getTableNames()).intersection(modules)
        modulesNotExist = modules.difference(modulesExist)

        # find modules for each symbol that needs updating
        symbolModules = {}
        for symbol in self.symbols:
            symbolModules[symbol] = set(modules)
        
        # set symbol existing modules where history time is lower then 1 day ago
        now = datetime.now()
        updateTimestamp = int(now.timestamp())-(60*60*24)
        # updateTimestamp = int(now.timestamp())-0
        lowestTimestamp = int(now.timestamp())
        for module in modulesExist:
            values, params = db.getRows(module, ['keySymbol', 'timestamp'])
            for value in values:
                if value[0] in symbolModules:
                    if value[1] < lowestTimestamp: lowestTimestamp = value[1]
                    if value[1] >= updateTimestamp:
                        symbolModules[value[0]].remove(module)

        # final tally of symbols that need updating
        self.symbols = []
        for symbol, modules in symbolModules.items():
            if len(modules) > 0: self.symbols.append(symbol)
        
        log.info('types requested     : %s' % " ".join(types))
        log.info('requested modules   : %s' % " ".join(modules))
        log.info('existing modules    : %s' % " ".join(modulesExist))
        log.info('update before       : %s' % datetime.fromtimestamp(updateTimestamp))
        log.info('last time updated   : %s' % (now - datetime.fromtimestamp(lowestTimestamp)))
        log.info('modules to be added : %s' % " ".join(modulesNotExist))
        log.info('symbols processed   : %s' % len(self.symbols))

        if len(self.symbols) == 0: return
           
        requestArgsList = []
        for symbol in self.symbols:
            modulesString = ",".join(symbolModules[symbol])
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
            params = ['keySymbol','timestamp']
            values = [symbol,int(datetime.now().timestamp())]
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
                    None
                else:
                    missedTypes.add(type(value))
            db.insertOrReplace(module, params, tuple(values))
            if len(missedTypes) > 0:
                log.info('QuoteSummary: missed data types: %s' % list(missedTypes))

    def pushAPIData(self, symbolIndex, symbolData):
        symbol = self.symbols[symbolIndex]
        symbolData = symbolData['quoteSummary']
        if symbolData['result'] == None: return
        symbolData = symbolData['result'][0]
        
        db = database.Database('yahoo_quotesummary')
        for module, moduleData in symbolData.items():
            self.updateDatabaseRow(symbol, module, moduleData, db)



