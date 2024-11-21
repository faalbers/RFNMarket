from ...utils import log, database
from .base import Base
from pprint import pp
from datetime import datetime
import json, copy
from . import const
import pandas as pd
import numpy as np

# https://yahooquery.dpguthrie.com/guide/ticker/modules/

class QuoteSummary(Base):
    dbName = 'yahoo_quotesummary'

    @staticmethod
    def getTableNames(tableName):
        if tableName == 'all':
            return list(const.QUOTESUMMARY_MODULES.keys())
        return [tableName]

    @staticmethod
    def getModuleUpdatePeriods():
        # maybe use actual dates instead of time differences from now ?
        moduleUpdatePeriods = {
            'default': 60*60*24,
            'price': 60*60*24,
            'defaultKeyStatistics': 60*60*24,
            'summaryDetail': 60*60*24,
            'quoteType': 60*60*24*31*3,
            'assetProfile': 60*60*24*31*3,
            'fundProfile': 60*60*24*31*3,
            'summaryProfile': 60*60*24*31*3,
            'fundProfile': 60*60*24*31*3,
        }
        return moduleUpdatePeriods
    
    def update(self, symbols, tables, forceUpdate):
        modules = set(tables)
        symbolModules = {}

        if forceUpdate:
            for symbol in symbols:
                symbolModules[symbol] = modules
            return symbolModules

        # check last timestamp of symbols in quote database
        dataStatus = self.db.tableRead('status_db', keyValues=symbols, columns=tables)

        # build status check for all symbols
        statusCheck = {}
        for symbol in symbols:
            statusCheck[symbol] = {}
            if symbol in dataStatus:
                for module in modules:
                    statusCheck[symbol][module] = {}
                    if module in dataStatus[symbol]:
                        # module was done for symbol before
                        statusCheck[symbol][module]['status'] = dataStatus[symbol][module]
                    else:
                        # No status for tstype in symbol, set both to none
                        statusCheck[symbol][module] = {'status': None}
            else:
                # no status for symbol, set all modules to status and latest None
                for module in modules:
                    statusCheck[symbol][module] = {'status': None}

        # create symbolModules
        moduleUpdatePeriods = self.getModuleUpdatePeriods()
        tenyearTimediff = int(60*60*24*365.2422*10)
        now = int(datetime.now().timestamp())
        for symbol, checkData in statusCheck.items():
            # setup settings 
            settings = set()
            for module, statusData in  checkData.items():
                if statusData['status'] == None:
                    settings.add(module)
                else:
                    # set the update time
                    if module in moduleUpdatePeriods:
                        updateTimestamp = now - moduleUpdatePeriods[module]
                    else:
                        updateTimestamp = now - moduleUpdatePeriods['default']
                    # get last entry timestamp for module
                    lastTimeStamp = dataStatus[symbol][module]
                    if lastTimeStamp <= updateTimestamp:
                        # we need to update
                        settings.add(module)
            # if settings is not empty addit to the symbol entry of symbolModules
            if len(settings) > 0:
                symbolModules[symbol] = settings

        return symbolModules

    def __init__(self, symbols=[], tables=[], forceUpdate=False):
        super().__init__()
        self.db = database.Database(self.dbName)
        
        # update if needed 
        # modules not used , might as well remove it, it's always empty
        # symbolModules = self.getSymbolModules(symbols, tables, forceUpdate=forceUpdate)
        symbolModules = self.update(symbols, tables, forceUpdate=forceUpdate)

        # dont'run  update if no symbols
        if len(symbolModules) == 0: return
      
        log.info('QuoteSummary update')
        log.info('requested modules  : %s' % " ".join(tables))
        log.info('symbols processing : %s' % len(symbolModules))

        # lets backup the database first
        self.db.backup()

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
    
    def pushAPIData(self, symbolIndex, response):
        symbol = self.symbols[symbolIndex]
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
                    modulesStart = datetime.now()
                    for module, moduleData in symbolData.items():
                        self.db.tableWrite(module, {symbol: moduleData}, 'keySymbol', method='replace')

        # update status
        status = {}
        for module in self.symbolModules[symbol]:
            status[module] = int(datetime.now().timestamp())
        self.db.tableWrite('status_db', {symbol: status}, 'keySymbol', method='update')
    
    def dbCommit(self):
        # call from base to commit
        self.db.commit()
        
