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
        }
        return moduleUpdatePeriods
    
    def getSymbolModules(self, symbols, tables, forceUpdate):
        modules = set(tables)
        symbolModules = {}

        # if forced update we just get all requested modules on all symbols
        if forceUpdate:
            for symbol in symbols:
                symbolModules[symbol] = modules
            return symbolModules

        moduleUpdatePeriods = self.getModuleUpdatePeriods()

        # get status
        status = 'status_db'
        dfStatus = self.db.tableRead(status, keyValues=symbols)
       
        # check all requested symbols
        now = int(datetime.now().timestamp())
        for symbol in symbols:
            if symbol in dfStatus:
                # check all the found timestamps 
                foundModules = set(dfStatus[symbol].keys())
                # add the modules that are not found
                symbolModulesToDo = modules.difference(foundModules)
                # now check to see if we need to update the found oes based on timestamp
                for module in foundModules.difference(symbolModulesToDo):
                    moduleTimestamp = dfStatus[symbol][module]
                    updateTimestamp = now-moduleUpdatePeriods['default']
                    if module in moduleUpdatePeriods:
                        # we found it, use that one
                        updateTimestamp = now-moduleUpdatePeriods[module]
                    if updateTimestamp >= moduleTimestamp:
                        # this one needs update
                        symbolModulesToDo.add(module)
                # dont even add symbol if no modules need updating
                if len(symbolModulesToDo) > 0:
                    symbolModules[symbol] = symbolModulesToDo
            else:
                # symbol not found , update all requested modules
                symbolModules[symbol] = modules

        return symbolModules

    def __init__(self, symbols=[], tables=[], forceUpdate=False):
        super().__init__()
        self.db = database.Database(self.dbName)
        
        # update if needed 
        # modules not used , might as well remove it, it's always empty
        symbolModules = self.getSymbolModules(symbols, tables, forceUpdate=forceUpdate)

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
    
    def pushAPIData(self, symbolIndex, response):
        pushStart = datetime.now()
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
        
