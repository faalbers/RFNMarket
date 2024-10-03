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
    def getModuleUpdatePeriods(forceUpdate):
        mult = 1
        if forceUpdate:
            mult = 0
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
    
    def getSymbolModules(self, symbols, tables, forceUpdate):
        modules = set(tables)
        moduleUpdatePeriods = self.getModuleUpdatePeriods(forceUpdate)

        # symbolModules = {}
        # for symbol in symbols:
        #     symbolModules[symbol] = modules
        # return symbolModules

        # get status
        status = 'status_db'
        dfStatus = None
        now = datetime.now()
        if self.db.tableExists(status):
            dfStatus = pd.read_sql("SELECT * FROM '%s'" % status, self.db.getConnection(), index_col='keySymbol')
        
        # check all requested symbols
        symbolModules = {}
        for symbol in symbols:
            if isinstance(dfStatus, pd.DataFrame):
                updateModules = set(dfStatus.columns)
                symbolModulesToDo = modules.difference(updateModules)
                for module in modules.intersection(updateModules):
                    moduleTimestamp = dfStatus[module].get(symbol, None)
                    if moduleTimestamp == None:
                        symbolModulesToDo.add(module)
                        continue
                    else:
                        updateTimestamp = int(now.timestamp())-moduleUpdatePeriods['default']
                        if module in moduleUpdatePeriods:
                            # we found it, use that one
                            updateTimestamp = int(now.timestamp())-moduleUpdatePeriods[module]
                        # add module if update timestamp is lower or equal then the module update timestamp
                        if updateTimestamp >= moduleTimestamp:
                            symbolModulesToDo.add(module)
                if len(symbolModulesToDo) > 0: symbolModules[symbol] = symbolModulesToDo

            else:
                symbolModules[symbol] = set().union(modules)

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
                        self.db.idxTableWriteRow(moduleData, module, 'keySymbol', symbol, 'update')

        # update status
        status = {}
        for module in self.symbolModules[symbol]:
            status[module] = int(datetime.now().timestamp())
        self.db.idxTableWriteRow(status, 'status_db', 'keySymbol', symbol, 'update')
    
    def dbCommit(self):
        # call from base to commit
        self.db.commit()
        
